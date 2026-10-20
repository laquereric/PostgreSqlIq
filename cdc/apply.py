"""The global side of CDC: one schema's captured changes, replayed into a Postgres the rest of IQ reads.

cdc.py ships every change on the box's Postgres as an iq-cdc record into the blob store. For most tables the
records themselves are the point. Some schemas must be a working Postgres again on the far side. The first is the
lake's catalog (GherkinIq/Facet/Store/lake.py): it lives in the User/Group domain's local Postgres, and BI
(UC_Superset) attaches its replica in the global cluster read-only. This module is that replay.

    base       the source's latest cdc:snapshot covering the schema, if newer than what was applied: the target's
               tables are emptied and the snapshot's rows inserted. Stream records at or before its LSN are in it.
    stream     cdc:insert / update / delete / truncate past the watermark, in (commit_lsn, lsn) order, one target
               transaction per source transaction. An update or delete matches its `old` row: every column, as
               REPLICA IDENTITY FULL sends it (cdc.publish_schema), one row (a keyless table may hold two equal).
    watermark  (host, source_schema, target_schema) -> the last commit LSN applied, written in the same
               transaction as the changes. A re-run applies only what is past it.
    stats      CDC turns every e-mail in a value into an identity token (`iq:subj:`). A min/max statistic that
               carries one no longer bounds the data it describes, so it is applied as NULL: the reader scans
               that file instead of pruning it. Fewer skips, never a wrong answer.

CDC ships no DDL, so the target's tables must exist first: `prepare_lake` has DuckLake create its own catalog
tables in the target schema, then empties them for the replay.

Every write goes through the data plane (the user's rule, 2026-10-19): the target is one schema of the plane's record
role (dataplane.Plane.replica), so this module orders, de-identifies and watermarks, and the store's own dialect --
how a row is matched, how json is held -- stays in its adapter (dataplane.PostgresReplica).

    python apply.py lake <target-dsn> <target-schema> --host <box> [--schema ducklake] [--blobs <dir>]
                                                          [--domain <iq: IRI>]
    python apply.py plane <target-dsn> --host <box> [--hop proxy] [--prefix iq:token-status:] [--blobs <dir>]

`plane` is the proxy's end of the domain's second hop (cdc.py --hop proxy): the domain's token-status records, put
on the proxy's routing store as the values they are (put_records) -- never a replica, which could truncate it.

A device syncing to its domain is the same hop: with --domain, the device's heartbeat (cdc.status_record) and what
the replay made of its records become the device's status in the domain (devices.observe).

@use_case Development/UseCase/UC_CDC
@facet Store
@status Proposed
"""
from __future__ import annotations

import json
import sys
from itertools import groupby

import cdc

TOKEN = "iq:subj:"
STAT_COLUMNS = frozenset({"min_value", "max_value"})


def records_in(store, prefix: str = "auth/events/") -> list[dict]:
    """Every iq-cdc record the blob store holds (store: list(prefix) and get(key), as the bridge's stores)."""
    out = []
    for key in store.list(prefix):
        if "/svc=%s/" % cdc.SERVICE in key:
            body = store.get(key)
            out.append(json.loads(body.decode() if isinstance(body, bytes) else body))
    return out


def _tables(rec: dict) -> list[str]:
    return rec["table"].split(",")


def _ours(rec: dict, host: str, schema: str) -> bool:
    return (rec.get("service") == cdc.SERVICE and rec.get("host") == host
            and all(t.startswith(schema + ".") for t in _tables(rec)))


def _key(rec: dict) -> tuple:
    return cdc.lsn_int(rec["commit_lsn"]), cdc.lsn_int(rec["lsn"]), rec.get("seq", 0)


def scrub(row):
    """A statistic carrying an identity token bounds nothing: NULL, so the reader scans instead of pruning."""
    if row is None:
        return None
    return {k: (None if k in STAT_COLUMNS and isinstance(v, str) and TOKEN in v else v) for k, v in row.items()}


def _table(table: str) -> str:
    return table.split(".", 1)[1]


def _change(rep, rec: dict):
    op = rec["op"]
    if op == "cdc:truncate":
        rep.truncate([_table(t) for t in _tables(rec)])
        return
    table, new, old = _table(rec["table"]), scrub(rec.get("new")), scrub(rec.get("old"))
    if op == "cdc:insert":
        rep.insert(table, new)
    elif op == "cdc:update":
        if not old:
            raise ValueError("update without its old row on %s: publish the schema REPLICA IDENTITY FULL" % table)
        if rep.update(table, new, old) != 1:
            raise LookupError("update found no row on %s at %s" % (table, rec["lsn"]))
    elif op == "cdc:delete":
        if rep.delete(table, old) != 1:
            raise LookupError("delete found no row on %s at %s" % (table, rec["lsn"]))


def apply(rep, records, *, host: str, source_schema: str) -> dict:
    """Replay `records` (any mix; only this host's, for this schema, are used) into `rep`, one schema of the record
    store handed out by the data plane (Plane.replica). Returns {ok, base, applied, skipped, watermark} or
    {ok: False, reason, detail}."""
    ours = sorted((r for r in records if _ours(r, host, source_schema)), key=_key)
    with rep.transaction():
        held = rep.marked(host, source_schema)
    mark = cdc.lsn_int(held) if held else -1
    snaps = [r for r in ours if r["op"] == "cdc:snapshot"]
    base, applied, skipped = None, 0, 0
    if snaps:
        s_lsn = max(cdc.lsn_int(r["lsn"]) for r in snaps)
        if s_lsn > mark:
            rows = [r for r in snaps if cdc.lsn_int(r["lsn"]) == s_lsn]
            with rep.transaction():
                rep.truncate()
                for r in rows:
                    rep.insert(_table(r["table"]), scrub(r["new"]))
                rep.mark(host, source_schema, cdc.lsn_text(s_lsn))
            mark, base = s_lsn, {"lsn": cdc.lsn_text(s_lsn), "rows": len(rows)}
    stream = [r for r in ours if r["op"] != "cdc:snapshot"]
    for commit, txn in groupby(stream, key=lambda r: cdc.lsn_int(r["commit_lsn"])):
        txn = list(txn)
        if commit <= mark:
            skipped += len(txn)
            continue
        try:
            with rep.transaction():
                for r in txn:
                    _change(rep, r)
                rep.mark(host, source_schema, cdc.lsn_text(commit))
        except Exception as exc:  # noqa: BLE001 -- the transaction rolled back; stop at it, report it
            return {"ok": False, "reason": "apply_failed", "detail": str(exc).splitlines()[0][:200],
                    "at": cdc.lsn_text(commit), "base": base, "applied": applied, "skipped": skipped,
                    "watermark": cdc.lsn_text(mark) if mark >= 0 else None}
        applied, mark = applied + len(txn), commit
    return {"ok": True, "base": base, "applied": applied, "skipped": skipped,
            "watermark": cdc.lsn_text(mark) if mark >= 0 else None}


PLANE_TABLE = "public.plane_datum"


def _array(v) -> list:
    """A text[] as CDC ships it (Postgres's literal, e.g. {a,b}) or already a list."""
    if isinstance(v, list):
        return v
    inner = (v or "{}")[1:-1]
    return [x.strip().strip('"') for x in inner.split(",")] if inner else []


def put_records(plane, records, *, host: str, prefix: str) -> dict:
    """A hop of the plane's own table (cdc --hop proxy) replayed as values: every inserted or snapshot row of
    public.plane_datum under `prefix` is put on `plane` as the Datum it is. A plane row never changes, so a put is
    the whole replay: the same value again is a no-op, nothing is truncated, and no watermark is needed. A row
    outside `prefix` is not put (skipped: the proxy takes only what it may hold); an update or delete is refused
    (a plane row does not change). -> {ok, put, already, skipped, refused: [...]}"""
    sys.path.insert(0, str(_store_dir()))
    import dataplane
    ours = sorted((r for r in records if r.get("service") == cdc.SERVICE and r.get("host") == host
                   and r.get("table") == PLANE_TABLE), key=_key)
    out = {"ok": True, "put": 0, "already": 0, "skipped": 0, "refused": []}
    for rec in ours:
        if rec["op"] not in ("cdc:insert", "cdc:snapshot"):
            out["refused"].append("%s at %s: a plane row does not change" % (rec["op"], rec["lsn"]))
            continue
        row = rec["new"]
        if not str(row.get("iri", "")).startswith(prefix):
            out["skipped"] += 1
            continue
        d = dataplane.Datum(row["iri"], row["tier"], row["body"], row["media_type"], tuple(_array(row["derived_from"])),
                            row["recorded_at"])
        held = plane.get(d.iri, "cdc-put")
        r = plane.put(d, "cdc-put")
        if not r.ok:
            out["refused"].append("%s: %s" % (d.iri, r.reason))
        elif held.ok and held.found:
            out["already"] += 1
        else:
            out["put"] += 1
    out["ok"] = not out["refused"]
    return out


def _plane(target: str, env: dict | None = None):
    """The global side's data plane, bound to the target store (dataplane.from_env names the store, not us)."""
    sys.path.insert(0, str(_store_dir()))
    import dataplane
    return dataplane.from_env({"IQ_RECORD_DSN": target, **(env or {})})


def replica(target: str, target_schema: str):
    """The target schema as the plane hands it out for replay; raises when the plane refuses."""
    r, rep = _plane(target).replica(target_schema, "cdc-apply")
    if not r.ok:
        raise PermissionError("the data plane refused the replica: %s" % r.reason)
    return rep


def prepare_lake(target: str, target_schema: str) -> dict:
    """The target's catalog tables, created by DuckLake itself through the plane (so the DDL is the reader's own
    version's), then emptied: what they hold comes from the box. Done once; a schema already holding the catalog is
    left alone."""
    import tempfile
    rep = replica(target, target_schema)
    try:
        if "ducklake_metadata" in rep.tables():
            return {"ok": True, "created": False}
        r, conn = _plane(target, {"IQ_LAKE_SCHEMA": target_schema,
                                  "IQ_LAKE_DATA": tempfile.mkdtemp(prefix="iq-lake-prepare-")}).gold(
            write=True, operation_id="cdc-prepare")
        if not r.ok:
            return {"ok": False, "reason": r.reason}
        conn.close()
        held = rep.tables()
        rep.truncate(held)
        return {"ok": True, "created": True, "tables": len(held)}
    finally:
        rep.close()


def _store_dir():
    from pathlib import Path
    return Path(__file__).resolve().parents[3] / "GherkinIq" / "Facet" / "Store"


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="apply.py", description=__doc__.split("\n\n")[0])
    ap.add_argument("what", choices=["lake", "plane"])
    ap.add_argument("target_dsn", help="the global store, as the data plane's record role binds it")
    ap.add_argument("target_schema", nargs="?", help="lake: the schema the catalog is replayed into")
    ap.add_argument("--host", required=True, help="the box whose records to replay (the record's `host`)")
    ap.add_argument("--schema", default="ducklake", help="the source schema on the box")
    ap.add_argument("--blobs", help="a local directory of records (default: the iq-blobs bucket, S3 creds)")
    ap.add_argument("--domain", help="the domain's iq: IRI, when this hop is a device syncing to its domain: the"
                                     " device's status is recorded (GherkinIq/Facet/Store/devices.py)")
    ap.add_argument("--hop", default="proxy", help="plane: the hop whose records to put (cdc.py --hop)")
    ap.add_argument("--prefix", default="iq:token-status:", help="plane: only IRIs under this prefix are put")
    a = ap.parse_args(argv)
    if a.what == "lake" and not a.target_schema:
        ap.error("lake needs a target schema")
    store = DirStore(a.blobs) if a.blobs else _s3()
    if a.what == "plane":
        cdc.use_hop(a.hop)
        sys.path.insert(0, str(_store_dir()))
        import dataplane
        r = put_records(dataplane.from_env({"IQ_RECORD_DSN": a.target_dsn}, roles=("record",)), records_in(store),
                        host=a.host, prefix=a.prefix)
        print(json.dumps(r))
        return 0 if r["ok"] else 1
    print(json.dumps(prepare_lake(a.target_dsn, a.target_schema)))
    recs = records_in(store)
    rep = replica(a.target_dsn, a.target_schema)
    try:
        r = apply(rep, recs, host=a.host, source_schema=a.schema)
    finally:
        rep.close()
    print(json.dumps(r))
    if a.domain:
        import devices
        seen = devices.observe(_plane(a.target_dsn), a.domain, a.host, recs, r, "cdc-apply")
        print(json.dumps({"device": a.host, "reported": seen.ok if seen else None,
                          "reason": seen.reason if seen else "no heartbeat"}))
    return 0 if r["ok"] else 1


class DirStore:
    """Records as files under a directory, keyed by their path below it (a copy of the bucket's layout)."""

    def __init__(self, root: str):
        from pathlib import Path
        self.root = Path(root)

    def list(self, prefix: str = ""):
        return sorted(str(p.relative_to(self.root)) for p in self.root.rglob("*.json")
                      if str(p.relative_to(self.root)).startswith(prefix))

    def get(self, key: str):
        return (self.root / key).read_bytes()


def _s3():
    """The iq-blobs bucket, through the Store facet's S3 client and /etc/iq/creds.json (as md_observe)."""
    import os
    from pathlib import Path
    sys.path.insert(0, str(_store_dir()))
    from s3 import S3Client
    c = json.loads(Path(os.environ.get("IQ_CREDS", "/etc/iq/creds.json")).read_text())
    return S3Client(c.get("IKIGAI_S3_ENDPOINT", "https://ewr1.vultrobjects.com"), c.get("IKIGAI_S3_BUCKET", "iq-blobs"),
                    c["S3_ACCESS_KEY"], c["S3_SECRET"], c.get("IKIGAI_S3_REGION", "us-east-1"))


if __name__ == "__main__":
    raise SystemExit(main())
