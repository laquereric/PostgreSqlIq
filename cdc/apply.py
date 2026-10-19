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
    watermark  iq_cdc_apply.applied (host, source_schema, target_schema) -> the last commit LSN applied, written
               in the same transaction as the changes. A re-run applies only what is past it.
    stats      CDC turns every e-mail in a value into an identity token (`iq:subj:`). A min/max statistic that
               carries one no longer bounds the data it describes, so it is applied as NULL: the reader scans
               that file instead of pruning it. Fewer skips, never a wrong answer.

CDC ships no DDL, so the target's tables must exist first: `prepare_lake` has DuckLake create its own catalog
tables in the target schema, then empties them for the replay.

    python apply.py lake <target-dsn> <target-schema> --host <box> [--schema ducklake] [--blobs <dir>]

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
WATERMARK = "iq_cdc_apply.applied"


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


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


def _target(table: str, schema: str) -> str:
    return "%s.%s" % (_q(schema), _q(table.split(".", 1)[1]))


def _values(v):
    from psycopg.types.json import Jsonb
    return Jsonb(v) if isinstance(v, (dict, list)) else v


def _insert(c, table: str, row: dict):
    cols = list(row)
    c.execute("INSERT INTO %s (%s) VALUES (%s)" % (table, ", ".join(map(_q, cols)), ", ".join(["%s"] * len(cols))),
              [_values(row[k]) for k in cols])


def _match(c, table: str, old: dict) -> str:
    return "ctid = (SELECT ctid FROM %s WHERE %s LIMIT 1)" % (
        table, " AND ".join("%s IS NOT DISTINCT FROM %%s" % _q(k) for k in old))


def _change(c, rec: dict, schema: str):
    op = rec["op"]
    if op == "cdc:truncate":
        c.execute("TRUNCATE %s" % ", ".join(_target(t, schema) for t in _tables(rec)))
        return
    table, new, old = _target(rec["table"], schema), scrub(rec.get("new")), scrub(rec.get("old"))
    if op == "cdc:insert":
        _insert(c, table, new)
    elif op == "cdc:update":
        if not old:
            raise ValueError("update without its old row on %s: publish the schema REPLICA IDENTITY FULL" % table)
        cur = c.execute("UPDATE %s SET %s WHERE %s" % (table, ", ".join("%s = %%s" % _q(k) for k in new),
                                                       _match(c, table, old)),
                        [_values(v) for v in new.values()] + [_values(v) for v in old.values()])
        if cur.rowcount != 1:
            raise LookupError("update found no row on %s at %s" % (table, rec["lsn"]))
    elif op == "cdc:delete":
        cur = c.execute("DELETE FROM %s WHERE %s" % (table, _match(c, table, old)), [_values(v) for v in old.values()])
        if cur.rowcount != 1:
            raise LookupError("delete found no row on %s at %s" % (table, rec["lsn"]))


def _watermark(c, host: str, source: str, target: str) -> int:
    c.execute("CREATE SCHEMA IF NOT EXISTS iq_cdc_apply")
    c.execute("CREATE TABLE IF NOT EXISTS %s (host text, source_schema text, target_schema text, commit_lsn text,"
              " applied_at timestamptz DEFAULT now(), PRIMARY KEY (host, source_schema, target_schema))" % WATERMARK)
    r = c.execute("SELECT commit_lsn FROM %s WHERE host=%%s AND source_schema=%%s AND target_schema=%%s" % WATERMARK,
                  (host, source, target)).fetchone()
    return cdc.lsn_int(r[0]) if r else -1


def _mark(c, host: str, source: str, target: str, lsn: int):
    c.execute("INSERT INTO %s (host, source_schema, target_schema, commit_lsn) VALUES (%%s, %%s, %%s, %%s)"
              " ON CONFLICT (host, source_schema, target_schema) DO UPDATE SET commit_lsn = excluded.commit_lsn,"
              " applied_at = now()" % WATERMARK, (host, source, target, cdc.lsn_text(lsn)))


def apply(conn, records, *, host: str, source_schema: str, target_schema: str) -> dict:
    """Replay `records` (any mix; only this host's, for this schema, are used) into `target_schema` on `conn`
    (psycopg, autocommit). Returns {ok, base, applied, skipped, watermark} or {ok: False, reason, detail}."""
    ours = sorted((r for r in records if _ours(r, host, source_schema)), key=_key)
    with conn.transaction():
        mark = _watermark(conn, host, source_schema, target_schema)
    snaps = [r for r in ours if r["op"] == "cdc:snapshot"]
    base, applied, skipped = None, 0, 0
    if snaps:
        s_lsn = max(cdc.lsn_int(r["lsn"]) for r in snaps)
        if s_lsn > mark:
            rows = [r for r in snaps if cdc.lsn_int(r["lsn"]) == s_lsn]
            with conn.transaction():
                held = [t for (t,) in conn.execute("SELECT tablename FROM pg_tables WHERE schemaname=%s",
                                                   (target_schema,))]
                if held:
                    conn.execute("TRUNCATE %s" % ", ".join("%s.%s" % (_q(target_schema), _q(t)) for t in held))
                for r in rows:
                    _insert(conn, _target(r["table"], target_schema), scrub(r["new"]))
                _mark(conn, host, source_schema, target_schema, s_lsn)
            mark, base = s_lsn, {"lsn": cdc.lsn_text(s_lsn), "rows": len(rows)}
    stream = [r for r in ours if r["op"] != "cdc:snapshot"]
    for commit, txn in groupby(stream, key=lambda r: cdc.lsn_int(r["commit_lsn"])):
        txn = list(txn)
        if commit <= mark:
            skipped += len(txn)
            continue
        try:
            with conn.transaction():
                for r in txn:
                    _change(conn, r, target_schema)
                _mark(conn, host, source_schema, target_schema, commit)
        except Exception as exc:  # noqa: BLE001 -- the transaction rolled back; stop at it, report it
            return {"ok": False, "reason": "apply_failed", "detail": str(exc).splitlines()[0][:200],
                    "at": cdc.lsn_text(commit), "base": base, "applied": applied, "skipped": skipped,
                    "watermark": cdc.lsn_text(mark) if mark >= 0 else None}
        applied, mark = applied + len(txn), commit
    return {"ok": True, "base": base, "applied": applied, "skipped": skipped,
            "watermark": cdc.lsn_text(mark) if mark >= 0 else None}


def prepare_lake(target_dsn: str, target_schema: str) -> dict:
    """The target's catalog tables, created by DuckLake itself (so the DDL is the reader's own version's), then
    emptied: what they hold comes from the box. Done once; a schema already holding the catalog is left alone."""
    import psycopg
    with psycopg.connect(target_dsn, autocommit=True) as c:
        if c.execute("SELECT 1 FROM pg_tables WHERE schemaname=%s AND tablename='ducklake_metadata'",
                     (target_schema,)).fetchone():
            return {"ok": True, "created": False}
    sys.path.insert(0, str(_store_dir()))
    import lake
    import tempfile
    lake.connect({"IQ_LAKE_CATALOG": target_dsn, "IQ_LAKE_SCHEMA": target_schema,
                  "IQ_LAKE_DATA": tempfile.mkdtemp(prefix="iq-lake-prepare-")}).close()
    with psycopg.connect(target_dsn, autocommit=True) as c:
        held = [t for (t,) in c.execute("SELECT tablename FROM pg_tables WHERE schemaname=%s", (target_schema,))]
        c.execute("TRUNCATE %s" % ", ".join("%s.%s" % (_q(target_schema), _q(t)) for t in held))
    return {"ok": True, "created": True, "tables": len(held)}


def _store_dir():
    from pathlib import Path
    return Path(__file__).resolve().parents[3] / "GherkinIq" / "Facet" / "Store"


def main(argv=None) -> int:
    import argparse
    import psycopg
    ap = argparse.ArgumentParser(prog="apply.py", description=__doc__.split("\n\n")[0])
    ap.add_argument("what", choices=["lake"])
    ap.add_argument("target_dsn")
    ap.add_argument("target_schema")
    ap.add_argument("--host", required=True, help="the box whose records to replay (the record's `host`)")
    ap.add_argument("--schema", default="ducklake", help="the source schema on the box")
    ap.add_argument("--blobs", help="a local directory of records (default: the iq-blobs bucket, S3 creds)")
    a = ap.parse_args(argv)
    store = DirStore(a.blobs) if a.blobs else _s3()
    print(json.dumps(prepare_lake(a.target_dsn, a.target_schema)))
    with psycopg.connect(a.target_dsn, autocommit=True) as c:
        r = apply(c, records_in(store), host=a.host, source_schema=a.schema, target_schema=a.target_schema)
    print(json.dumps(r))
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
