# @use_case Development/UseCase/UC_CDC
# @facet Store
# @status Proposed
"""CDC: the box's own Postgres streamed to the IQ Server, only while the IQ Server is up (UC_CDC slice 1).

The store standard is pgembed Postgres (IQ_REGISTRY_DSN, `UC_App/iq-pg.sh`). Its changes are read the way the
README recommends, from the WAL, with nothing new installed:

  * **logical decoding** (`wal_level = logical`) through the built-in **pgoutput** plugin -- the image's pgembed
    ships pgoutput and neither wal2json nor test_decoding;
  * a **publication** (`iq_cdc`) names what is captured, by default every table in `public`;
  * a **replication slot** (`iq_cdc`) is the cursor -- it keeps the WAL until the IQ Server has the change.

The streamer reads the slot over plain SQL (`pg_logical_slot_peek_binary_changes`, protocol 1, decoded here), so
no replication connection and no psycopg2. It **peeks**, never gets: a change leaves the slot only after the IQ
Server has acknowledged it (`pg_replication_slot_advance` to the commit's end LSN). Every change is one record on
the existing UC_LaptopCloudBridge relay (`POST /events` on memory.*), so the server side needs nothing new:

    {operation_id: iq:op:<sha256(system id, change LSN)[:32]>, op: cdc:insert|update|delete|truncate,
     decision: captured, service: iq-cdc, at: <commit time UTC>, table, lsn, commit_lsn, xid, new, old}

The operationId is a function of the change, so a change resent after a crash between ack and advance is the
same canonical key and the relay's have/missing answer drops it -- at-least-once on the wire, once in the store.

**Only while the IQ Server is up.** Each cycle first asks `{server}/whoami` with the paired credential; anything
but a 200 reads nothing and advances nothing, so the WAL waits in the slot. **Never autostarted**: no launchd job,
no app hook -- `iq-cdc.sh run` streams in the foreground until stopped. Because a slot that nobody reads keeps WAL
forever, setup caps it (`max_slot_wal_keep_size`, default 1GB): past that Postgres invalidates the slot rather
than fill the disk, and `status` says so.

**The rows that were there first.** The slot captures from its creation on; `iq-cdc.sh snapshot` ships what
existed before: it drains the slot, reads every published table in one repeatable-read transaction, ships each
row as `cdc:snapshot` (operationId from the row's content), and records the transaction's `pg_current_snapshot()`.
From then on the streamer advances over any transaction visible in that snapshot -- committed after the drain but
before the snapshot, so already in the rows -- and ships the rest: nothing twice, nothing missed. While a
snapshot is incomplete the streamer pauses; re-running it ships only what the server lacks.

**Only some rows.** `iq-cdc.sh filter <table> "<where>"` sets a publication row filter. An update that moves a
row out of the filter ships as a delete, and one that moves it in ships as an insert (Postgres's own rewrite). The
snapshot applies the same filter. A filter Postgres would let break the table's writers (a column outside the
replica identity) is refused before it lands, unless `--full`.

**Only some columns.** `iq-cdc.sh columns <table> a,b,c` sets a publication column list: a column not
published never leaves the box, in the stream or the snapshot. The list must include the replica identity, and
one that does not is refused before it lands. Filters and column lists are kept together: setting one leaves the
other as it is.

**Who never leaves the box.** Records cross the same de-identification the bridge's Flusher applies (`subject`
dropped, `subject_iri` and any `iq:identity:` value tokenized by the IdentityMap); in addition any e-mail address
in a captured value becomes the token of its identity, since a table row is not a log and may hold one anywhere.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import struct
import sys
import time
from datetime import datetime, timedelta, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
_BRIDGE = os.path.abspath(os.path.join(_HERE, "..", "UC_LaptopCloudBridge"))


def _bridge(name: str):
    """A UC_LaptopCloudBridge module (flush: de-identification; relay: canonical keys + the HTTP transport; the
    identity map's client), loaded when first needed -- reading the lag (the Dashboard) never loads them."""
    if _BRIDGE not in sys.path:
        sys.path.insert(0, _BRIDGE)
    import importlib
    return importlib.import_module(name)

SLOT = "iq_cdc"
PUBLICATION = "iq_cdc"
SERVICE = "iq-cdc"
WAL_CAP = "1GB"
BATCH = 200                     # records per POST /events
PG_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


def dsn() -> str:
    """The box's Postgres: IQ_REGISTRY_DSN, else iq-pg.sh's default (unix socket under ~/.iq/pg)."""
    return os.environ.get("IQ_REGISTRY_DSN") or "postgresql://postgres@/iq?host=" + os.path.expanduser(
        os.environ.get("IQ_PGDATA", "~/.iq/pg"))


def subject_iri(email: str) -> str:
    """iq:identity:<sha256(lower(email))[:24]> -- the auth identity hash (Sites/memory-intentionalquality-com/observ)."""
    return "iq:identity:" + hashlib.sha256(email.strip().lower().encode()).hexdigest()[:24]


# -- pgoutput protocol 1 --------------------------------------------------------------------------------------
class _Buf:
    def __init__(self, b: bytes):
        self.b, self.i = b, 0

    def take(self, fmt: str):
        v = struct.unpack_from("!" + fmt, self.b, self.i)
        self.i += struct.calcsize("!" + fmt)
        return v[0] if len(v) == 1 else v

    def byte(self) -> str:
        c = chr(self.b[self.i])
        self.i += 1
        return c

    def cstr(self) -> str:
        j = self.b.index(b"\0", self.i)
        s = self.b[self.i:j].decode()
        self.i = j + 1
        return s

    def raw(self, n: int) -> bytes:
        s = self.b[self.i:self.i + n]
        self.i += n
        return s


def lsn_text(v: int) -> str:
    return "%X/%X" % (v >> 32, v & 0xFFFFFFFF)


def lsn_int(s: str) -> int:
    hi, lo = s.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def _ts(us: int) -> str:
    return (PG_EPOCH + timedelta(microseconds=us)).isoformat().replace("+00:00", "Z")


# text -> JSON value by the column's type oid; everything else stays the text Postgres sent
_INT = {20, 21, 23, 26}
_FLOAT = {700, 701}
_JSON = {114, 3802}


def _value(oid: int, text: str):
    if oid == 16:
        return text == "t"
    if oid in _INT:
        return int(text)
    if oid in _FLOAT:
        return float(text)
    if oid in _JSON:
        return json.loads(text)
    return text


def _tuple(buf: _Buf, cols, key_only: bool = False) -> dict:
    """TupleData. A key-only old tuple ('K') carries every column, nulls for the non-key ones: only the key is
    claimed."""
    out = {}
    for k in range(buf.take("h")):
        name, oid, is_key = cols[k] if k < len(cols) else ("col%d" % k, 25, True)
        kind = buf.byte()
        if kind in "tb":
            data = buf.raw(buf.take("i"))
            v = _value(oid, data.decode()) if kind == "t" else data.hex()
        elif kind == "n":
            v = None
        else:
            continue                            # 'u' unchanged TOAST: not sent, so not claimed
        if is_key or not key_only:
            out[name] = v
    return out


class Decoder:
    """pgoutput (protocol 1) messages -> change dicts. Relations are learnt from 'R' messages, which pgoutput sends
    before a relation's first change in each decoding call. Begin/commit frame the transaction; every change of a
    transaction is completed with its commit's end LSN and time when the commit arrives."""

    def __init__(self):
        self.rel = {}                           # relid -> (schema.table, [(col, type oid)])
        self.txn = []                           # the open transaction's changes
        self.xid = None
        self.at_lsn = {}                        # LSN -> rows seen under it in the open transaction

    def feed(self, lsn: str, data: bytes):
        """One message. Returns the closed transaction's changes on its commit, a ("commit", end_lsn) marker for
        a transaction with none, or None."""
        b = _Buf(bytes(data))
        t = b.byte()
        if t == "B":
            b.take("q")
            b.take("q")
            self.xid, self.txn, self.at_lsn = b.take("i"), [], {}
        elif t == "R":
            relid, ns, name = b.take("I"), b.cstr(), b.cstr()
            b.byte()
            cols = []
            for _ in range(b.take("h")):
                flags = b.take("b")
                cname = b.cstr()
                oid = b.take("I")
                b.take("i")
                cols.append((cname, oid, bool(flags & 1)))      # flag 1: part of the replica identity key
            self.rel[relid] = ("%s.%s" % (ns, name), cols)
        elif t in "IUD":
            relid = b.take("I")
            table, cols = self.rel.get(relid, ("relid:%d" % relid, []))
            ch = {"op": {"I": "insert", "U": "update", "D": "delete"}[t], "table": table, "lsn": lsn,
                  "xid": self.xid, "old": None, "new": None}
            # One WAL record can carry many rows (a COPY is a multi-insert): they share its LSN, so their place
            # under it is part of their identity. seq 0 is the record's first row.
            seq = self.at_lsn.get(lsn, 0)
            self.at_lsn[lsn] = seq + 1
            if seq:
                ch["seq"] = seq
            k = b.byte()
            if k in "KO":                       # old key ('K') or whole old row ('O', REPLICA IDENTITY FULL)
                ch["old"] = _tuple(b, cols, key_only=(k == "K"))
                k = b.byte() if t == "U" else None
            if k == "N":
                ch["new"] = _tuple(b, cols)
            self.txn.append(ch)
        elif t == "T":
            n = b.take("i")
            b.take("b")
            tables = [self.rel.get(r, ("relid:%d" % r, []))[0] for r in (b.take("I") for _ in range(n))]
            self.txn.append({"op": "truncate", "table": ",".join(tables), "lsn": lsn, "xid": self.xid,
                             "old": None, "new": None})
        elif t == "C":
            b.take("b")
            b.take("q")
            end, at = lsn_text(b.take("q")), _ts(b.take("q"))
            self.last_end = end
            done, self.txn = self.txn, []
            for ch in done:
                ch["commit_lsn"], ch["at"] = end, at
            return done if done else ("commit", end)
        return None                             # 'O' origin, 'Y' type: nothing to ship


# -- the slot ---------------------------------------------------------------------------------------------
def _connect(d: str):
    import psycopg
    return psycopg.connect(d, autocommit=True, connect_timeout=5)


def setup(d: str, tables=(), *, restart_cmd=None, wal_cap: str = WAL_CAP) -> dict:
    """Make the box's Postgres capturable: wal_level=logical (+ a WAL cap for idle slots), the publication, the
    slot. Idempotent. wal_level needs a restart: done by `restart_cmd()` when given, else reported as needed."""
    with _connect(d) as c:
        level = c.execute("show wal_level").fetchone()[0]
        if level != "logical":
            c.execute("ALTER SYSTEM SET wal_level = 'logical'")
        if c.execute("show max_slot_wal_keep_size").fetchone()[0] in ("-1", "-1kB"):
            c.execute("ALTER SYSTEM SET max_slot_wal_keep_size = %s" % _lit(wal_cap))
            c.execute("select pg_reload_conf()")
    if level != "logical":
        if restart_cmd is None:
            return {"ok": False, "reason": "restart_needed",
                    "detail": "wal_level is %s; set to logical, takes effect on restart" % level}
        restart_cmd()
    with _connect(d) as c:
        if not c.execute("select 1 from pg_publication where pubname=%s", (PUBLICATION,)).fetchone():
            target = ("TABLE " + ", ".join(_ident(t) for t in tables)) if tables else "TABLES IN SCHEMA public"
            c.execute("CREATE PUBLICATION %s FOR %s" % (PUBLICATION, target))
        if not c.execute("select 1 from pg_replication_slots where slot_name=%s", (SLOT,)).fetchone():
            c.execute("select pg_create_logical_replication_slot(%s, 'pgoutput')", (SLOT,))
    return {"ok": True, **status(d)}


def teardown(d: str, state: str = "") -> dict:
    """Drop the slot (so it stops holding WAL), the publication and the snapshot state (its boundary belonged to
    that slot). wal_level is left as it is."""
    if state and os.path.exists(state):
        os.remove(state)
    with _connect(d) as c:
        slot = bool(c.execute("select 1 from pg_replication_slots where slot_name=%s", (SLOT,)).fetchone())
        if slot:
            c.execute("select pg_drop_replication_slot(%s)", (SLOT,))
        c.execute("DROP PUBLICATION IF EXISTS %s" % PUBLICATION)
        c.execute("DROP EVENT TRIGGER IF EXISTS %s" % FULL_TRIGGER)
    return {"ok": True, "dropped_slot": slot}


FULL_TRIGGER = "iq_cdc_full_identity"


def publish_schema(d: str, schema: str) -> dict:
    """Add a whole schema to the publication with every table in it REPLICA IDENTITY FULL, now and when created
    later (an event trigger). For a schema whose tables have no key -- DuckLake's catalog (lake.py): published
    without an identity, Postgres refuses their UPDATE and DELETE, and the lake's writer breaks. FULL also makes an
    update's or delete's `old` the whole row, which is what apply.py matches on. Idempotent."""
    with _connect(d) as c:
        if not c.execute("select 1 from pg_publication where pubname=%s", (PUBLICATION,)).fetchone():
            return {"ok": False, "reason": "no_publication", "detail": "run setup first"}
        c.execute("CREATE SCHEMA IF NOT EXISTS %s" % _ident(schema))
        c.execute("""CREATE OR REPLACE FUNCTION %s() RETURNS event_trigger LANGUAGE plpgsql AS $f$
            DECLARE r record;
            BEGIN
              FOR r IN SELECT d.objid::regclass AS t FROM pg_event_trigger_ddl_commands() d
                       WHERE d.object_type = 'table' AND d.schema_name IN (
                         SELECT n.nspname FROM pg_publication_namespace pn
                         JOIN pg_namespace n ON n.oid = pn.pnnspid
                         JOIN pg_publication p ON p.oid = pn.pnpubid
                         WHERE p.pubname = '%s' AND n.nspname <> 'public')
              LOOP EXECUTE format('ALTER TABLE %%s REPLICA IDENTITY FULL', r.t); END LOOP;
            END $f$""" % (FULL_TRIGGER, PUBLICATION))
        if not c.execute("select 1 from pg_event_trigger where evtname=%s", (FULL_TRIGGER,)).fetchone():
            c.execute("CREATE EVENT TRIGGER %s ON ddl_command_end WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS')"
                      " EXECUTE FUNCTION %s()" % (FULL_TRIGGER, FULL_TRIGGER))
        tables = [r[0] for r in c.execute("select tablename from pg_tables where schemaname=%s", (schema,))]
        for t in tables:
            c.execute("ALTER TABLE %s.%s REPLICA IDENTITY FULL" % (_ident(schema), _ident(t)))
        if not c.execute("select 1 from pg_publication_namespace pn join pg_namespace n on n.oid = pn.pnnspid "
                         "join pg_publication p on p.oid = pn.pnpubid where p.pubname=%s and n.nspname=%s",
                         (PUBLICATION, schema)).fetchone():
            c.execute("ALTER PUBLICATION %s ADD TABLES IN SCHEMA %s" % (PUBLICATION, _ident(schema)))
    return {"ok": True, "schema": schema, "tables": len(tables)}


def status(d: str) -> dict:
    with _connect(d) as c:
        level = c.execute("show wal_level").fetchone()[0]
        cap = c.execute("show max_slot_wal_keep_size").fetchone()[0]
        pub = c.execute("select puballtables, (select array_agg(n.nspname::text) from pg_publication_namespace pn "
                        "join pg_namespace n on n.oid=pn.pnnspid where pn.pnpubid=p.oid) "
                        "from pg_publication p where pubname=%s", (PUBLICATION,)).fetchone()
        spec = _spec(c) if pub else {}
        s = c.execute("select active, confirmed_flush_lsn::text, wal_status, "
                      "pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, "
                      "pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint, safe_wal_size "
                      "from pg_replication_slots where slot_name=%s", (SLOT,)).fetchone()
    out = {"wal_level": level, "wal_cap": cap, "publication": None, "slot": None}
    if pub:
        out["publication"] = {"schemas": pub[1] or [], "tables": sorted(spec),
                              "filters": {t: v["where"] for t, v in sorted(spec.items()) if v["where"]},
                              "columns": {t: v["columns"] for t, v in sorted(spec.items()) if v["columns"]}}
    if s:
        out["slot"] = {"active": s[0], "confirmed": s[1], "wal_status": s[2], "retained_bytes": s[3],
                       "behind_bytes": s[4], "safe_wal_bytes": s[5]}
    return out


def _ident(name: str) -> str:
    return ".".join('"%s"' % p.replace('"', '""') for p in name.split("."))


def _lit(v: str) -> str:
    return "'%s'" % v.replace("'", "''")


def system_id(c) -> str:
    return str(c.execute("select system_identifier from pg_control_system()").fetchone()[0])


def peek(c, limit: int = 1000, upto: str = None):
    """The slot's next whole transactions, not consumed: [(lsn, xid, bytes)]. With `upto`, decoding stops there; a
    read returning fewer than `limit` rows decoded everything up to it."""
    return c.execute("select lsn::text, xid::text, data from pg_logical_slot_peek_binary_changes"
                     "(%s, %s::pg_lsn, %s, 'proto_version', '1', 'publication_names', %s)",
                     (SLOT, upto, limit, PUBLICATION)).fetchall()


def flushed(c) -> str:
    return c.execute("select pg_current_wal_flush_lsn()::text").fetchone()[0]


def advance(c, lsn: str):
    c.execute("select pg_replication_slot_advance(%s, %s::pg_lsn)", (SLOT, lsn))


# -- changes -> relay records -----------------------------------------------------------------------------
def record(change: dict, sysid: str, host: str = "") -> dict:
    """A change as shipped. Its operationId is a function of where it is in the WAL: the change's LSN, and for the
    second and later rows of one WAL record (a COPY's multi-insert) its seq under that LSN -- without it those rows
    would share a key and all but one be dropped as already held. A first row keeps the key it always had."""
    seq = change.get("seq", 0)
    where = "%s:%s" % (sysid, change["lsn"]) + (":%d" % seq if seq else "")
    opid = hashlib.sha256(where.encode()).hexdigest()[:32]
    rec = {"operation_id": "iq:op:" + opid, "op": "cdc:" + change["op"], "decision": "captured",
            "service": SERVICE, "at": change["at"], "host": host, "table": change["table"],
            "lsn": change["lsn"], "commit_lsn": change["commit_lsn"], "xid": change["xid"],
            "new": change["new"], "old": change["old"]}
    if seq:
        rec["seq"] = seq
    return rec


class Deidentifier:
    """The bridge's Flusher de-identification, plus: an e-mail address anywhere in a value becomes the token of
    its identity. A tokenizer that cannot answer raises, and nothing ships."""

    def __init__(self, idmap):
        self.idmap = idmap
        self._f = _bridge("flush").Flusher(None, idmap, None)

    def __call__(self, rec: dict) -> dict:
        return self._emails(self._f.de_identify(rec))

    def _emails(self, obj):
        if isinstance(obj, dict):
            return {k: self._emails(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._emails(x) for x in obj]
        if isinstance(obj, str) and "@" in obj:
            return EMAIL.sub(lambda m: self.idmap.tokenize(subject_iri(m.group(0))), obj)
        return obj


# -- the IQ Server ----------------------------------------------------------------------------------------
def server_up(base: str, bearer: str, opener=None) -> tuple:
    """(up, http): up iff {base}/whoami answers 200 for the paired credential."""
    import urllib.error
    import urllib.request
    req = urllib.request.Request(base.rstrip("/") + "/whoami", headers={"Authorization": "Bearer " + bearer})
    try:
        with (opener or urllib.request.urlopen)(req, timeout=6) as r:
            return r.status == 200, r.status
    except urllib.error.HTTPError as e:
        return False, e.code
    except Exception:                           # unreachable, DNS, TLS, timeout: down
        return False, 0


class Streamer:
    """One cycle = if the server is up: peek, decode, de-identify, negotiate have/missing, ship in batches,
    advance the slot past each transaction whose every record the server holds. The connection, the server
    probe, the de-identifier and the transport (have/send) are injected. `state` is the snapshot state file: while
    a snapshot is incomplete the streamer pauses (unless it is the snapshot's own drain); once one is done, a
    transaction the snapshot already saw is advanced over, not shipped."""

    def __init__(self, conn, *, up, deid, transport, host: str = "", limit: int = 1000, batch: int = BATCH,
                 state: str = "", draining: bool = False):
        self.c, self.up, self.deid, self.t = conn, up, deid, transport
        self.host, self.limit, self.batch = host, limit, batch
        self.state, self.draining = state, draining
        self.sysid = system_id(conn)

    def cycle(self) -> dict:
        out = self._cycle()
        if self.state:                          # the last cycle, for the lag's reader (the Dashboard)
            write_state(os.path.join(os.path.dirname(self.state), "cycle.json"),
                        dict(out, at=datetime.now(timezone.utc).isoformat()))
        return out

    def _cycle(self) -> dict:
        ok, http = self.up()
        if not ok:
            return {"server": "down", "http": http, "sent": 0, "already": 0, "advanced": None}
        snap = read_state(self.state)
        if snap.get("status") == "running" and not self.draining:
            return {"server": "up", "http": http, "sent": 0, "already": 0, "advanced": None,
                    "paused": "snapshot incomplete -- run: iq-cdc.sh snapshot"}
        seen = snap.get("snapshot") if snap.get("status") == "done" else None
        upto = flushed(self.c)
        rows = peek(self.c, self.limit, upto)
        whole = len(rows) < self.limit           # decoded everything up to `upto`
        dec, txns, skipped = Decoder(), [], 0    # [(end_lsn, [records])]
        for lsn, _xid, data in rows:
            got = dec.feed(lsn, data)
            if isinstance(got, tuple):
                txns.append((got[1], []))
            elif got is not None:
                if seen and visible(seen, got[0]["xid"]):
                    skipped += len(got)          # its rows are in the snapshot already
                    txns.append((dec.last_end, []))
                    continue
                txns.append((got[0]["commit_lsn"], [record(ch, self.sysid, self.host) for ch in got]))
        out = {"server": "up", "http": http, "transactions": len(txns), "sent": 0, "already": 0,
               "advanced": None}
        if skipped:
            out["in_snapshot"] = skipped
        if not txns:
            if whole:                           # nothing published up to `upto`: the slot may stand there
                advance(self.c, upto)
                out["advanced"] = upto
            return out
        try:
            flat = [(end, self.deid(r)) for end, recs in txns for r in recs]
        except Exception as e:                  # no token, no shipping
            out["error"] = "de-identify: %s" % e
            return out
        sent, todo = 0, flat                    # until have() answers, everything is unacked
        try:
            todo, out["already"] = missing(self.t, flat)
            for i in range(0, len(todo), self.batch):
                self.t.send([r for _, r in todo[i:i + self.batch]])
                sent = min(i + self.batch, len(todo))
                out["sent"] = sent
        except Exception as e:                  # refused or gone mid-cycle: keep what was acked
            out["error"] = str(e)
        safe = self.safe_end(txns, todo, sent)
        if whole and sent >= len(todo) and not out.get("error"):
            safe = upto                         # every transaction up to `upto` is held: stand at `upto`
        if safe:
            advance(self.c, safe)
            out["advanced"] = safe
        return out

    @staticmethod
    def safe_end(txns, todo, sent):
        """The end LSN of the last transaction the server wholly holds: every transaction before the one holding
        the first unacked record (all of them when everything is acked, including those with nothing to send)."""
        if sent >= len(todo):
            return txns[-1][0]
        first_unacked, safe = todo[sent][0], None
        for end, _ in txns:
            if end == first_unacked:
                break
            safe = end
        return safe


def missing(transport, flat):
    """Ask the server which of these (end, record) it holds: (the rest, how many it had)."""
    keys = [_bridge("relay").key_for(r) for _, r in flat]
    have = set(transport.have(keys)) if keys else set()
    return [(e, r) for (e, r), k in zip(flat, keys) if k not in have], sum(k in have for k in keys)


# -- row filters ------------------------------------------------------------------------------------------
KEEP = object()                                 # "leave this part of a table's narrowing as it is"


def _spec(c) -> dict:
    """What the publication captures, per table: {schema.table: {"where": expr | None, "columns": [..] | None}}.
    Read from pg_publication_rel, where no column list (None) differs from a list naming every column (a column
    added later is not published); a schema-wide publication's tables come from pg_publication_tables, unnarrowed."""
    spec = {t: {"where": None, "columns": None} for (t,) in c.execute(
        "select schemaname||'.'||tablename from pg_publication_tables where pubname=%s", (PUBLICATION,))}
    for t, where, cols in c.execute(
            "select n.nspname||'.'||k.relname, pg_get_expr(r.prqual, r.prrelid), "
            "(select array_agg(a.attname::text order by a.attnum) from pg_attribute a "
            " where a.attrelid = r.prrelid and a.attnum = any(r.prattrs::int2[])) "
            "from pg_publication_rel r join pg_publication p on p.oid = r.prpubid "
            "join pg_class k on k.oid = r.prrelid join pg_namespace n on n.oid = k.relnamespace "
            "where p.pubname = %s", (PUBLICATION,)):
        spec[t] = {"where": where, "columns": cols}
    return spec


def narrow(d: str, table: str, *, where=KEEP, columns=KEEP, full: bool = False) -> dict:
    """Narrow what is captured of `table`: `where` a row filter, `columns` a column list (Postgres 15+); None
    clears that part, KEEP leaves it. Both need the table named in the publication, so a schema-wide publication
    becomes the list of the tables it covers now (new tables are then not captured until `reset_filters`).
    Postgres refuses UPDATE and DELETE on a table whose filter names a column outside its replica identity, or
    whose column list leaves a replica identity column out -- so every change is proved in its own transaction
    (a zero-row UPDATE and DELETE, which Postgres checks before touching a row) and rolled back if refused: a
    narrowing never breaks a writer. `full` sets REPLICA IDENTITY FULL first, which lets a filter name any column
    (and makes an update or delete carry the whole old row)."""
    import psycopg
    table = _qualified(d, table)
    with psycopg.connect(d) as c:               # one transaction: the change and its proof, or neither
        try:
            if not c.execute("select 1 from pg_publication where pubname=%s", (PUBLICATION,)).fetchone():
                return {"ok": False, "reason": "no_publication", "detail": "run: iq-cdc.sh setup"}
            spec = _spec(c)
            t = spec.setdefault(table, {"where": None, "columns": None})
            if where is not KEEP:
                t["where"] = where
            if columns is not KEEP:
                t["columns"] = list(columns) if columns else None
            if full:
                c.execute("ALTER TABLE %s REPLICA IDENTITY FULL" % _ident(table))
            c.execute("ALTER PUBLICATION %s SET TABLE %s" % (PUBLICATION, ", ".join(
                _ident(n) + (" (%s)" % ", ".join('"%s"' % x.replace('"', '""') for x in v["columns"])
                             if v["columns"] else "") + (" WHERE (%s)" % v["where"] if v["where"] else "")
                for n, v in sorted(spec.items()))))
            col = c.execute("select attname from pg_attribute where attrelid=%s::regclass and attnum > 0 "
                            "and not attisdropped order by attnum limit 1", (_ident(table),)).fetchone()[0]
            c.execute("UPDATE %s SET %s = %s WHERE false" % (_ident(table), _ident(col), _ident(col)))
            c.execute("DELETE FROM %s WHERE false" % _ident(table))
        except psycopg.Error as e:
            c.rollback()
            return {"ok": False, "reason": "refused", "table": table,
                    "detail": (e.diag.message_primary or str(e)).strip(),
                    "hint": (e.diag.message_detail or e.diag.message_hint or "")}
    pub = status(d)["publication"]
    return {"ok": True, "table": table, "filters": pub["filters"], "columns": pub["columns"],
            "tables": pub["tables"], "schemas": pub["schemas"]}


def set_filter(d: str, table: str, where: str = None, *, full: bool = False) -> dict:
    """Capture only the rows of `table` matching `where`; None clears the filter (its column list is kept)."""
    return narrow(d, table, where=where, full=full)


def set_columns(d: str, table: str, columns=None) -> dict:
    """Capture only these columns of `table` (they must include its replica identity); None or [] clears the
    list (its filter is kept). A column not published never leaves the box."""
    return narrow(d, table, columns=columns)


def reset_filters(d: str, schema: str = "public") -> dict:
    """Back to setup's default: every table in `schema`, no filters (a table's REPLICA IDENTITY is left as set)."""
    with _connect(d) as c:
        c.execute("ALTER PUBLICATION %s SET TABLES IN SCHEMA %s" % (PUBLICATION, _ident(schema)))
    pub = status(d)["publication"]
    return {"ok": True, "schemas": pub["schemas"], "tables": pub["tables"], "filters": pub["filters"]}


def _qualified(d: str, table: str) -> str:
    """schema.table as the catalog names it (a bare name resolves on the search path)."""
    with _connect(d) as c:
        return c.execute("select n.nspname||'.'||c.relname from pg_class c join pg_namespace n "
                         "on n.oid = c.relnamespace where c.oid = %s::regclass",
                         (_ident(table) if "." in table else '"%s"' % table,)).fetchone()[0]


# -- the lag ----------------------------------------------------------------------------------------------
def _bytes(cap: str):
    """max_slot_wal_keep_size as bytes ('1GB', '512MB', '-1' -> None)."""
    m = re.fullmatch(r"(\d+)\s*([kMGT]?B)?", cap or "")
    if not m or cap.startswith("-"):
        return None
    return int(m.group(1)) * {"kB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30, "TB": 1 << 40}.get(m.group(2) or "MB", 1)


def lag(d: str, state: str = "") -> dict:
    """How far the IQ Server is behind the box, read without touching the slot (a running streamer holds it):
    the WAL written since the slot's position (`behind_bytes`: unshipped, since an idle cycle moves the slot to the
    WAL's end), the WAL the slot holds against its cap, the snapshot, and the last cycle (when, server up or down).
    `health`: no slot | lost | at risk (unreserved, or over half the cap) | snapshot incomplete | behind (WAL
    unshipped and no cycle in 5 minutes) | ok."""
    st = status(d)
    snap = read_state(state) if state else {}
    last = read_state(os.path.join(os.path.dirname(state), "cycle.json")) if state else {}
    cap = _bytes(st["wal_cap"])
    out = {"slot": st["slot"], "wal_level": st["wal_level"], "wal_cap": st["wal_cap"], "cap_bytes": cap,
           "tables": (st["publication"] or {}).get("tables", []),
           "filters": (st["publication"] or {}).get("filters", {}),
           "columns": (st["publication"] or {}).get("columns", {}),
           "schemas": (st["publication"] or {}).get("schemas", []),
           "snapshot": {k: v for k, v in snap.items() if k != "tables"} or None, "last_cycle": last or None}
    s = st["slot"]
    age = None
    if last.get("at"):
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(last["at"])).total_seconds()
    out["last_cycle_age_s"] = None if age is None else round(age)
    if not s:
        out["health"] = "no slot"
    elif s["wal_status"] == "lost":
        out["health"] = "lost"
    elif s["wal_status"] == "unreserved" or (cap and s["retained_bytes"] > cap / 2):
        out["health"] = "at risk"
    elif snap.get("status") == "running":
        out["health"] = "snapshot incomplete"
    elif s["behind_bytes"] > 0 and (age is None or age > 300):
        out["health"] = "behind"
    else:
        out["health"] = "ok"
    return out


# -- the initial snapshot ---------------------------------------------------------------------------------
def state_path() -> str:
    return os.path.join(os.path.expanduser(os.environ.get("IQ_CDC_HOME", "~/.iq/cdc")), "snapshot.json")


def read_state(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def write_state(path: str, st: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path + ".tmp", "w") as f:
        json.dump(st, f, indent=2)
    os.replace(path + ".tmp", path)


def visible(snapshot: str, xid32: int) -> bool:
    """Whether a transaction (pgoutput's 32-bit xid) is visible in a pg_current_snapshot() 'xmin:xmax:xip,...':
    the xid is widened to the epoch nearest the snapshot's xmax (a streamed transaction is recent)."""
    xmin, xmax, xip = snapshot.split(":")
    xmin, xmax = int(xmin), int(xmax)
    x = (xmax & ~0xFFFFFFFF) | (int(xid32) & 0xFFFFFFFF)
    if x - xmax > 1 << 31:
        x -= 1 << 32
    elif xmax - x > 1 << 31:
        x += 1 << 32
    return x < xmin or (x < xmax and str(x) not in xip.split(","))


def snapshot_record(table: str, row: dict, sysid: str, snap: dict, host: str = "") -> dict:
    """A row as it stood at the snapshot. Its operationId is a function of the row's content, so re-running a
    snapshot over an unchanged row is the same key (dedup), and a row changed since is a new record."""
    body = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
    opid = hashlib.sha256(("%s:snapshot:%s:%s" % (sysid, table, body)).encode()).hexdigest()[:32]
    return {"operation_id": "iq:op:" + opid, "op": "cdc:snapshot", "decision": "captured", "service": SERVICE,
            "at": snap["at"], "host": host, "table": table, "lsn": snap["lsn"], "commit_lsn": snap["lsn"],
            "xid": None, "new": row, "old": None, "snapshot": snap["snapshot"]}


def snapshot(d: str, streamer: "Streamer", state: str, *, between=None, fetch: int = 1000) -> dict:
    """The rows that existed before the slot, shipped once, in line with the stream:
      1. mark the state running (the streamer pauses; a failure leaves it so, and a re-run starts again);
      2. drain the slot through `streamer` (the snapshot's own drain);
      3. read every published table in ONE repeatable-read transaction, recording pg_current_snapshot() and the
         WAL position, and ship the rows (have/missing, batches);
      4. mark it done with that snapshot: the streamer then advances over every transaction visible in it
         (committed after the drain but before the snapshot -- already in the rows), so nothing is shipped twice
         and nothing is missed.
    Needs the server up throughout; `between` (tests) runs after the drain, before the snapshot is taken."""
    ok, http = streamer.up()
    if not ok:
        return {"ok": False, "server": "down", "http": http}
    write_state(state, {"status": "running", "started": datetime.now(timezone.utc).isoformat()})
    streamer.draining = True
    try:
        while True:
            res = streamer.cycle()
            if res.get("error") or res["server"] != "up":
                return {"ok": False, "stage": "drain", **res}
            if not res["transactions"]:
                break
    finally:
        streamer.draining = False
    if between:
        between()
    import psycopg
    out = {"ok": True, "tables": {}, "sent": 0, "already": 0}
    with psycopg.connect(d) as c:               # not autocommit: one transaction, one snapshot
        c.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        snap_text, lsn, at = c.execute("select pg_current_snapshot()::text, pg_current_wal_lsn()::text, "
                                       "now()").fetchone()
        snap = {"snapshot": snap_text, "lsn": lsn,
                "at": at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")}
        tables = c.execute("select schemaname, tablename, rowfilter, attnames::text[] from pg_publication_tables "
                           "where pubname=%s order by 1, 2", (PUBLICATION,)).fetchall()
        try:
            for schema, name, rowfilter, published in tables:
                table = "%s.%s" % (schema, name)
                cols = c.execute("select attname, atttypid from pg_attribute where attrelid=%s::regclass "
                                 "and attnum > 0 and not attisdropped and attname = any(%s) order by attnum",
                                 (_ident(table), published)).fetchall()   # what the stream publishes, no more
                sel = ", ".join("%s::text" % _ident(n) for n, _ in cols)
                n = 0
                with c.cursor(name="iq_cdc_snapshot") as cur:
                    cur.execute("SELECT %s FROM %s%s" % (sel, _ident(table),
                                                         " WHERE " + rowfilter if rowfilter else ""))
                    while True:
                        rows = cur.fetchmany(fetch)
                        if not rows:
                            break
                        recs = [streamer.deid(snapshot_record(
                            table, {cn: (None if v is None else _value(oid, v)) for (cn, oid), v in zip(cols, r)},
                            streamer.sysid, snap, streamer.host)) for r in rows]
                        todo, had = missing(streamer.t, [(None, r) for r in recs])
                        for i in range(0, len(todo), streamer.batch):
                            streamer.t.send([r for _, r in todo[i:i + streamer.batch]])
                        out["sent"] += len(todo)
                        out["already"] += had
                        n += len(rows)
                out["tables"][table] = n
        except Exception as e:                  # the state stays running: a re-run ships only what is missing
            return {"ok": False, "stage": "rows", "error": str(e), **{k: out[k] for k in ("tables", "sent", "already")}}
    write_state(state, {"status": "done", **snap, "tables": out["tables"],
                        "finished": datetime.now(timezone.utc).isoformat()})
    return {**out, **snap}


# -- CLI --------------------------------------------------------------------------------------------------
def credentials() -> list:
    """The paired credentials to try, read afresh each time: $IQ_HARNESS_SESSION, then ~/.iq/harness-token. A
    long `run` outlives a token (the file is rotated in place; a shell's env copy goes stale), so the probe tries
    each and the cycle ships with the first the server accepts."""
    out = []
    env = os.environ.get("IQ_HARNESS_SESSION", "").strip()
    if env:
        out.append(env)
    try:
        with open(os.path.expanduser("~/.iq/harness-token")) as f:
            t = f.read().strip()
        if t and t not in out:
            out.append(t)
    except OSError:
        pass
    return out


class PairedProbe:
    """The `up` of a Streamer in prod: a 200 on /whoami for some paired credential, which then becomes the Bearer
    of the transport and the tokenizer for that cycle."""

    def __init__(self, base: str, *clients, opener=None):
        self.base, self.clients, self.opener = base, clients, opener

    def __call__(self):
        http = 0
        for bearer in credentials():
            ok, http = server_up(self.base, bearer, self.opener)
            if ok:
                for cl in self.clients:
                    cl.headers = dict(cl.headers, Authorization="Bearer " + bearer)
                return True, http
        return False, http


def restart(pgdata: str):
    """pg_ctl restart from the image's pgembed (never a Homebrew postgres)."""
    import subprocess
    import pgembed
    pg_ctl = os.path.join(os.path.dirname(pgembed.__file__), "pginstall", "bin", "pg_ctl")
    # no -o: restart reuses postmaster.opts, i.e. the unix-socket-only options pgembed started it with
    subprocess.run([pg_ctl, "-D", pgdata, "-m", "fast", "-w", "restart", "-l", os.path.join(pgdata, "restart.log")],
                   check=True, stdout=subprocess.DEVNULL)


def main(argv=None) -> int:
    import argparse
    p = argparse.ArgumentParser(prog="iq-cdc", description="Stream the box's Postgres to the IQ Server while it is up.")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("setup")
    s.add_argument("tables", nargs="*", help="schema.table to capture (default: every table in public)")
    s.add_argument("--restart", action="store_true", help="restart Postgres if wal_level must change")
    sub.add_parser("status")
    sub.add_parser("teardown")
    pb = sub.add_parser("publish", help="add a whole schema, REPLICA IDENTITY FULL (the lake's catalog: ducklake)")
    pb.add_argument("schema")
    sub.add_parser("snapshot", help="ship the rows that existed before the slot (drains the slot first)")
    f = sub.add_parser("filter", help="capture only matching rows of a table (publication row filter)")
    f.add_argument("table", nargs="?", help="schema.table (none: list the filters)")
    f.add_argument("where", nargs="?", help="a row filter expression, e.g. \"realm = 'root'\"")
    f.add_argument("--clear", action="store_true", help="capture every row of the table again")
    f.add_argument("--full", action="store_true", help="REPLICA IDENTITY FULL, so the filter may name any column")
    f.add_argument("--reset", action="store_true", help="back to every table in public, no filters")
    k = sub.add_parser("columns", help="capture only some columns of a table (publication column list)")
    k.add_argument("table", nargs="?", help="schema.table (none: list the column lists)")
    k.add_argument("names", nargs="?", help="comma-separated columns; must include the replica identity")
    k.add_argument("--clear", action="store_true", help="capture every column of the table again")
    for name in ("once", "run"):
        r = sub.add_parser(name)
        r.add_argument("--interval", type=float, default=10.0)
    a = p.parse_args(argv)
    d = dsn()
    if a.cmd == "setup":
        res = setup(d, a.tables, restart_cmd=(lambda: restart(os.path.expanduser(os.environ.get("IQ_PGDATA", "~/.iq/pg"))))
                    if a.restart else None)
        print(json.dumps(res, indent=2))
        return 0 if res["ok"] else 1
    if a.cmd == "filter":
        if a.reset:
            res = reset_filters(d)
        elif not a.table:
            pub = status(d)["publication"] or {}
            res = {"ok": True, "schemas": pub.get("schemas", []), "tables": pub.get("tables", []),
                   "filters": pub.get("filters", {})}
        elif a.clear == bool(a.where):
            print("[iq-cdc] give a WHERE expression or --clear")
            return 2
        else:
            res = set_filter(d, a.table, None if a.clear else a.where, full=a.full)
        print(json.dumps(res, indent=2))
        return 0 if res["ok"] else 1
    if a.cmd == "columns":
        if not a.table:
            pub = status(d)["publication"] or {}
            res = {"ok": True, "schemas": pub.get("schemas", []), "tables": pub.get("tables", []),
                   "columns": pub.get("columns", {})}
        elif a.clear == bool(a.names):
            print("[iq-cdc] give comma-separated columns or --clear")
            return 2
        else:
            res = set_columns(d, a.table, None if a.clear else [x.strip() for x in a.names.split(",") if x.strip()])
        print(json.dumps(res, indent=2))
        return 0 if res["ok"] else 1
    if a.cmd == "teardown":
        print(json.dumps(teardown(d, state_path())))
        return 0
    if a.cmd == "publish":
        res = publish_schema(d, a.schema)
        print(json.dumps(res, indent=2))
        return 0 if res["ok"] else 1
    base = os.environ.get("IQ_MEMORY", "https://memory.intentionalquality.com")
    creds = credentials()
    if a.cmd == "status":
        st = status(d)
        st["server"] = {"base": base, "up": PairedProbe(base)()[0]}
        lg = lag(d, state_path())
        st.update(snapshot=lg["snapshot"], last_cycle=lg["last_cycle"], health=lg["health"])
        print(json.dumps(st, indent=2))
        return 0
    if not creds:
        print("[iq-cdc] no credential paired (~/.iq/harness-token or IQ_HARNESS_SESSION) -- run iq-login first")
        return 2
    identity_map = _bridge("identity_map")
    st = status(d)
    if not st["slot"]:
        print("[iq-cdc] no slot %s -- run: iq-cdc.sh setup" % SLOT)
        return 2
    if st["slot"]["wal_status"] == "lost":
        print("[iq-cdc] slot %s lost its WAL (cap %s exceeded) -- teardown and setup again" % (SLOT, st["wal_cap"]))
        return 2
    with _connect(d) as c:
        tokenizer = identity_map.RemoteTokenizer(base, bearer=creds[0])
        transport = _bridge("relay").HttpRelayTransport(base, bearer=creds[0])
        st = Streamer(c, up=PairedProbe(base, tokenizer, transport), deid=Deidentifier(tokenizer),
                      transport=transport, host=os.uname().nodename,
                      state=state_path())
        if a.cmd == "snapshot":
            res = snapshot(d, st, state_path())
            print(json.dumps(res, indent=2))
            return 0 if res["ok"] else 1
        while True:
            res = st.cycle()
            if a.cmd == "once" or res["sent"] or res["already"] or res.get("error") or res.get("paused") \
                    or res["server"] == "down":
                print("[iq-cdc] %s %s" % (datetime.now().strftime("%H:%M:%S"), json.dumps(res)), flush=True)
            if a.cmd == "once":
                return 0 if not res.get("error") and not res.get("paused") and res["server"] == "up" else 1
            if res["server"] == "up" and res.get("transactions") and not res.get("error"):
                continue                        # more may be waiting: drain before sleeping
            time.sleep(a.interval)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(0)
