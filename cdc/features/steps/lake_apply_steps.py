"""Steps for lake_apply.feature: the box is the run's scratch Postgres (context.dsn), the global cluster a second
pgembed Postgres for the run, the lake's files a temporary directory.

@use_case Development/UseCase/UC_CDC
@facet Gherkin
"""
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

import psycopg
from behave import given, then, when

import apply as APPLY
import cdc
import identity_map
import relay

sys.path.insert(0, str(Path(__file__).resolve().parents[5] / "GherkinIq" / "Facet" / "Store"))
import lake  # noqa: E402

SOURCE, TARGET = "ducklake", "ducklake_box"
_GLOBAL = {}


class _Store:
    def __init__(self):
        self.blobs = {}

    def put(self, key, body):
        self.blobs[key] = body

    def get(self, key):
        return self.blobs.get(key)

    def list(self, prefix=""):
        return sorted(k for k in self.blobs if k.startswith(prefix))


class _Server:
    def __init__(self):
        self.store = _Store()
        self.relay = relay.EventRelay(self.store, resolve_session=lambda s: "iq:subj:box" if s else None)
        self.up = True

    def probe(self):
        return (True, 200) if self.up else (False, 0)

    def have(self, keys):
        return self.relay.have(keys)

    def send(self, records):
        res = self.relay.accept(records, session="paired")
        if not res["accepted"]:
            raise RuntimeError("relay refused batch: " + res["reason"])
        return res["written"]


def _global_dsn() -> str:
    if "dsn" not in _GLOBAL:
        import pgembed
        d = tempfile.mkdtemp(prefix="iqgl", dir="/tmp")
        srv = pgembed.get_server(d, cleanup_mode="delete")
        srv.ensure_pgdata_inited()
        srv.ensure_postgres_running()
        _GLOBAL.update(server=srv, dsn="postgresql://postgres@/postgres?host=" + d)
    return _GLOBAL["dsn"]


def _sql(dsn, q, args=None):
    with psycopg.connect(dsn, autocommit=True) as c:
        cur = c.execute(q, args)
        return cur.fetchall() if cur.description else None


def _box_env(context):
    return {"IQ_LAKE_CATALOG": context.dsn, "IQ_LAKE_SCHEMA": SOURCE, "IQ_LAKE_DATA": context.lake_data}


def _replica_env(context):
    return {"IQ_LAKE_CATALOG": _global_dsn(), "IQ_LAKE_SCHEMA": TARGET}


def _fresh(context, publish=True):
    cdc.setup(context.dsn, restart_cmd=lambda: cdc.restart(context.pgdata))
    context.state = os.path.join(context.pgdata, "cdc-state", "lake-snapshot.json")
    cdc.teardown(context.dsn, context.state)
    _sql(context.dsn, "DROP SCHEMA IF EXISTS %s CASCADE" % SOURCE)
    _sql(_global_dsn(), "DROP SCHEMA IF EXISTS %s CASCADE" % TARGET)
    _sql(_global_dsn(), "DROP SCHEMA IF EXISTS iq_cdc_apply CASCADE")
    context.lake_data = tempfile.mkdtemp(prefix="iq-lake-")
    context.add_cleanup(shutil.rmtree, context.lake_data, True)
    assert cdc.setup(context.dsn)["ok"]
    if publish:
        assert cdc.publish_schema(context.dsn, SOURCE)["ok"]
    context.server, context.tokens, context.conn = _Server(), identity_map.IdentityMap("k"), None


def _streamer(context):
    context.conn = context.conn or psycopg.connect(context.dsn, autocommit=True)
    return cdc.Streamer(context.conn, up=context.server.probe, deid=cdc.Deidentifier(context.tokens),
                        transport=context.server, host="box", state=context.state)


def _write(context, n, table):
    w = lake.connect(_box_env(context))
    w.execute("CREATE OR REPLACE TABLE iq_main.%s AS SELECT 'op' || i AS operation_id, i AS n FROM range(%d) t(i)"
              % (table, n))
    w.close()


def _apply(context):
    assert APPLY.prepare_lake(_global_dsn(), TARGET)["ok"]
    rep = APPLY.replica(_global_dsn(), TARGET)
    try:
        r = APPLY.apply(rep, APPLY.records_in(context.server.store), host="box", source_schema=SOURCE)
    finally:
        rep.close()
    assert r["ok"], r
    return r


@given("the lake's catalog published for CDC on a fresh box")
def _given_fresh(context):
    _fresh(context)


@given("the IQ Server is up for the lake")
def _given_up(context):
    context.server.up = True


@given('the box wrote {n:d} rows into "{table}" before the catalog was published')
def _given_before(context, n, table):
    cdc.teardown(context.dsn, context.state)
    _sql(context.dsn, "DROP SCHEMA IF EXISTS %s CASCADE" % SOURCE)
    assert cdc.setup(context.dsn)["ok"]
    _write(context, n, table)


@when('the box writes {n:d} rows into "{table}"')
def _when_write(context, n, table):
    _write(context, n, table)


@when('the box writes an account "{email}"')
def _when_account(context, email):
    w = lake.connect(_box_env(context))
    w.execute("CREATE OR REPLACE TABLE iq_main.accounts AS SELECT * FROM (VALUES ('amy@example.com'), (?),"
              " ('zed@example.com')) t(email)", [email])
    w.close()


@when("CDC ships a cycle and the global side applies it")
def _when_cycle_apply(context):
    r = _streamer(context).cycle()
    assert r.get("ok", True) and r.get("sent", 0) > 0, r
    context.applied = _apply(context)


@when("the catalog is published and CDC ships its snapshot")
def _when_snapshot(context):
    assert cdc.publish_schema(context.dsn, SOURCE)["ok"]
    context.snap = cdc.snapshot(context.dsn, _streamer(context), context.state)
    assert context.snap["ok"], context.snap


@when("the global side applies it")
def _when_apply(context):
    context.applied = _apply(context)


@when("the global side applies again")
def _when_again(context):
    context.again = _apply(context)


@then('a read-only reader of the replica counts {n:d} rows in "{table}"')
def _then_count(context, n, table):
    r = lake.connect(_replica_env(context), read_only=True)
    got = r.execute("SELECT count(*) FROM %s" % table).fetchone()[0]
    r.close()
    assert got == n, (got, n)


@then('a read-only reader of the replica finds "{email}" in "{table}"')
def _then_finds(context, email, table):
    r = lake.connect(_replica_env(context), read_only=True)
    got = r.execute("SELECT email FROM %s WHERE email = ?" % table, [email]).fetchall()
    r.close()
    assert got == [(email,)], got


@then("the replica holds as many lake snapshots as the box")
def _then_snapshots(context):
    b = lake.connect(_box_env(context), read_only=True)
    r = lake.connect(_replica_env(context), read_only=True)
    nb, nr = len(lake.snapshots(b)), len(lake.snapshots(r))
    b.close()
    r.close()
    assert nb == nr, (nb, nr)


@then("every table in the box's catalog is REPLICA IDENTITY FULL")
def _then_full(context):
    rows = _sql(context.dsn, "SELECT c.relname, c.relreplident FROM pg_class c JOIN pg_namespace n ON n.oid ="
                             " c.relnamespace WHERE n.nspname = %s AND c.relkind = 'r'", (SOURCE,))
    assert rows and all(k == "f" for _, k in rows), rows


@then("the second apply applied nothing")
def _then_nothing(context):
    assert context.again["applied"] == 0 and context.again["skipped"] > 0 and context.again["base"] is None, context.again


@then("the replica's statistics carry no identity token")
def _then_no_token(context):
    for t in ("ducklake_file_column_stats", "ducklake_table_column_stats"):
        rows = _sql(_global_dsn(), "SELECT min_value, max_value FROM %s.%s" % (TARGET, t))
        assert not any(v and "iq:subj:" in v for row in rows for v in row), (t, rows)


@then("the apply started from the snapshot")
def _then_base(context):
    assert context.applied["base"] and context.applied["base"]["rows"] > 0, context.applied
