"""Steps for token_hop.feature: the domain is the run's scratch Postgres (context.dsn) with the data plane's own
table, the proxy a database on the same server, the IQ Server the bridge's relay over a dict.

@use_case Development/UseCase/UC_CDC
@facet Gherkin
"""
import json
import os
import sys
import uuid
from pathlib import Path

import psycopg
from behave import given, then, when

import apply as APPLY
import cdc
import identity_map
import relay

sys.path.insert(0, str(Path(__file__).resolve().parents[5] / "GherkinIq" / "Facet" / "Store"))
import dataplane  # noqa: E402
import revisions  # noqa: E402

STATUS, TOKEN, MAP = "iq:token-status:j1", "iq:token:j1", "iq:identity-map:abc"
SUBJECT = "iq:identity:ada"


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

    def probe(self):
        return True, 200

    def have(self, keys):
        return self.relay.have(keys)

    def send(self, records):
        res = self.relay.accept(records, session="paired")
        if not res["accepted"]:
            raise RuntimeError("relay refused batch: " + res["reason"])
        return res["written"]

    def records(self, svc):
        return [json.loads(v) for k, v in sorted(self.store.blobs.items()) if "/svc=%s/" % svc in k]


def _sql(dsn, q, args=None):
    with psycopg.connect(dsn, autocommit=True) as c:
        cur = c.execute(q, args)
        return cur.fetchall() if cur.description else None


def _hop(context, name):
    cdc.use_hop(name)
    context.add_cleanup(cdc.use_hop, "")


def _state(context):
    return os.path.join(context.pgdata, "cdc-state", "snapshot%s.json" % ("-" + cdc.HOP if cdc.HOP else ""))


@given("a domain store and a proxy")
def _domain(context):
    _hop(context, "proxy")
    cdc.teardown(context.dsn, _state(context))
    _sql(context.dsn, "DROP TABLE IF EXISTS plane_datum")
    context.domain = dataplane.Plane([dataplane.PostgresAdapter(context.dsn)])     # makes the plane's table
    name = "proxy_%s" % uuid.uuid4().hex[:8]
    _sql(context.dsn, "CREATE DATABASE %s" % name)
    context.proxy_dsn = context.dsn.replace("@/postgres?", "@/%s?" % name)
    context.proxy = dataplane.from_env({"IQ_RECORD_DSN": context.proxy_dsn}, roles=("record",))
    context.server, context.tokens, context.conn = _Server(), identity_map.IdentityMap("k"), None


@given("the proxy hop set up on the domain store")
def _setup(context):
    res = cdc.setup(context.dsn, restart_cmd=lambda: cdc.restart(context.pgdata))
    assert res["ok"], res


@given("the domain holding a token, its status and an identity mapping")
def _holding(context):
    assert revisions.revise(context.domain, TOKEN, {"jti": "j1", "sub": SUBJECT, "name": "laptop",
                                                    "scopes": ["mcp:call:*"], "iat": 1, "exp": None,
                                                    "revoked": False}).ok
    assert revisions.revise(context.domain, STATUS, {"jti": "j1", "exp": None, "revoked": False}).ok
    assert context.domain.put(dataplane.Datum(MAP, "Bronze", json.dumps({"token": "iq:subj:abc",
                                                                       "subject_iri": SUBJECT}))).ok


def _cycle(context):
    context.conn = context.conn or psycopg.connect(context.dsn, autocommit=True)
    st = cdc.Streamer(context.conn, up=context.server.probe, deid=cdc.Deidentifier(context.tokens),
                      transport=context.server, host="domain", state=_state(context))
    context.out = st.cycle()
    assert not context.out.get("error") and context.out["server"] == "up", context.out


def _apply(context):
    context.applied = APPLY.put_records(context.proxy, APPLY.records_in(context.server.store), host="domain",
                                        prefix="iq:token-status:")
    assert context.applied["ok"], context.applied


@when("the proxy hop runs one cycle")
def _one(context):
    _cycle(context)


@when("the proxy hop runs one cycle and the proxy applies it")
def _one_applied(context):
    _cycle(context)
    _apply(context)


@when("the proxy applies the hop again")
def _again(context):
    _apply(context)


@then("the server holds 1 record of the proxy hop, the token's status")
def _only_status(context):
    got = context.server.records("iq-cdc-proxy")
    assert [(r["op"], r["table"], r["new"]["iri"]) for r in got] == [
        ("cdc:insert", "public.plane_datum", STATUS + "@000001")], got


@then("no record of the proxy hop names the token's subject or the identity")
def _no_subject(context):
    blob = json.dumps(context.server.records("iq-cdc-proxy"))
    assert SUBJECT not in blob and "identity-map" not in blob and "iq:token:" not in blob, blob


def _proxy_iris(context):
    return json.loads(context.proxy.list("iq:").answer)["iris"]


@then("the proxy holds only the token's status, {state}")
def _proxy_status(context, state):
    assert all(i.startswith("iq:token-status:") for i in _proxy_iris(context)), _proxy_iris(context)
    rec = revisions.value(revisions.current(context.proxy, STATUS))
    assert rec == {"jti": "j1", "exp": None, "revoked": state == "revoked"}, rec


@when("the domain revokes the token")
def _revoke(context):
    assert revisions.revise(context.domain, STATUS, {"jti": "j1", "exp": None, "revoked": True}).ok


@then("it puts nothing new and holds {n:d} revisions")
def _nothing_new(context, n):
    assert context.applied["put"] == 0 and context.applied["already"] == n, context.applied
    assert len(_proxy_iris(context)) == n, _proxy_iris(context)


@when("the default hop ships the whole plane table")
def _default_ships(context):
    context.proxy_records = context.server.records("iq-cdc-proxy")
    cdc.use_hop("")
    cdc.teardown(context.dsn, _state(context))
    assert cdc.setup(context.dsn, ["public.plane_datum"])["ok"]
    from datetime import datetime, timezone
    with psycopg.connect(context.dsn) as c:     # the rows were there before this slot: ship them as a snapshot
        snap = {"snapshot": "", "lsn": c.execute("select pg_current_wal_lsn()::text").fetchone()[0],
                "at": datetime.now(timezone.utc).isoformat()}
        cols = ("iri", "tier", "media_type", "body", "derived_from", "recorded_at")
        rows = c.execute("select %s from plane_datum order by iri" % ", ".join(cols)).fetchall()
    recs = [cdc.snapshot_record("public.plane_datum", dict(zip(cols, (list(r[:4]) + ["{}", r[5]]))),
                                "sys", snap, "domain") for r in rows]
    context.server.send(recs)
    context.default_records = context.server.records("iq-cdc")
    assert len(context.default_records) == 3, context.default_records


@when("the proxy applies the default hop's records")
def _apply_default(context):
    context.applied = APPLY.put_records(context.proxy, APPLY.records_in(context.server.store), host="domain",
                                        prefix="iq:token-status:")


@then("it puts {p:d}, skips {s:d}, and holds only the token's status")
def _skipped(context, p, s):
    assert (context.applied["put"], context.applied["skipped"], context.applied["refused"]) == (p, s, []), context.applied
    assert _proxy_iris(context) == [STATUS + "@000001"], _proxy_iris(context)


@then("the status row has a different operationId on each hop")
def _distinct(context):
    change = {"op": "insert", "at": "t", "table": "public.plane_datum", "lsn": "0/1", "commit_lsn": "0/1",
              "xid": "1", "new": {}, "old": None}
    cdc.use_hop("")
    default = cdc.record(change, "sys")["operation_id"]
    cdc.use_hop("proxy")
    assert cdc.record(change, "sys")["operation_id"] != default


@then("setting up the proxy hop on another table is refused")
def _setup_refused(context):
    assert cdc.main(["--hop", "proxy", "setup", "public.notes"]) == 2


@then("resetting or clearing its filter is refused")
def _filter_refused(context):
    assert cdc.main(["--hop", "proxy", "filter", "--reset"]) == 2
    assert cdc.main(["--hop", "proxy", "filter", "public.plane_datum", "--clear"]) == 2
    assert cdc.main(["--hop", "proxy", "columns", "public.plane_datum", "iri"]) == 2


@then("its filter is the token-status prefix")
def _filter(context):
    cdc.use_hop("proxy")
    pub = cdc.status(context.dsn)["publication"]
    assert pub["tables"] == ["public.plane_datum"], pub
    assert "iq:token-status:%" in pub["filters"]["public.plane_datum"], pub


@when("the proxy hop is torn down")
def _teardown(context):
    cdc.use_hop("")
    assert cdc.setup(context.dsn)["ok"]                    # the default hop, as a box streaming to its domain has
    cdc.use_hop("proxy")
    cdc.teardown(context.dsn, _state(context))


@then("the default hop's slot is still there")
def _default_kept(context):
    rows = _sql(context.dsn, "select slot_name from pg_replication_slots order by 1")
    assert ("iq_cdc",) in rows and ("iq_cdc_proxy",) not in rows, rows
    assert _sql(context.dsn, "select 1 from pg_publication where pubname = 'iq_cdc_proxy'") == []
