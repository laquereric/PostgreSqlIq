"""Steps for cdc.feature: the scratch Postgres is real (pgoutput, a real slot); the IQ Server is the bridge's
EventRelay in-process, with a probe and refusals the scenario sets.

@use_case Development/UseCase/UC_CDC
@facet Gherkin
"""
import json
import os
import subprocess

import psycopg
from behave import given, then, when

import cdc
import identity_map
import relay


class _MemStore:
    def __init__(self):
        self.blobs = {}

    def put(self, key, body):
        self.blobs[key] = body

    def get(self, key):
        return self.blobs.get(key)

    def list(self, prefix=""):
        return [k for k in self.blobs if k.startswith(prefix)]


class _Server:
    """The IQ Server as the scenario sees it: up or down, refusing chosen sends, holding what it accepted."""

    def __init__(self):
        self.store = _MemStore()
        self.relay = relay.EventRelay(self.store, resolve_session=lambda s: "iq:subj:box" if s else None)
        self.up, self.refuse_next, self.refuse_nth, self.sends = True, 0, None, 0

    def probe(self):
        return (True, 200) if self.up else (False, 0)

    def have(self, keys):
        return self.relay.have(keys)

    def send(self, records):
        self.sends += 1
        if self.refuse_next > 0 or self.sends == self.refuse_nth:
            self.refuse_next -= 1
            raise RuntimeError("relay refused batch: unavailable")
        res = self.relay.accept(records, session="paired")
        if not res["accepted"]:
            raise RuntimeError("relay refused batch: " + res["reason"] + json.dumps(res.get("problems")))
        return res["written"]

    def records(self, svc="iq-cdc"):
        return [json.loads(v) for k, v in sorted(self.store.blobs.items()) if "/svc=%s/" % svc in k]


class _NoTokens:
    def tokenize(self, subject_iri):
        raise RuntimeError("identity map unreachable")


def _sql(context, q, args=None):
    with psycopg.connect(context.dsn, autocommit=True) as c:
        cur = c.execute(q, args)
        return cur.fetchall() if cur.description else None


def _replica(context):
    cdc.teardown(context.dsn)
    _sql(context, "ALTER SYSTEM SET wal_level = 'replica'")
    _sql(context, "ALTER SYSTEM RESET max_slot_wal_keep_size")
    cdc.restart(context.pgdata)


def _streamer(context):
    context.conn = context.conn or psycopg.connect(context.dsn, autocommit=True)
    return cdc.Streamer(context.conn, up=context.server.probe, deid=cdc.Deidentifier(context.tokens),
                        transport=context.server, host="box", batch=getattr(context, "batch", cdc.BATCH),
                        state=context.state, heartbeat=getattr(context, "heartbeat", False))


# -- setup -------------------------------------------------------------------------------------------------
@given("a scratch Postgres with wal_level replica")
def _given_replica(context):
    _replica(context)
    assert _sql(context, "show wal_level")[0][0] == "replica"


@when("CDC is set up with restart allowed")
def _setup_restart(context):
    context.setup = cdc.setup(context.dsn, restart_cmd=lambda: cdc.restart(context.pgdata))


@when("CDC is set up without restart")
def _setup_norestart(context):
    context.setup = cdc.setup(context.dsn)


@then("wal_level is logical and the slot WAL cap is 1GB")
def _logical(context):
    assert context.setup["ok"], context.setup
    assert _sql(context, "show wal_level")[0][0] == "logical"
    assert _sql(context, "show max_slot_wal_keep_size")[0][0] == "1GB"


@then("the publication covers schema public and the slot iq_cdc exists")
def _pub_slot(context):
    st = cdc.status(context.dsn)
    assert st["publication"]["schemas"] == ["public"], st
    assert st["slot"] and st["slot"]["wal_status"] in ("reserved", "extended"), st


@then("setup answers restart_needed")
def _restart_needed(context):
    assert context.setup == {"ok": False, "reason": "restart_needed",
                             "detail": "wal_level is replica; set to logical, takes effect on restart"}, context.setup
    cdc.restart(context.pgdata)                 # leave the scratch cluster logical for what follows


@given("CDC is set up on a fresh notes table")
def _fresh(context, before=0):
    cdc.setup(context.dsn, restart_cmd=lambda: cdc.restart(context.pgdata))
    context.state = os.path.join(context.pgdata, "cdc-state", "snapshot.json")
    cdc.teardown(context.dsn, context.state)
    _sql(context, "DROP TABLE IF EXISTS notes")
    _sql(context, "CREATE TABLE notes (id int PRIMARY KEY, body text, tags jsonb, author text, actor jsonb, "
                  "subject text)")
    for i in range(1, before + 1):
        _sql(context, "INSERT INTO notes (id, body, tags, author) VALUES (%s, 'old', '{\"n\": %s}', "
                      "'pat@example.com')" % (i, i))
    assert cdc.setup(context.dsn)["ok"]
    context.server, context.tokens, context.conn = _Server(), identity_map.IdentityMap("k"), None


# -- the server --------------------------------------------------------------------------------------------
@given("the IQ Server is up")
def _up(context):
    context.server.up = True


@given("the IQ Server is down")
def _down(context):
    context.server.up = False


@when("the IQ Server comes up")
def _comes_up(context):
    context.server.up = True


@given("the IQ Server is up but refuses the next {n:d} batches")
def _refuse_next(context, n):
    context.server.refuse_next = n


@given("the IQ Server is up but refuses the {nth:d}rd batch")
def _refuse_nth(context, nth):
    context.server.refuse_nth = nth


@given("batches of {n:d} records")
def _batches(context, n):
    context.batch = n


@given("the IQ Server is up but the IdentityMap cannot be reached")
def _no_tokens(context):
    context.tokens = _NoTokens()


# -- changes -----------------------------------------------------------------------------------------------
@when("one transaction inserts note 1, updates it and deletes note 2")
def _iud(context):
    _sql(context, "INSERT INTO notes (id, body) VALUES (2, 'gone soon')")
    _streamer(context).cycle()                  # ship the setup row so the scenario counts only its own transaction
    context.server.store.blobs.clear()
    with psycopg.connect(context.dsn) as c:
        c.execute("INSERT INTO notes (id, body, tags) VALUES (1, 'first', '{\"k\": [1, 2]}')")
        c.execute("UPDATE notes SET body = 'first, edited' WHERE id = 1")
        c.execute("DELETE FROM notes WHERE id = 2")


@when("notes {a:d} to {b:d} are copied in")
def _copy(context, a, b):
    _streamer(context).cycle()                  # ship the setup row so the scenario counts only its own rows
    context.server.store.blobs.clear()
    with psycopg.connect(context.dsn, autocommit=True) as c:
        with c.cursor().copy("COPY notes (id, body) FROM STDIN") as cp:
            for i in range(a, b + 1):
                cp.write_row((i, "copied"))


@when("note {n:d} is inserted")
def _insert(context, n):
    _sql(context, "INSERT INTO notes (id, body) VALUES (%s, 'note')", (n,))


@when("{t:d} transactions each insert 3 notes")
def _many(context, t):
    for i in range(t):
        with psycopg.connect(context.dsn) as c:
            for j in range(3):
                c.execute("INSERT INTO notes (id, body) VALUES (%s, 'n')", (100 + 3 * i + j,))
    dec, context.ends = cdc.Decoder(), []
    with psycopg.connect(context.dsn, autocommit=True) as c:
        for lsn, _x, data in cdc.peek(c):
            got = dec.feed(lsn, data)
            if isinstance(got, list):
                context.ends.append(got[0]["commit_lsn"])
    assert len(context.ends) == t


@when('a note is inserted by "{email}" with actor "{iri}" and a column named subject')
def _identity(context, email, iri):
    context.email, context.iri = email, iri
    _sql(context, "INSERT INTO notes (id, body, author, actor, subject) VALUES (5, %s, %s, %s, 'Re: hello')",
         ("from " + email + " today", email, json.dumps({"actor": iri})))


@when('a note is inserted by "{email}"')
def _by(context, email):
    _sql(context, "INSERT INTO notes (id, body, author) VALUES (9, 'note', %s)", (email,))


@when("the server receives the change but the slot is not advanced")
def _crash(context):
    real = cdc.advance
    cdc.advance = lambda c, lsn: None
    try:
        _streamer(context).cycle()
    finally:
        cdc.advance = real
    assert len(context.server.records()) == 1


@when("the streamer runs one cycle")
def _cycle(context):
    context.out = _streamer(context).cycle()


@given("the box streams as a device, with its heartbeat")
def _given_device(context):
    context.heartbeat = True


@when("the same streamer runs {n:d} cycles")
def _cycles(context, n):
    st = _streamer(context)
    for _ in range(n):
        context.out = st.cycle()


@then("the heartbeat names the box and less than 1 kB pending")
def _heartbeat(context):
    beats = context.server.records()
    beats = [r for r in beats if r["op"] == "cdc:status"]
    assert len(beats) == 1 and beats[0]["host"] == "box" and beats[0]["pending_bytes"] < 1024, beats
    assert beats[0]["table"] == "" and beats[0]["lsn"], beats


@when("CDC is torn down")
def _teardown(context):
    cdc.teardown(context.dsn, getattr(context, "state", ""))


# -- outcomes ----------------------------------------------------------------------------------------------
@then("the server holds {n:d} records of service {svc}")
def _holds(context, n, svc):
    got = context.server.records(svc)
    assert len(got) == n, (len(got), context.out)


@then("they are cdc:insert, cdc:update, cdc:delete on public.notes in order")
def _order(context):
    recs = sorted(context.server.records(), key=lambda r: cdc.lsn_int(r["lsn"]))
    assert [r["op"] for r in recs] == ["cdc:insert", "cdc:update", "cdc:delete"], recs
    assert {r["table"] for r in recs} == {"public.notes"}
    assert len({r["commit_lsn"] for r in recs}) == 1
    assert recs[2]["old"] == {"id": 2} and recs[2]["new"] is None, recs[2]
    assert recs[1]["new"]["body"] == "first, edited"


@then("the inserted record has id 1 as a number and its tags as JSON")
def _typed(context):
    ins = [r for r in context.server.records() if r["op"] == "cdc:insert"][0]
    assert ins["new"]["id"] == 1 and ins["new"]["tags"] == {"k": [1, 2]}, ins
    assert ins["operation_id"].startswith("iq:op:") and ins["decision"] == "captured"
    assert ins["at"].endswith("Z")


@then("the slot has nothing left")
def _empty(context):
    with psycopg.connect(context.dsn, autocommit=True) as c:
        left = [r for r in cdc.peek(c) if bytes(r[2])[:1] in (b"I", b"U", b"D", b"T")]
    assert left == [], left


@then("the slot still holds the change")
def _held(context):
    with psycopg.connect(context.dsn, autocommit=True) as c:
        left = [r for r in cdc.peek(c) if bytes(r[2])[:1] == b"I"]
    assert len(left) == 1, left


@then("the cycle says the server is down")
def _says_down(context):
    assert context.out["server"] == "down" and context.out["advanced"] is None, context.out


@then("the cycle names an error and advanced nothing")
def _err(context):
    assert context.out.get("error") and context.out["advanced"] is None, context.out


@then("the cycle sent {s:d} and found {a:d} already there")
def _already(context, s, a):
    assert (context.out["sent"], context.out["already"]) == (s, a), context.out


@then("the cycle sent {s:d}")
def _sent(context, s):
    assert context.out["sent"] == s and context.out.get("error"), context.out


@then("the slot advanced to the end of the first transaction")
def _advanced_first(context):
    assert context.out["advanced"] == context.ends[0], (context.out, context.ends)
    st = cdc.status(context.dsn)
    assert st["slot"]["confirmed"] == context.ends[0], st


@then('no stored record contains "{email}" or "{prefix}" or a subject key')
def _clean(context, email, prefix):
    for r in context.server.records():
        blob = json.dumps(r)
        assert email not in blob.lower() and prefix not in blob, blob
        assert "subject" not in r["new"], r["new"]


@then("the e-mail became the token of its identity")
def _token(context):
    r = context.server.records()[0]
    tok = context.tokens.token_for(cdc.subject_iri(context.email))
    assert r["new"]["author"] == tok, r["new"]
    assert r["new"]["body"] == "from %s today" % tok, r["new"]
    assert r["new"]["actor"]["actor"] == context.tokens.token_for(context.iri), r["new"]


@then("there is no slot iq_cdc and no publication iq_cdc")
def _gone(context):
    st = cdc.status(context.dsn)
    assert st["slot"] is None and st["publication"] is None, st


@then("UC_CDC has no launchd job and no IQ app or service script starts iq-cdc")
def _no_autostart(context):
    uc = context.uc
    assert not list(uc.rglob("*.plist*")), list(uc.rglob("*.plist*"))
    root = uc.parent.parent.parent
    hits = subprocess.run(["git", "-C", str(root), "grep", "-l", "-e", "iq-cdc", "-e", "UC_CDC/cdc.py", "-e", "PostgreSqlIq/cdc", "--",
                           "Development/UseCase/UC_App", "DeployIq", "scripts", "*.plist*"],
                          capture_output=True, text=True).stdout.split()
    assert hits == [], hits


# -- the initial snapshot ----------------------------------------------------------------------------------
@given("a notes table holding {n:d} notes before CDC is set up")
def _before(context, n):
    _fresh(context, before=n)


def _snapshot(context, between=None):
    context.snap = cdc.snapshot(context.dsn, _streamer(context), context.state, between=between)


@when("the snapshot runs")
def _run_snapshot(context):
    _snapshot(context)


@when("the snapshot runs with note {n:d} inserted between the drain and the snapshot")
def _run_snapshot_between(context, n):
    _snapshot(context, between=lambda: _sql(context, "INSERT INTO notes (id, body) VALUES (%s, 'between')", (n,)))


@then("it is done with {n:d} rows of {table}")
def _done(context, n, table):
    assert context.snap["ok"] and context.snap["tables"] == {table: n}, context.snap
    st = cdc.read_state(context.state)
    assert st["status"] == "done" and st["snapshot"] == context.snap["snapshot"], st


def _of_op(context, op):
    return [r for r in context.server.records() if r["op"] == op]


@then("the server holds {n:d} records of op {op}, with tags as JSON and no identity")
def _snap_typed(context, n, op):
    recs = _of_op(context, op)
    assert len(recs) == n, recs
    assert sorted(r["new"]["tags"]["n"] for r in recs) == list(range(1, n + 1)), recs
    assert all(r["new"]["author"].startswith("iq:subj:") for r in recs), recs
    assert all(r["lsn"] == context.snap["lsn"] and r["old"] is None for r in recs)


@then("the server holds {n:d} records of op {op}")
def _holds_op(context, n, op):
    recs = _of_op(context, op)
    assert len(recs) == n, [(r["op"], r["new"]) for r in context.server.records()]


@then("the cycle passed over {n:d} change already in the snapshot and sent {s:d}")
def _passed(context, n, s):
    assert context.out.get("in_snapshot") == n and context.out["sent"] == s and context.out["advanced"], context.out


@then("the snapshot says the server is down and left no state")
def _snap_down(context):
    assert context.snap == {"ok": False, "server": "down", "http": 0}, context.snap
    assert not os.path.exists(context.state)


@then("the snapshot failed at its rows and the state is running")
def _snap_failed(context):
    assert not context.snap["ok"] and context.snap["stage"] == "rows", context.snap
    assert cdc.read_state(context.state)["status"] == "running"


@then("the cycle is paused for the snapshot")
def _paused(context):
    assert context.out.get("paused") and context.out["advanced"] is None, context.out


@then("the snapshot sent {s:d} and found {a:d} already there")
def _snap_again(context, s, a):
    assert context.snap["ok"] and (context.snap["sent"], context.snap["already"]) == (s, a), context.snap


@then("there is no snapshot state")
def _no_state(context):
    assert not os.path.exists(context.state)


@then('xid {xid:d} is {seen} in snapshot "{snap}"')
def _visible(context, xid, seen, snap):
    assert cdc.visible(snap, xid) == (seen == "visible"), (xid, snap)


# -- credentials -------------------------------------------------------------------------------------------
class _Answer:
    def __init__(self, status):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@given("the environment holds an expired token and the paired file a live one")
def _tokens(context):
    import tempfile
    home = tempfile.mkdtemp(prefix="iqcdchome")
    os.makedirs(os.path.join(home, ".iq"))
    with open(os.path.join(home, ".iq", "harness-token"), "w") as f:
        f.write("live-token\n")
    context.saved_env = {k: os.environ.get(k) for k in ("HOME", "IQ_HARNESS_SESSION")}
    os.environ.update(HOME=home, IQ_HARNESS_SESSION="expired-token")


@when("the server is probed")
def _probe(context):
    import urllib.error

    def opener(req, timeout=None):
        if req.get_header("Authorization") != "Bearer live-token":
            raise urllib.error.HTTPError(req.full_url, 401, "Unauthorized", {}, None)
        return _Answer(200)
    context.clients = (relay.HttpRelayTransport("https://iq.test", bearer="expired-token"),
                       identity_map.RemoteTokenizer("https://iq.test", bearer="expired-token"))
    try:
        context.probed = cdc.PairedProbe("https://iq.test", *context.clients, opener=opener)()
    finally:
        for k, v in context.saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@then("it is up, and the transport and the tokenizer carry the file's token")
def _carry(context):
    assert context.probed == (True, 200), context.probed
    assert all(c.headers["Authorization"] == "Bearer live-token" for c in context.clients)


# -- the lag -----------------------------------------------------------------------------------------------
@when("a table outside the publication takes {n:d} rows")
def _unpublished(context, n):
    _sql(context, "CREATE SCHEMA IF NOT EXISTS private")
    _sql(context, "CREATE TABLE IF NOT EXISTS private.scratch (id serial PRIMARY KEY, pad text)")
    _sql(context, "INSERT INTO private.scratch (pad) SELECT repeat('x', 100) FROM generate_series(1, %s)" % n)


@then("the cycle advanced with nothing to send")
def _idle_advanced(context):
    assert context.out["advanced"] and context.out["transactions"] == 0 and context.out["sent"] == 0, context.out


def _lag(context):
    return cdc.lag(context.dsn, context.state)


@then("the lag is ok and behind by less than 1 kB")
def _lag_ok(context):
    lg = _lag(context)
    assert lg["health"] == "ok" and lg["slot"]["behind_bytes"] < 1024, lg


@when("no cycle has run for {m:d} minutes")
def _stale(context, m):
    from datetime import datetime, timedelta, timezone
    cdc.write_state(os.path.join(os.path.dirname(context.state), "cycle.json"),
                    {"server": "up", "sent": 0, "at": (datetime.now(timezone.utc) - timedelta(minutes=m)).isoformat()})


@then("the lag says {health}")
def _lag_is(context, health):
    lg = _lag(context)
    assert lg["health"] == health, lg


@then("the lag's last cycle saw the server up and sent {n:d}")
def _lag_last(context, n):
    lg = _lag(context)
    assert lg["last_cycle"]["server"] == "up" and lg["last_cycle"]["sent"] == n and lg["last_cycle_age_s"] < 60, lg


@when("the snapshot is left running")
def _left_running(context):
    cdc.write_state(context.state, {"status": "running", "started": "now"})


@then("the lag is at risk once the slot may hold only 1MB and a table outside the publication takes {n:d} rows")
def _small_cap(context, n):
    _sql(context, "ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'")
    _sql(context, "select pg_reload_conf()")
    try:
        _unpublished(context, n)
        context.at_risk = _lag(context)
    finally:
        _sql(context, "ALTER SYSTEM SET max_slot_wal_keep_size = '1GB'")
        _sql(context, "select pg_reload_conf()")
    assert context.at_risk["health"] == "at risk", context.at_risk
    context.server.up = True


@then("the Agents tab's cdc value has the slot, the health and the last cycle")
def _agents_tab(context):
    import importlib.util
    path = context.uc.parents[2] / "Development" / "UseCase" / "UC_Dashboard" / "tabs" / "agents" / "agents.py"
    spec = importlib.util.spec_from_file_location("iq_agents_tab", path)
    tab = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tab)
    got = tab.cdc_lag(dsn=context.dsn, state=context.state)
    want = _lag(context)
    assert got["health"] == want["health"] == "ok", got
    assert got["slot"]["confirmed"] == want["slot"]["confirmed"] and got["last_cycle"]["sent"] == 1, got


# -- row filters -------------------------------------------------------------------------------------------
@when('the filter on {table} is "{where}" with a full replica identity')
def _filter_full(context, table, where):
    context.filtered = cdc.set_filter(context.dsn, table, where, full=True)
    assert context.filtered["ok"], context.filtered


@when('the filter on {table} is "{where}"')
def _filter(context, table, where):
    context.filtered = cdc.set_filter(context.dsn, table, where)


@when("the filter on {table} is cleared")
def _filter_clear(context, table):
    context.filtered = cdc.set_filter(context.dsn, table, None)
    assert context.filtered["ok"] and not context.filtered["filters"], context.filtered


@when("the filters are reset")
def _filter_reset(context):
    assert cdc.reset_filters(context.dsn)["ok"]


@then("the filter is set, and the publication names its tables instead of schema public")
def _filter_set(context):
    f = context.filtered
    assert f["ok"] and f["filters"] == {"public.notes": "(id < 100)"} and f["schemas"] == [], f
    assert cdc.lag(context.dsn, context.state)["filters"] == {"public.notes": "(id < 100)"}


@then("the shipped insert is note {n:d}")
def _shipped_note(context, n):
    assert [r["new"]["id"] for r in _of_op(context, "cdc:insert")] == [n]


@when('note {n:d}\'s body becomes "{body}"')
def _body(context, n, body):
    _sql(context, "UPDATE notes SET body = %s WHERE id = %s", (body, n))


@then("the shipped delete carries the whole old row of note {n:d}")
def _whole_old(context, n):
    d = _of_op(context, "cdc:delete")[0]
    assert d["old"]["id"] == n and d["old"]["body"] == "note", d


@then("the filter is refused naming the replica identity")
def _refused_ri(context):
    f = context.filtered
    assert not f["ok"] and f["reason"] == "refused" and "replica identity" in (f["detail"] + f["hint"]).lower(), f


@then("the filter is refused")
def _refused(context):
    assert not context.filtered["ok"] and context.filtered["reason"] == "refused", context.filtered


@then("the publication still covers schema public with no filters")
def _still_schema(context):
    pub = cdc.status(context.dsn)["publication"]
    assert pub["schemas"] == ["public"] and pub["filters"] == {}, pub


@then("note {n:d} can still be inserted, updated and deleted")
def _writers_ok(context, n):
    _sql(context, "INSERT INTO notes (id, body) VALUES (%s, 'w')", (n,))
    _sql(context, "UPDATE notes SET body = 'w2' WHERE id = %s", (n,))
    _sql(context, "DELETE FROM notes WHERE id = %s", (n,))


# -- column lists ------------------------------------------------------------------------------------------
class _CountingTokens:
    def __init__(self, inner):
        self.inner, self.asked = inner, 0

    def tokenize(self, subject_iri):
        self.asked += 1
        return self.inner.tokenize(subject_iri)

    def token_for(self, subject_iri):
        return self.inner.token_for(subject_iri)


@when('the columns of {table} are "{names}"')
def _columns(context, table, names):
    context.tokens = _CountingTokens(context.tokens) if hasattr(context, "tokens") and context.tokens else None
    context.narrowed = cdc.set_columns(context.dsn, table, names.split(","))


@when("the columns of {table} are cleared")
def _columns_clear(context, table):
    context.narrowed = cdc.set_columns(context.dsn, table, None)
    assert context.narrowed["ok"], context.narrowed


@then("every shipped record of {table} carries only {a} and {b}")
def _only(context, table, a, b):
    recs = [r for r in context.server.records() if r["table"] == table]
    assert recs, context.server.records()
    for r in recs:
        for side in ("new", "old"):
            if r[side] is not None:
                assert set(r[side]) <= {a, b}, (side, r)
        assert set(r["new"] or r["old"]) == {a, b} or r["op"] == "cdc:delete", r


@then("the IdentityMap was asked once, for the e-mail in the body, and never for the unpublished author or actor")
def _asked_once(context):
    assert context.tokens.asked == 1, context.tokens.asked      # author (same e-mail) and actor (an IRI) never decoded
    ins = [r for r in context.server.records() if r["op"] == "cdc:insert"][0]
    assert ins["new"]["body"].startswith("from iq:subj:"), ins


@then("the column list is refused naming the replica identity")
def _col_refused(context):
    f = context.narrowed
    assert not f["ok"] and f["reason"] == "refused" and "replica identity" in (f["detail"] + f["hint"]).lower(), f


@then('the publication has filter "{where}" and columns {a}, {b} on {table}')
def _both(context, where, a, b, table):
    pub = cdc.status(context.dsn)["publication"]
    assert pub["filters"] == {table: where} and pub["columns"] == {table: [a, b]}, pub


@then('the publication has filter "{where}" and no column list on {table}')
def _filter_only(context, where, table):
    pub = cdc.status(context.dsn)["publication"]
    assert pub["filters"] == {table: where} and pub["columns"] == {}, pub
