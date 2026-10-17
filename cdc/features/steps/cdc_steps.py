"""Steps for cdc.feature: the scratch Postgres is real (pgoutput, a real slot); the IQ Server is the bridge's
EventRelay in-process, with a probe and refusals the scenario sets.

@use_case Development/UseCase/UC_CDC
@facet Gherkin
"""
import json
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
                        transport=context.server, host="box", batch=getattr(context, "batch", cdc.BATCH))


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
def _fresh(context):
    cdc.setup(context.dsn, restart_cmd=lambda: cdc.restart(context.pgdata))
    cdc.teardown(context.dsn)
    _sql(context, "DROP TABLE IF EXISTS notes")
    _sql(context, "CREATE TABLE notes (id int PRIMARY KEY, body text, tags jsonb, author text, actor jsonb, "
                  "subject text)")
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


@when("CDC is torn down")
def _teardown(context):
    cdc.teardown(context.dsn)


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
    hits = subprocess.run(["git", "-C", str(root), "grep", "-l", "-e", "iq-cdc", "-e", "UC_CDC/cdc.py", "--",
                           "Development/UseCase/UC_App", "DeployIq", "scripts", "*.plist*"],
                          capture_output=True, text=True).stdout.split()
    assert hits == [], hits
