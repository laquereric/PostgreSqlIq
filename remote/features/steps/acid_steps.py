"""Steps for acid.feature.

@use_case Development/UseCase/UC_Citus
@facet Store
@status Proposed
"""
import threading
import time

import psycopg
from behave import given, then, when

import citus_plane as cp

STORE = cp.NS + "Store"
A = cp.RDF + "type"


def iri(name):
    return cp.NS + name


def conn(context, autocommit=False):
    c = context.cluster.connect(autocommit=autocommit)
    context.open_conns = getattr(context, "open_conns", []) + [c]
    return c


def typed(name, cls):
    return [(iri(name), A, iri(cls), False)]


def stores(c):
    return cp.ask(c, (None, A, STORE))


# --- background ---------------------------------------------------------------------------------------------------

@given("the Citus cluster on Vultr")
def step_cluster(context):
    """The store as deploy-citus.sh put it up: a cluster with two workers, or -- the server shape -- one server with
    no Citus at all (the Citus-only scenarios are skipped there, environment.py)."""
    with context.cluster.connect(autocommit=True) as c:
        if context.cluster.mode == "server":
            n = c.execute("SELECT count(*) FROM pg_extension WHERE extname = 'citus'").fetchone()[0]
            assert n == 0, "a server shape with Citus loaded"
            return
        n = c.execute("SELECT count(*) FROM citus_get_active_worker_nodes()").fetchone()[0]
    assert n >= 2, "%d workers" % n


@given("the platform is put as datums, its vocabulary on every node")
def step_platform(context):
    context.platform = cp.load_platform(context.cluster)


# --- placement ----------------------------------------------------------------------------------------------------

def workers(context):
    """The workers holding datum shards, as `nodename:port`."""
    with context.cluster.connect(autocommit=True) as c:
        return sorted("%s:%d" % r for r in c.execute(
            "SELECT DISTINCT nodename, nodeport FROM citus_shards WHERE table_name = 'iq.datum'::regclass").fetchall())


@then("the datums' shards are on {n:d} workers")
def step_on_workers(context, n):
    ws = workers(context)
    assert len(ws) == n, ws
    with context.cluster.connect(autocommit=True) as c:
        spread = c.execute("SELECT nodename || ':' || nodeport, count(*) FROM citus_shards "
                           "WHERE table_name = 'iq.datum'::regclass GROUP BY 1 ORDER BY 1").fetchall()
    context.measures["datum shards per worker (%d workers)" % n] = \
        ", ".join("%s %d" % (context.cluster.name(k), m) for k, m in sorted(spread, key=lambda r: context.cluster.name(r[0])))


@then("every datum's triples are on the worker that holds its document")
def step_colocated(context):
    with context.cluster.connect(autocommit=True) as c:
        bad = c.execute("""
            SELECT d.shardid, q.shardid FROM citus_shards d JOIN citus_shards q
              ON q.table_name = 'iq.quad'::regclass AND d.table_name = 'iq.datum'::regclass
             AND (SELECT shardminvalue FROM pg_dist_shard WHERE shardid = q.shardid)
               = (SELECT shardminvalue FROM pg_dist_shard WHERE shardid = d.shardid)
             WHERE (q.nodename, q.nodeport) <> (d.nodename, d.nodeport)""").fetchall()
        groups = c.execute("SELECT count(DISTINCT colocationid) FROM pg_dist_partition "
                           "WHERE logicalrelid IN ('iq.datum'::regclass, 'iq.quad'::regclass)").fetchone()[0]
    assert groups == 1 and not bad, (groups, bad)
    for name in context.platform.datums:
        assert context.cluster.node_of(name) in context.cluster.topo["workers"]


@then("the vocabulary is on every node")
def step_vocab_everywhere(context):
    n = len(context.platform.vocab)
    for node in (cp.COORD, *workers(context)):
        table = "iq.vocab" if node == cp.COORD else "iq.vocab_%s" % shard_of_vocab(context)
        got = context.cluster.admin("SELECT count(*) FROM %s" % table, node)
        assert int(got) == n, (context.cluster.name(node), got, n)


def shard_of_vocab(context):
    return context.cluster.admin("SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'iq.vocab'::regclass")


# --- a datum is one transaction ------------------------------------------------------------------------------------

def present(c, name):
    """What of a datum answers: document, text, vector (nearest to its own), triples."""
    i = iri(name)
    row = c.execute("SELECT tier, embedding::text FROM iq.datum WHERE iri = %s", (i,)).fetchone()
    quads = c.execute("SELECT count(*) FROM iq.quad WHERE g = %s", (i,)).fetchone()[0]
    out = {"document": row is not None, "triples": quads}
    if row:
        out["nearest"] = c.execute("SELECT iri FROM iq.datum ORDER BY embedding <-> %s::vector LIMIT 1",
                                   (row[1],)).fetchone()[0]
    return out


@when('the datum "{name}" is put as a RecordStore with the text "{text}"')
def step_put_text(context, name, text):
    t0 = time.perf_counter()
    with context.cluster.connect() as c:
        cp.put(c, iri(name), typed(name, "RecordStore"), body={"iri": iri(name), "text": text})
        c.commit()
    context.measures["one datum put (document, text, vector, triples), one transaction"] = \
        "%.0f ms" % ((time.perf_counter() - t0) * 1000)


@then('its document, a text search for "{word}", the nearest vector and its triples all answer "{name}"')
def step_all_answer(context, word, name):
    with context.cluster.connect(autocommit=True) as c:
        p = present(c, name)
        hits = [r[0] for r in c.execute("SELECT iri FROM iq.datum WHERE words @@ plainto_tsquery('english', %s)",
                                        (word,)).fetchall()]
    assert p["document"] and p["triples"] == 1 and p["nearest"] == iri(name), p
    assert hits == [iri(name)], hits


@when('the datum "{name}" is put and its last triple is refused')
def step_put_broken(context, name):
    c = conn(context)
    try:
        cp.put(c, iri(name), typed(name, "RecordStore") + [(iri(name), iri("license"), None, True)],
               body={"iri": iri(name), "text": "broken datum"})
        c.commit()
        context.refused = None
    except psycopg.Error as e:
        context.refused = e
        c.rollback()
    assert isinstance(context.refused, psycopg.errors.NotNullViolation), context.refused


@then('nothing of "{name}" answers: no document, no text, no vector, no triples')
def step_nothing(context, name):
    with context.cluster.connect(autocommit=True) as c:
        p = present(c, name)
        hits = c.execute("SELECT count(*) FROM iq.datum WHERE words @@ plainto_tsquery('english', 'broken')").fetchone()[0]
        near = c.execute("SELECT iri FROM iq.datum ORDER BY embedding <-> %s::vector LIMIT 1",
                         (str(cp.embedding(iri(name))),)).fetchone()[0]
    assert not p["document"] and p["triples"] == 0 and hits == 0 and near != iri(name), (p, hits, near)


# --- inference ----------------------------------------------------------------------------------------------------

@then("{mode} inference every competency question answers {how}")
def step_questions(context, mode, how):
    inference = mode == "with"
    with context.cluster.connect() as c:
        for qid, text, _, pattern, expected in cp.questions():
            got, ms = cp.timed(cp.ask, c, pattern, inference)
            want = expected if inference else set()
            assert got == want, (qid, text, sorted(got), sorted(want))
            if inference:
                context.measures.setdefault("%s with inference (query time, in the transaction)" % qid, "%.0f ms" % ms)
        c.rollback()


@given('a transaction that has put "{name}" as a RecordStore and not committed')
def step_open_tx(context, name):
    context.tx = conn(context)
    cp.put(context.tx, iri(name), typed(name, "RecordStore"))


@then('inside it "{name}" is a Store')
def step_inside(context, name):
    assert name in stores(context.tx), stores(context.tx)


@then('another session does not see "{name}" as a Store')
def step_other(context, name):
    with context.cluster.connect() as c:
        assert name not in stores(c)
        c.rollback()


@when("the transaction rolls back")
def step_rollback(context):
    context.tx.rollback()


@then('no session sees "{name}" as a Store')
def step_no_session(context, name):
    assert name not in stores(context.tx)
    context.tx.rollback()
    with context.cluster.connect() as c:
        assert name not in stores(c)
        c.rollback()


@when('"{name}" is committed as a RecordStore')
def step_commit_record(context, name):
    with context.cluster.connect() as c:
        cp.put(c, iri(name), typed(name, "RecordStore"))
        c.commit()


@then('"{name}" is a Store at once, with nothing recomputed')
def step_at_once(context, name):
    with context.cluster.connect() as c:
        s, ms = cp.timed(stores, c)
        c.rollback()
    assert name in s, s
    context.measures.setdefault("a change answered by inference: first query after the commit", "%.0f ms (no recompute)" % ms)


@when('one transaction makes "{cls}" a subclass of Store and commits "{name}" as a ColumnStore')
def step_vocab_change(context, cls, name):
    with context.cluster.connect() as c:
        c.execute("INSERT INTO iq.vocab (s, p, o) VALUES (%s, %s, %s)", (iri(cls), cp.RDFS + "subClassOf", STORE))
        cp.put(c, iri(name), [(iri(name), A, iri(cls), False)])
        c.commit()


@then('every node\'s copy of the vocabulary holds "{cls}"')
def step_vocab_copy(context, cls):
    shard = shard_of_vocab(context)
    for node in (cp.COORD, *workers(context)):
        table = "iq.vocab" if node == cp.COORD else "iq.vocab_%s" % shard
        got = context.cluster.admin("SELECT count(*) FROM %s WHERE s = '%s'" % (table, iri(cls)), node)
        assert got == "1", (context.cluster.name(node), got)


# --- two-phase commit ---------------------------------------------------------------------------------------------

def on_distinct_workers(context, k=2, prefix="test:pair"):
    seen = {}
    for n in range(200):
        name = "%s-%d" % (prefix, n)
        seen.setdefault(context.cluster.node_of(iri(name)), name)
        if len(seen) >= k:
            return list(seen.items())
    raise AssertionError("no %d workers among the shards" % k)


@given("two test datums whose shards are on different workers")
def step_pair(context):
    context.pair = on_distinct_workers(context)


@when("one transaction puts both")
def step_put_both(context):
    with context.cluster.connect() as c:
        for _, name in context.pair:
            cp.put(c, iri(name), typed(name, "RecordStore"))
        c.commit()


@then("both answer")
def step_both(context):
    with context.cluster.connect(autocommit=True) as c:
        for _, name in context.pair:
            assert present(c, name)["document"], name


@when("one transaction puts a datum on one worker and, on the other, a triple whose datum does not exist")
def step_orphan(context):
    (pa, _), (pb, _) = context.pair
    a = next(n for p, n in [(context.cluster.node_of(iri("test:x-%d" % i)), "test:x-%d" % i) for i in range(200)] if p == pa)
    b = next(n for p, n in [(context.cluster.node_of(iri("test:y-%d" % i)), "test:y-%d" % i) for i in range(200)] if p == pb)
    context.second = (a, b)
    context.refused_at = "write"
    c = conn(context)
    try:
        cp.put(c, iri(a), typed(a, "RecordStore"))
        c.execute("INSERT INTO iq.quad (g, s, p, o) VALUES (%s, %s, %s, %s)", (iri(b), iri(b), A, iri("RecordStore")))
        context.refused_at = "commit"                    # the orphan is checked when the workers PREPARE
        c.commit()
        context.refused = None
    except psycopg.Error as e:
        context.refused = e
        try:
            c.rollback()
        except psycopg.Error:
            pass


@then("the commit is refused")
def step_commit_refused(context):
    assert isinstance(context.refused, psycopg.errors.ForeignKeyViolation), context.refused
    assert context.refused_at == "commit", "refused at the %s, not by the commit" % context.refused_at


@then("neither of the second transaction's writes answers")
def step_neither(context):
    a, b = context.second
    with context.cluster.connect(autocommit=True) as c:
        assert not present(c, a)["document"] and present(c, a)["triples"] == 0
        assert c.execute("SELECT count(*) FROM iq.quad WHERE g = %s", (iri(b),)).fetchone()[0] == 0
    pending = context.cluster.admin("SELECT count(*) FROM pg_prepared_xacts", context.cluster.node_of(iri(a)))
    assert pending == "0", "prepared transactions left on the worker: %s" % pending


# --- a two-phase commit cut between PREPARE and COMMIT --------------------------------------------------------------

HOLD = """
SET citus.enable_unsafe_triggers = on;
CREATE OR REPLACE FUNCTION iq.test_hold_prepare() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.body ? 'iq_test_hold_prepare' THEN PERFORM pg_sleep((NEW.body->>'iq_test_hold_prepare')::float); END IF;
    RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS test_hold_prepare ON iq.datum;
CREATE CONSTRAINT TRIGGER test_hold_prepare AFTER INSERT ON iq.datum DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION iq.test_hold_prepare();
ALTER SYSTEM SET citus.recover_2pc_interval = -1;
SELECT pg_reload_conf();
SELECT pg_sleep(2);     -- the reload reaches the maintenance daemon (which recovers two-phase commits) at its next turn
"""
UNHOLD = """
SET citus.enable_unsafe_triggers = on;
DROP TRIGGER IF EXISTS test_hold_prepare ON iq.datum;
DROP FUNCTION IF EXISTS iq.test_hold_prepare();
ALTER SYSTEM RESET citus.recover_2pc_interval;
SELECT pg_reload_conf();
"""


@given("the first worker holds its PREPARE for {secs:d} seconds")
def step_hold(context, secs):
    """A deferred trigger runs when its transaction prepares: on the first datum's worker it sleeps. The coordinator's
    own recovery of two-phase commits is paused (recover_2pc_interval -1), so the scenario sees what it leaves."""
    context.cluster.admin(HOLD)
    context.hold = secs


def put_both_while(context, crash: str):
    (_, a), (b_node, b) = context.pair
    watcher = context.cluster.crash_once_prepared(crash, b_node)
    context.refused, t0 = None, time.perf_counter()
    c = conn(context)
    try:
        cp.put(c, iri(a), typed(a, "RecordStore"), body={"iri": iri(a), "iq_test_hold_prepare": context.hold})
        cp.put(c, iri(b), typed(b, "RecordStore"))
        c.commit()
    except psycopg.Error as e:
        context.refused = e
    context.commit_ms = (time.perf_counter() - t0) * 1000
    out, err = watcher.communicate(timeout=120)
    assert watcher.returncode == 0, "the watcher did not crash %s: %s" % (context.cluster.name(crash), err[-300:])
    context.crashed = crash


@when("one transaction puts both, and the second worker crashes the moment it has prepared")
def step_crash_worker_2pc(context):
    put_both_while(context, context.pair[1][0])


@when("one transaction puts both, and the coordinator crashes the moment the second worker has prepared")
def step_crash_coord_2pc(context):
    put_both_while(context, cp.COORD)


@then("the commit succeeds")
def step_commit_ok(context):
    assert context.refused is None, context.refused
    context.measures["2PC, worker crashed after PREPARE: commit answered after"] = "%.0f ms" % context.commit_ms


@then("the second worker came back holding the transaction prepared, neither committed nor rolled back")
def step_in_doubt(context):
    b_node, b = context.pair[1]
    assert "not properly shut down" in recovery_log(context, b_node)
    assert context.cluster.prepared(b_node) == 1, context.cluster.prepared(b_node)
    with context.cluster.connect(autocommit=True) as c:
        assert not present(c, b)["document"], "the in-doubt datum is visible before its COMMIT PREPARED"


@then("the commit is lost with the connection")
def step_commit_lost(context):
    assert isinstance(context.refused, psycopg.OperationalError), context.refused
    for _ in range(30):
        try:
            with context.cluster.connect(autocommit=True):
                return
        except psycopg.OperationalError:
            time.sleep(1)
    raise AssertionError("the coordinator did not come back")


@when("the coordinator recovers its two-phase commits")
def step_recover(context):
    time.sleep(max(0.0, context.hold + 1 - context.commit_ms / 1000))   # the held PREPARE has finished
    t0 = time.perf_counter()
    n = 0
    for _ in range(30):
        n += int(context.cluster.admin("SELECT recover_prepared_transactions()"))
        if all(context.cluster.prepared(w) == 0 for w in workers(context)):
            break
        time.sleep(1)
    context.measures["2PC recovery after a %s crash: prepared transactions resolved / took" % context.cluster.name(
        context.crashed).split(":")[0]] = "%d / %.1f s" % (n, time.perf_counter() - t0)


@then("neither answers")
def step_neither_pair(context):
    with context.cluster.connect(autocommit=True) as c:
        for _, name in context.pair:
            p = present(c, name)
            assert not p["document"] and p["triples"] == 0, (name, p)


@then("no prepared transaction is left on any worker")
def step_no_prepared(context):
    left = {context.cluster.name(w): context.cluster.prepared(w) for w in workers(context)}
    assert not any(left.values()), left


# --- isolation ----------------------------------------------------------------------------------------------------

@given('"{name}" is committed at tier "{tier}"')
def step_tiered(context, name, tier):
    with context.cluster.connect() as c:
        cp.put(c, iri(name), typed(name, "RecordStore"), tier=tier)
        c.commit()


@when('a repeatable-read transaction reads it, another moves it to "{t2}" and commits, and the first then moves it to "{t3}"')
def step_race(context, t2, t3):
    i = iri("test:tiered")
    first = conn(context)
    first.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    first.execute("SELECT tier FROM iq.datum WHERE iri = %s", (i,)).fetchone()
    with context.cluster.connect() as other:
        other.execute("UPDATE iq.datum SET tier = %s WHERE iri = %s", (t2, i))
        other.commit()
    try:
        first.execute("UPDATE iq.datum SET tier = %s WHERE iri = %s", (t3, i))
        first.commit()
        context.refused = None
    except psycopg.Error as e:
        context.refused = e
        first.rollback()


@then("the first is refused with a serialization failure")
def step_serialization(context):
    assert isinstance(context.refused, psycopg.errors.SerializationFailure), context.refused


@then('"{name}" is at tier "{tier}"')
def step_at_tier(context, name, tier):
    with context.cluster.connect(autocommit=True) as c:
        assert c.execute("SELECT tier FROM iq.datum WHERE iri = %s", (iri(name),)).fetchone()[0] == tier


# --- durability ---------------------------------------------------------------------------------------------------

@when("the worker holding it is stopped as a crash stops it, and started again")
def step_crash(context):
    node = context.cluster.node_of(iri("test:durable"))
    t0 = time.perf_counter()
    context.cluster.crash(node)
    context.measures["worker crash (immediate stop) to accepting again"] = "%.1f s" % (time.perf_counter() - t0)
    log = recovery_log(context, node)
    assert "not properly shut down" in log, "the worker did not start by crash recovery: %r" % log
    context.measures["the worker's restart after the crash"] = "; ".join(l.strip() for l in log.splitlines())[:300]


def recovery_log(context, node):
    unit = context.cluster.node(node)["unit"]
    return context.cluster.host_sh("tail -n 40 $(ls -t /var/lib/pgsql/17/%s/log/*.log | head -1) | "
                                   "grep -E 'not properly shut down|redo (starts|done)' | sed 's/^[^]]*] //'" % unit, node)


@then('"{name}" answers with its document and its triples')
def step_durable(context, name):
    for _ in range(10):
        try:
            with context.cluster.connect(autocommit=True) as c:
                p = present(c, name)
            break
        except psycopg.OperationalError:
            time.sleep(1)
    assert p["document"] and p["triples"] == 1, p


# --- scale out and in ---------------------------------------------------------------------------------------------



@given("the third worker is not in the cluster")
def step_no_w3(context):
    cp.two_workers(context.cluster)
    assert not cp.in_cluster(context.cluster, context.cluster.spare)


def while_writing(context, label, action):
    """Run `action` while a writer puts one datum per transaction; every write must commit."""
    stop, written, errors = threading.Event(), [], []

    def writer():
        with context.cluster.connect() as c:
            n = 0
            while not stop.is_set():
                name = "test:during-%s-%d" % (label, n)
                try:
                    cp.put(c, iri(name), typed(name, "Probe"))       # not a Store: the questions stay as they are
                    c.commit()
                    written.append(name)
                except psycopg.Error as e:
                    c.rollback()
                    errors.append(repr(e)[:200])
                n += 1
    t = threading.Thread(target=writer)
    t.start()
    t0 = time.perf_counter()
    try:
        action()
    finally:
        took = time.perf_counter() - t0
        stop.set()
        t.join()
    context.during = written
    context.measures["%s: time / datums put meanwhile / failed writes" % label] = \
        "%.1f s / %d / %d" % (took, len(written), len(errors))
    assert not errors, errors[:3]
    assert written, "no write landed during the move"


@when("the third worker is added and the shards rebalanced while datums are being put")
def step_add_w3(context):
    def add():
        context.cluster.admin(cp.add_worker_sql(context.cluster.spare) +
                              "SELECT citus_rebalance_start(shard_transfer_mode => 'force_logical'); "
                              "SELECT citus_rebalance_wait();")
    while_writing(context, "add a worker and rebalance", add)


@when("the third worker is drained and removed while datums are being put")
def step_remove_w3(context):
    def drain():
        cp.remove_worker(context.cluster, context.cluster.spare)
    while_writing(context, "drain and remove a worker", drain)


@then("no orphaned shard is left behind")
def step_no_orphans(context):
    assert context.cluster.admin("SELECT count(*) FROM pg_dist_cleanup") == "0"


@then("every datum put during the move answers")
def step_during(context):
    with context.cluster.connect(autocommit=True) as c:
        got = {r[0] for r in c.execute("SELECT iri FROM iq.datum WHERE iri = ANY(%s)",
                                       ([iri(n) for n in context.during],)).fetchall()}
        quads = c.execute("SELECT count(*) FROM iq.quad WHERE g = ANY(%s)", ([iri(n) for n in context.during],)).fetchone()[0]
    assert len(got) == len(context.during) and quads == len(context.during), (len(got), quads, len(context.during))
