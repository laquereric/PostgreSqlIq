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

NODE = {5433: "w1", 5434: "w2", 5435: "w3"}
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
    with context.cluster.connect(autocommit=True) as c:
        n = c.execute("SELECT count(*) FROM citus_get_active_worker_nodes()").fetchone()[0]
    assert n >= 2, "%d workers" % n


@given("the platform is put as datums, its vocabulary on every node")
def step_platform(context):
    context.platform = cp.load_platform(context.cluster)


# --- placement ----------------------------------------------------------------------------------------------------

def worker_ports(context):
    with context.cluster.connect(autocommit=True) as c:
        return sorted(r[0] for r in c.execute(
            "SELECT DISTINCT nodeport FROM citus_shards WHERE table_name = 'iq.datum'::regclass").fetchall())


@then("the datums' shards are on {n:d} workers")
def step_on_workers(context, n):
    ports = worker_ports(context)
    assert len(ports) == n, ports
    with context.cluster.connect(autocommit=True) as c:
        spread = c.execute("SELECT nodeport, count(*) FROM citus_shards WHERE table_name = 'iq.datum'::regclass "
                           "GROUP BY 1 ORDER BY 1").fetchall()
    context.measures["datum shards per worker (%d workers)" % n] = ", ".join("%s %d" % (NODE[p], k) for p, k in spread)


@then("every datum's triples are on the worker that holds its document")
def step_colocated(context):
    with context.cluster.connect(autocommit=True) as c:
        bad = c.execute("""
            SELECT d.shardid, q.shardid FROM citus_shards d JOIN citus_shards q
              ON q.table_name = 'iq.quad'::regclass AND d.table_name = 'iq.datum'::regclass
             AND (SELECT shardminvalue FROM pg_dist_shard WHERE shardid = q.shardid)
               = (SELECT shardminvalue FROM pg_dist_shard WHERE shardid = d.shardid)
             WHERE q.nodeport <> d.nodeport""").fetchall()
        groups = c.execute("SELECT count(DISTINCT colocationid) FROM pg_dist_partition "
                           "WHERE logicalrelid IN ('iq.datum'::regclass, 'iq.quad'::regclass)").fetchone()[0]
    assert groups == 1 and not bad, (groups, bad)
    for name in context.platform.datums:
        assert context.cluster.node_of(name) in NODE


@then("the vocabulary is on every node")
def step_vocab_everywhere(context):
    n = len(context.platform.vocab)
    for port in (5432, *worker_ports(context)):
        got = context.cluster.admin("SELECT count(*) FROM iq.vocab", port) if port == 5432 else \
            context.cluster.admin("SELECT count(*) FROM iq.vocab_%s" % shard_of_vocab(context), port)
        assert int(got) == n, (port, got, n)


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
    for port in (5432, *worker_ports(context)):
        table = "iq.vocab" if port == 5432 else "iq.vocab_%s" % shard
        got = context.cluster.admin("SELECT count(*) FROM %s WHERE s = '%s'" % (table, iri(cls)), port)
        assert got == "1", (port, got)


# --- two-phase commit ---------------------------------------------------------------------------------------------

def on_distinct_workers(context, k=2, prefix="test:pair"):
    seen = {}
    for n in range(200):
        name = "%s-%d" % (prefix, n)
        port = context.cluster.node_of(iri(name))
        seen.setdefault(port, name)
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
    port = context.cluster.node_of(iri("test:durable"))
    node = NODE[port]
    t0 = time.perf_counter()
    context.cluster.host_sh(
        "sudo -u postgres /usr/pgsql-17/bin/pg_ctl -D /var/lib/pgsql/17/%s stop -m immediate >/dev/null; "
        "systemctl reset-failed iq-citus@%s 2>/dev/null; systemctl start iq-citus@%s; "
        "for _ in $(seq 60); do sudo -u postgres /usr/pgsql-17/bin/pg_isready -q -p %d && exit 0; sleep 1; done; exit 1"
        % (node, node, node, port))
    context.measures["worker crash (immediate stop) to accepting again"] = "%.1f s" % (time.perf_counter() - t0)
    log = context.cluster.host_sh("tail -n 40 $(ls -t /var/lib/pgsql/17/%s/log/*.log | head -1) | "
                                  "grep -E 'not properly shut down|redo (starts|done)' | sed 's/^[^]]*] //'" % node)
    assert "not properly shut down" in log, "the worker did not start by crash recovery: %r" % log
    context.measures["the worker's restart after the crash"] = "; ".join(l.strip() for l in log.splitlines())[:300]


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

W3 = 5435


def in_cluster(context, port):
    return context.cluster.admin("SELECT count(*) FROM pg_dist_node WHERE nodeport = %d" % port) == "1"


@given("the third worker is not in the cluster")
def step_no_w3(context):
    cp.two_workers(context.cluster)
    assert not in_cluster(context, W3)


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
        context.cluster.admin("SELECT citus_add_node('localhost', %d); "
                              "SELECT citus_rebalance_start(shard_transfer_mode => 'force_logical'); "
                              "SELECT citus_rebalance_wait();" % W3)
    while_writing(context, "add a worker and rebalance", add)


@when("the third worker is drained and removed while datums are being put")
def step_remove_w3(context):
    def drain():
        cp.remove_worker(context.cluster, W3)
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
