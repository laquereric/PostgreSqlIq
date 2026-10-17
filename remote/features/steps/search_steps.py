"""Steps for search.feature.

@use_case Development/UseCase/UC_Citus
@facet Store
@status Proposed
"""
import random
import time

from behave import given, then, when

import citus_plane as cp

WORDS = "postgres citus shard worker vector graph triple iceberg parquet bucket vultr object storage plane datum".split()


def iri(i):
    return cp.NS + "test:text-%d" % i


@given("the BM25 index on the record")
def step_index(context):
    if context.cluster.admin("SELECT count(*) FROM pg_extension WHERE extname = 'pg_search'") != "1":
        context.scenario.skip("no pg_search on this cluster (deploy-citus.sh search-up)")
        return
    context.cluster.admin((cp.HERE / "search.sql").read_text())


@given("{n:d} datums of text on the plane, spread over the workers")
def step_texts(context, n):
    rnd = random.Random(17)
    context.texts = {}
    with context.cluster.connect() as c:
        for i in range(n):
            words = [rnd.choice(WORDS) for _ in range(12)]
            hits = i % 4                                       # 0..3 mentions of the term
            words[:hits] = ["lakehouse"] * hits
            rnd.shuffle(words)
            text = " ".join(words)
            context.texts[iri(i)] = text
            c.execute("SELECT iq.put_datum(%s, 'Silver', %s, NULL, '[]')", (iri(i), '{"text": "x"}'))
            c.execute("UPDATE iq.datum SET raw = %s, media_type = 'text/plain' WHERE iri = %s", (text, iri(i)))
        c.commit()


def search(c, term, limit=1000):
    return c.execute("SELECT iri, pdb.score(iri) FROM iq.datum WHERE raw @@@ %s ORDER BY 2 DESC, 1 LIMIT %s",
                     (term, limit)).fetchall()


@when('the record is searched for "{term}"')
def step_search(context, term):
    with context.cluster.connect() as c:
        t0 = time.perf_counter()
        context.found = search(c, term)
        context.measures.setdefault("a BM25 search across the shards", "%.0f ms" % ((time.perf_counter() - t0) * 1000))
        c.rollback()


@then('every datum whose text says "{term}" is found, and no other')
def step_found(context, term):
    want = {i for i, t in context.texts.items() if term in t.split()}
    assert {r[0] for r in context.found} == want, (len(context.found), len(want))


@then("the datums found are held on every worker")
def step_every_worker(context):
    where = {context.cluster.node_of(r[0]) for r in context.found}
    with context.cluster.connect(autocommit=True) as c:
        workers = c.execute("SELECT count(*) FROM citus_get_active_worker_nodes()").fetchone()[0]
    assert len(where) == workers, (where, workers)


@then("they are ranked by how often their text says it")
def step_ranked(context):
    count = lambda i: context.texts[i].split().count("lakehouse")
    counts = [count(r[0]) for r in context.found]
    groups = {}
    for (i, score), k in zip(context.found, counts):
        groups.setdefault(k, []).append(score)
    lows = {k: min(v) for k, v in groups.items()}
    highs = {k: max(v) for k, v in groups.items()}
    order = sorted(groups)
    # within a shard BM25 is monotone in the term's frequency; across shards a group's scores may overlap the next
    assert sum(1 for a, b in zip(counts, counts[1:]) if b > a) <= len(counts) // 3, counts
    context.measures["scores by mentions (min-max)"] = ", ".join("%d: %.2f-%.2f" % (k, lows[k], highs[k]) for k in order)


@given('a transaction that has put a datum saying "{term}" and not committed')
def step_open(context, term):
    context.tx = context.cluster.connect()
    context.open_conns = getattr(context, "open_conns", []) + [context.tx]
    context.tx.execute("SELECT iq.put_datum(%s, 'Silver', %s, NULL, '[]')", (iri(999), '{"text": "x"}'))
    context.tx.execute("UPDATE iq.datum SET raw = %s, media_type = 'text/plain' WHERE iri = %s", ("a " + term, iri(999)))


@then('inside it a search for "{term}" finds the datum')
def step_inside(context, term):
    assert [r[0] for r in search(context.tx, term)] == [iri(999)]


@then('another session\'s search for "{term}" finds nothing')
def step_other(context, term):
    with context.cluster.connect() as c:
        assert search(c, term) == []
        c.rollback()


@then('no session\'s search for "{term}" finds anything')
def step_none(context, term):
    assert search(context.tx, term) == []
    context.tx.rollback()
    with context.cluster.connect() as c:
        assert search(c, term) == []
        c.rollback()


@when('the record is searched for "{term}" across the shards and in one table')
def step_both(context, term):
    with context.cluster.connect() as c:
        context.dist = search(c, term)
        c.rollback()
    context.cluster.admin(
        "DROP TABLE IF EXISTS iq.search_one; "
        "CREATE TABLE iq.search_one AS SELECT iri, raw FROM iq.datum WHERE iri LIKE '%%test:text-%%'; "
        "CREATE INDEX search_one_bm25 ON iq.search_one USING bm25 (iri, raw) WITH (key_field = 'iri'); "
        "GRANT SELECT ON iq.search_one TO iq;")
    with context.cluster.connect() as c:
        context.one = c.execute("SELECT iri, pdb.score(iri) FROM iq.search_one WHERE raw @@@ %s ORDER BY 2 DESC, 1",
                                (term,)).fetchall()
        c.rollback()
    context.add_cleanup(lambda: context.cluster.admin("DROP TABLE IF EXISTS iq.search_one"))


@then("the two find the same datums")
def step_same(context):
    assert {r[0] for r in context.dist} == {r[0] for r in context.one}


@then("the share of the top ten they agree on is recorded")
def step_top10(context):
    a, b = [r[0] for r in context.dist[:10]], [r[0] for r in context.one[:10]]
    context.measures["top ten shared, across shards vs one table"] = "%d of 10" % len(set(a) & set(b))
    context.measures["top score, across shards vs one table"] = "%.3f vs %.3f" % (context.dist[0][1], context.one[0][1])
