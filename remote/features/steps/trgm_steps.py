"""Steps for trgm.feature (the datums of text are search_steps.py's).

@use_case Development/UseCase/UC_Citus
@facet Store
@status Proposed
"""
import time

from behave import given, then, when

import citus_plane as cp


@given("the trigram index on the record")
def step_index(context):
    if context.cluster.admin("SELECT count(*) FROM pg_available_extensions WHERE name = 'pg_trgm'") != "1":
        context.scenario.skip("no pg_trgm on this cluster (postgresql17-contrib)")
        return
    context.cluster.admin((cp.HERE / "trgm.sql").read_text())


@when('the record is searched for words like "{q}"')
def step_like(context, q):
    with context.cluster.connect() as c:
        t0 = time.perf_counter()
        context.found = c.execute("SELECT iri, word_similarity(%s, raw) FROM iq.datum WHERE raw %%> %s "
                                  "AND iri LIKE '%%test:text-%%' ORDER BY 2 DESC, 1", (q, q)).fetchall()
        context.measures.setdefault("a trigram search across the shards", "%.0f ms" % ((time.perf_counter() - t0) * 1000))
        c.rollback()


@when('the nearest {n:d} datums to "{q}" are asked')
def step_nearest(context, n, q):
    context.knn = ("SELECT iri, raw FROM iq.datum WHERE iri LIKE '%%test:text-%%' ORDER BY raw <->> %s, iri LIMIT %s", (q, n))
    with context.cluster.connect() as c:
        context.nearest = c.execute(*context.knn).fetchall()
        c.rollback()
    assert len(context.nearest) == n, context.nearest


@then('each of them says "{word}"')
def step_says(context, word):
    assert all(word in raw.split() for _, raw in context.nearest), context.nearest


@then('every shard\'s plan for it scans its "{index}" index')
def step_plan(context, index):
    # 60 rows are too few for the planner to prefer an index on its own; with sequential scans off, the plan shows
    # whether the shard CAN walk the GiST index for the distance order (Citus EXPLAIN ALL shows every task)
    sql, params = context.knn
    with context.cluster.connect() as c:
        c.execute("SET LOCAL citus.propagate_set_commands TO 'local'")
        c.execute("SET LOCAL enable_seqscan TO off")
        c.execute("SET LOCAL citus.explain_all_tasks TO on")
        plan = "\n".join(r[0] for r in c.execute("EXPLAIN (COSTS OFF) " + sql, params).fetchall())
        c.rollback()
    with context.cluster.connect(autocommit=True) as c:
        shards = c.execute("SELECT count(*) FROM pg_dist_shard WHERE logicalrelid = 'iq.datum'::regclass").fetchone()[0]
    scans = plan.count("Index Scan using %s_" % index)
    assert scans == shards, (scans, shards, plan)
    context.measures["shards walking %s for the nearest" % index] = "%d of %d" % (scans, shards)


@then("every shard of iq.datum has the index as GiST with gist_trgm_ops")
def step_gist(context):
    with context.cluster.connect(autocommit=True) as c:
        shards = c.execute("SELECT count(*) FROM pg_dist_shard WHERE logicalrelid = 'iq.datum'::regclass").fetchone()[0]
        rows = c.execute(
            "SELECT result FROM run_command_on_workers($$SELECT count(*) FROM pg_index i JOIN pg_class x ON x.oid = i.indexrelid "
            "JOIN pg_am a ON a.oid = x.relam JOIN pg_opclass o ON o.oid = i.indclass[0] "
            "WHERE x.relname LIKE 'datum\\_trgm\\_%' AND a.amname = 'gist' AND o.opcname = 'gist_trgm_ops'$$)").fetchall()
    assert sum(int(r[0]) for r in rows) == shards, (rows, shards)
