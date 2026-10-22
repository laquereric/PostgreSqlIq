"""Steps for the cluster's record, distributed by owner (domains.feature).

@use_case Development/UseCase/UC_Citus
@facet Gherkin
"""
import json
import sys
from pathlib import Path

from behave import given, then, when

REMOTE = Path(__file__).resolve().parents[2]
ROOT = REMOTE.parents[2]
for _p in (REMOTE.parent / "cdc", ROOT / "GherkinIq" / "Facet" / "Store"):
    if str(_p) not in sys.path:
        sys.path.append(str(_p))
import apply  # noqa: E402
import cdc  # noqa: E402
from dataplane import ClusterAdapter  # noqa: E402

TABLE = "iq.cluster_datum"
GROUP, USER = "g-" + "0c1d7e5" * 3 + "0c1", "u-" + "0c1d7e5" * 3 + "0c2"       # test owners, removed after


def _dsn(context):
    cl = context.cluster
    return "postgresql://iq:%s@127.0.0.1:%d/iq?connect_timeout=10" % (cl.password, cl.port)


def _clean(context):
    context.cluster.admin("DELETE FROM %s WHERE owner IN ('%s', '%s')" % (TABLE, GROUP, USER))


def _records(ns, iris):
    """What a domain's slot ships for these inserts (cdc.record's shape), one transaction."""
    out = []
    for i, iri in enumerate(iris, 1):
        lsn = "0/%X" % (0x1000 + i)
        out.append({"service": cdc.SERVICE, "host": ns, "table": apply.PLANE_TABLE, "op": "cdc:insert",
                    "lsn": lsn, "commit_lsn": "0/2000", "xid": "7", "at": "2026-10-22T00:00:00+00:00",
                    "new": {"iri": iri, "tier": "Bronze", "body": json.dumps({"n": i}), "media_type": "application/json",
                            "derived_from": "{}", "recorded_at": "2026-10-22T00:00:00+00:00"}, "old": None})
    return out


@given("the cluster's record is distributed by owner")
def step_distributed(context):
    _clean(context)
    context.add_cleanup(_clean, context)
    assert ClusterAdapter(_dsn(context), TABLE).distribution() == "owner"


@when("a group's domain ships {g:d} rows and a user's domain ships {u:d}, and the cluster applies each through its plane")
def step_apply(context, g, u):
    context.shipped = {GROUP: ["iq:group:%s/r%d" % (GROUP, i) for i in range(g)],
                       USER: ["iq:note:%s/n%d" % (USER, i) for i in range(u)]}
    for ns, iris in context.shipped.items():
        r = apply.apply_domain(_dsn(context), _records(ns, iris), host=ns, namespace=ns, table=TABLE)
        assert r["ok"] and r["put"] == len(iris), r


@then("every row is on the cluster under its domain's owner")
def step_owned(context):
    with context.cluster.connect(autocommit=True) as c:
        rows = c.execute("SELECT owner, iri FROM %s WHERE owner IN (%%s, %%s) ORDER BY iri" % TABLE, (GROUP, USER)).fetchall()
    want = sorted((ns, i) for ns, iris in context.shipped.items() for i in iris)
    assert sorted(rows) == want, rows


def _worker_of(context, ns):
    with context.cluster.connect(autocommit=True) as c:
        shard, node, port = c.execute(
            "SELECT pl.shardid, n.nodename, n.nodeport FROM pg_dist_placement pl JOIN pg_dist_node n USING (groupid)"
            " WHERE pl.shardid = get_shard_id_for_distribution_column(%s, %s::text)", (TABLE, ns)).fetchone()
    return shard, "%s:%d" % (node, port)


@then("each domain's rows sit on the one worker that holds its owner's shard, and on no other")
def step_local(context):
    for ns, iris in context.shipped.items():
        shard, home = _worker_of(context, ns)
        for key in context.cluster.topo["workers"]:
            if not cdc_in_cluster(context, key):
                continue
            held = context.cluster.admin("SELECT to_regclass('iq.cluster_datum_%d') IS NOT NULL" % shard, node=key)
            if key == home:
                count = context.cluster.admin("SELECT count(*) FROM iq.cluster_datum_%d WHERE owner = '%s'"
                                              % (shard, ns), node=key)
                assert held == "t" and int(count) == len(iris), (ns, key, held, count)
            else:
                assert held == "f", (ns, key, "holds the shard too")


def cdc_in_cluster(context, key):
    import citus_plane as cp
    return cp.in_cluster(context.cluster, key)


@then("reading one domain's rows is a single-shard query")
def step_router(context):
    with context.cluster.connect(autocommit=True) as c:
        plan = "\n".join(r[0] for r in c.execute("EXPLAIN SELECT iri FROM %s WHERE owner = %%s" % TABLE, (GROUP,)))
    assert "Task Count: 1" in plan, plan


@then("restoring each domain by owner selects exactly its rows, the same values")
def step_restore(context):
    adapter = ClusterAdapter(_dsn(context), TABLE)
    for ns, iris in context.shipped.items():
        assert adapter.owned_by(ns) == sorted(iris), adapter.owned_by(ns)
        for n, iri in enumerate(sorted(iris, key=lambda x: int(x.rsplit("/", 1)[1][1:])), 1):
            assert adapter.get(iri).datum.body == json.dumps({"n": n}), iri


@then("applying the slots again puts nothing new")
def step_again(context):
    for ns, iris in context.shipped.items():
        r = apply.apply_domain(_dsn(context), _records(ns, iris), host=ns, namespace=ns, table=TABLE)
        assert r["ok"] and r["put"] == 0 and r["already"] == len(iris), r


@when("a group's domain ships a row of a user's namespace among its own, and the cluster applies it")
def step_foreign(context):
    context.foreign = "iq:note:%s/smuggled" % USER
    iris = ["iq:group:%s/r0" % GROUP, context.foreign]
    context.applied = apply.apply_domain(_dsn(context), _records(GROUP, iris), host=GROUP, namespace=GROUP, table=TABLE)


@then('that row is refused as "{reason}", and the cluster holds nothing of that user')
def step_refused(context, reason):
    assert context.applied["refused"] == ["%s: %s" % (context.foreign, reason)] and context.applied["put"] == 1, \
        context.applied
    assert ClusterAdapter(_dsn(context), TABLE).owned_by(USER) == []
