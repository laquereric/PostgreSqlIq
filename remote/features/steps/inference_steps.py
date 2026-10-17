"""Steps for inference.feature.

@use_case Development/UseCase/UC_Citus
@facet Store
@status Proposed
"""
import json
import random
import sys
import time

from behave import given, then, when

import citus_plane as cp

sys.path.insert(0, str(cp.ROOT / "GherkinIq/Facet/Store"))
from dataplane import BlobAdapter, Datum, LocalObjects, Plane  # noqa: E402
from sparql_citus import CitusGraphAdapter, CitusRecordAdapter, CitusStore  # noqa: E402

PLATFORM = "iq:onto:platform:graph:self@1"


def closure(c, seeds=None):
    if seeds is None:
        rows = c.execute("SELECT s, p, o, lit FROM iq.entail()").fetchall()
    else:
        rows = c.execute("SELECT s, p, o, lit FROM iq.entail(%s)", (seeds,)).fetchall()
    c.rollback()
    return {tuple(r) for r in rows}


def agree(whole, scoped):
    """The scoped closure says exactly what the whole says about every node it reaches (as a subject)."""
    nodes = {t[0] for t in scoped}
    a = {t for t in whole if t[0] in nodes}
    b = {t for t in scoped if t[0] in nodes}
    return a == b, len(a ^ b)


@then("for each competency question's anchor, and for the class Store, the scoped closure agrees with the whole")
def step_platform_agrees(context):
    with context.cluster.connect() as c:
        whole = closure(c)
        for qid, _, _, (s, p, o), _ in cp.questions():
            seeds = [x for x in (s, o) if x and x.startswith("http")] or [o]
            ok, diff = agree(whole, closure(c, seeds))
            assert ok, (qid, seeds, diff)
        ok, diff = agree(whole, closure(c, [cp.NS + "Store"]))
        assert ok, diff


@when("the platform is copied {sizes} times, each copy its own component")
def step_copies(context, sizes):
    context.sizes = [int(x) for x in sizes.replace(" and ", ", ").split(", ")]
    context.scale = []
    loaded, quads = 0, 0
    for k in context.sizes:
        quads += cp.load_copies(context.cluster, k, loaded)
        loaded = k
        with context.cluster.connect() as c:
            t0 = time.perf_counter()
            whole = closure(c)
            t_whole = time.perf_counter() - t0
            picks = random.Random(k).sample(range(k), 3)
            runs = []
            for j in picks:
                seed = [cp.NS + "test:scale-c%d-engine:age" % j]
                t0 = time.perf_counter()
                sc = closure(c, seed)
                runs.append((time.perf_counter() - t0, len(sc), agree(whole, sc)))
        context.scale.append({"copies": k, "quads": quads, "whole": len(whole), "t_whole": t_whole, "runs": runs})
        context.measures["%d copies (%d quads): whole closure" % (k, quads)] = "%d triples, %.2f s" % (len(whole), t_whole)
        context.measures["%d copies: scoped closure of a copy (3 copies)" % k] = ", ".join(
            "%d triples %.2f s" % (n, t) for t, n, _ in runs)


@then("at each size the scoped closure of a copy agrees with the whole closure about every node it reaches")
def step_scale_agrees(context):
    for row in context.scale:
        for _, _, (ok, diff) in row["runs"]:
            assert ok, (row["copies"], diff)


@then("the scoped closure of a copy is the same size at every size of the graph")
def step_flat(context):
    sizes = {n for row in context.scale for _, n, _ in row["runs"]}
    assert len(sizes) == 1, sizes


@then("at the largest size the scoped closure answers at least {x:d} times faster than the whole")
def step_faster(context, x):
    last = context.scale[-1]
    worst = max(t for t, _, _ in last["runs"])
    context.measures["at %d copies: whole / slowest scoped" % last["copies"]] = "%.1fx" % (last["t_whole"] / worst)
    assert last["t_whole"] >= x * worst, (last["t_whole"], worst)


def plane(context, inference):
    context.store = CitusStore(context.cluster.connect, inference=inference)
    objects = LocalObjects(cp.HERE / ".plane-blobs")
    context.plane = Plane([CitusRecordAdapter(context.store), CitusGraphAdapter(context.store), BlobAdapter(objects)])
    with context.cluster.connect() as c:
        c.execute("DELETE FROM iq.datum WHERE media_type IS NOT NULL")
        c.commit()
    context.add_cleanup(lambda: _clean_plane(context))


def _clean_plane(context):
    with context.cluster.connect() as c:
        c.execute("DELETE FROM iq.datum WHERE media_type IS NOT NULL")
        c.commit()


@given("a data plane on the cluster whose graph answers with inference")
def step_plane_inf(context):
    plane(context, True)


@given("a data plane on the cluster")
def step_plane(context):
    plane(context, False)


@when("the platform's Turtle is put on the plane as one graph")
def step_put_platform(context):
    ttl = (cp.INFERENCE / "platform.ttl").read_text()
    r = context.plane.put(Datum(PLATFORM, "Silver", ttl, "text/turtle"))
    assert r.ok, r


def answer(context, sparql, **kw):
    out = context.store.sparql(sparql, **kw)
    return {cp.short(r[0]) for r in out["rows"]}


@then("every competency question answers through the plane as on MarkLogic")
def step_plane_questions(context):
    src = (cp.INFERENCE / "compare.py").read_text()
    ns = {}
    exec(src[src.index("STORES5 = "):src.index("]\n\n", src.index("QUESTIONS = [")) + 1], ns)
    for qid, _, _, sparql, expected in ns["QUESTIONS"]:
        t0 = time.perf_counter()
        got = answer(context, sparql)
        ms = (time.perf_counter() - t0) * 1000
        assert got == set(expected), (qid, sorted(got), sorted(expected))
        context.measures["%s through plane_sparql with inference (%s)" % (qid, context.store.last_plan)] = "%.0f ms" % ms
    context.questions = ns["QUESTIONS"]


@then("without inference the same questions answer empty")
def step_plane_empty(context):
    for qid, _, _, sparql, _ in context.questions:
        assert answer(context, sparql, inference=False) == set(), qid


SUITE = [
    "SELECT ?x WHERE { ?x a <https://iq.intentionalquality.com/ns#TripleStore> }",
    "SELECT ?x ?y WHERE { ?x <https://iq.intentionalquality.com/ns#derivedFrom> ?y }",
    "SELECT ?x ?y WHERE { ?x <https://iq.intentionalquality.com/ns#derivedFrom> ?m . ?m <https://iq.intentionalquality.com/ns#derivedFrom> ?y }",
    "SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } }",
    "SELECT ?s ?o WHERE { ?s <https://iq.intentionalquality.com/ns#peerOf> ?o . FILTER (?s != ?o) }",
    "ASK { ?s <https://iq.intentionalquality.com/ns#license> \"Apache-2.0\" }",
    "SELECT ?s WHERE { ?s ?p <https://iq.intentionalquality.com/ns#datum:silver> } LIMIT 5",
]


@then("every query without OPTIONAL answers the same on the workers as on the coordinator")
def step_pushdown(context):
    workers = pulled = 0.0
    for q in SUITE:
        t0 = time.perf_counter()
        a = context.store.sparql(q, pushdown=True)
        t1 = time.perf_counter()
        assert context.store.last_plan == "on the workers", context.store.last_plan
        b = context.store.sparql(q, pushdown=False)
        t2 = time.perf_counter()
        norm = lambda r: json.dumps(r.get("boolean", sorted(map(tuple, r.get("rows", [])))), sort_keys=True)
        assert norm(a) == norm(b), (q, a, b)
        workers, pulled = workers + t1 - t0, pulled + t2 - t1
    context.measures["%d queries on the workers / pulled to the coordinator (summed)" % len(SUITE)] = \
        "%.0f ms / %.0f ms" % (workers * 1000, pulled * 1000)


@then("a query of one pattern is planned on the workers, a join pulled to the coordinator")
def step_plans(context):
    context.store.sparql(SUITE[0])
    assert context.store.last_plan == "on the workers", context.store.last_plan
    context.store.sparql(SUITE[2])
    assert context.store.last_plan == "pulled to the coordinator", context.store.last_plan
