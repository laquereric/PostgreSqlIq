"""Steps for shacl.feature.

@use_case Development/UseCase/UC_Citus
@facet Store
@status Proposed
"""
import collections
import json
import time

import psycopg
from behave import given, then, when

import citus_plane as cp
import shacl

STORE = "https://w3id.org/iq/store#"
XSD = shacl.XSD
A = cp.RDF + "type"
GRAPH = cp.TEST + "shapes:store"
STORE_SHAPES = next(f for f in shacl.FACET_SHAPES if f.parent.parent.name == "Store")


def iri(name):
    return cp.NS + name


def lit(v, dt=XSD + "string"):
    return (v, True, dt)


def claim(name, cls=STORE + "Claim", drop=(), extra=()):
    i = iri(name)
    props = {"subject_iri": lit("iq:engine:postgres"), "predicate": lit("iq:license"), "object": lit("PostgreSQL"),
             "derived_from": lit("iq:blob:abc"), "source_span": lit("0:10"), "trust_tier": lit("T1_user_direct"),
             "confidence": lit("0.9", XSD + "float")}
    quads = [(i, A, cls, False, None)]
    quads += [(i, STORE + k, *v) for k, v in props.items() if k not in drop]
    return i, quads + [(i, p, *v) for p, v in extra]


def commit(context, *datums):
    """Put datums in one transaction and commit; the refusal, if any, is kept with the shapes' report."""
    context.refused, context.report = None, []
    c = context.cluster.connect()
    context.open_conns = getattr(context, "open_conns", []) + [c]
    try:
        for i, quads in datums:
            cp.put(c, i, quads, body={"iri": i})
        t0 = time.perf_counter()
        c.commit()
        context.measures.setdefault("a commit held to the shapes (one Claim)", "%.0f ms" % ((time.perf_counter() - t0) * 1000))
    except psycopg.Error as e:
        context.refused = e
        if isinstance(e, psycopg.errors.CheckViolation) and e.diag.message_detail:
            context.report = json.loads(e.diag.message_detail)
        c.rollback()


@given("the Store facet's shapes are loaded")
def step_shapes(context):
    context.compiled = shacl.compile_shapes(STORE_SHAPES.read_text(), GRAPH)
    with context.cluster.connect() as c:
        shacl.load(c, context.compiled)
        c.commit()


@then("{n:d} of the {total:d} shapes graphs compile")
def step_compile(context, n, total):
    graphs = shacl.FACET_SHAPES + [shacl.ROOT / "Development/UseCase/UC_MarkovKernel/markov-kernel-shapes.ttl"]
    ok, context.unsupported, props = 0, {}, 0
    for f in graphs:
        try:
            props += len(shacl.compile_shapes(f.read_text(), "x").properties)
            ok += 1
        except shacl.Unsupported as e:
            context.unsupported[f.name] = e.found
    assert (ok, len(graphs)) == (n, total), (ok, len(graphs), context.unsupported)
    context.measures["IQ's LinkML shapes compiled"] = "%d of %d graphs, %d property shapes" % (ok, len(graphs), props)


@then('the ScapeShape shapes are refused for "{a}" and the MarkovKernel shapes for "{b}"')
def step_refused_by_name(context, a, b):
    assert any(a in x for x in context.unsupported["scapeshape.shacl.ttl"]), context.unsupported
    assert any(b in x for x in context.unsupported["markov-kernel-shapes.ttl"]), context.unsupported


@given('a Claim "{name}" with every required property is committed')
@when('a Claim "{name}" with every required property is committed')
def step_claim_ok(context, name):
    commit(context, claim(name))


@then('"{name}" answers')
def step_answers(context, name):
    assert context.refused is None, context.refused
    with context.cluster.connect(autocommit=True) as c:
        assert c.execute("SELECT count(*) FROM iq.datum WHERE iri = %s", (iri(name),)).fetchone()[0] == 1


BREAKS = {
    "no subject_iri": dict(drop=("subject_iri",)),
    "two objects": dict(extra=((STORE + "object", lit("MySQL")),)),
    "a confidence that is not a float": dict(drop=("confidence",), extra=((STORE + "confidence", lit("high")),)),
    "a trust_tier off its list": dict(drop=("trust_tier",), extra=((STORE + "trust_tier", lit("T9_rumour")),)),
    "an IRI for its source_span": dict(drop=("source_span",), extra=((STORE + "source_span", ("urn:iq:span", False, None)),)),
    "a property its closed shape does not name": dict(extra=((iri("test:extra"), lit("x")),)),
}


@when('a Claim "{name}" broken by {change} is committed')
def step_claim_bad(context, name, change):
    commit(context, claim(name, **BREAKS[change]))


@then('the commit is refused by the shapes, naming "{path}" for "{constraint}"')
def step_refused(context, path, constraint):
    assert isinstance(context.refused, psycopg.errors.CheckViolation), context.refused
    full = iri(path) if path.startswith("test:") else STORE + path
    hits = [r for r in context.report if r["path"] == full and r["constraint_name"] == constraint]
    assert hits, json.dumps(context.report, indent=1)[:1500]


@then('nothing of "{name}" answers')
def step_nothing(context, name):
    with context.cluster.connect(autocommit=True) as c:
        assert c.execute("SELECT count(*) FROM iq.datum WHERE iri = %s", (iri(name),)).fetchone()[0] == 0
        assert c.execute("SELECT count(*) FROM iq.quad WHERE g = %s", (iri(name),)).fetchone()[0] == 0


@given('the vocabulary makes "{name}" a subclass of Claim')
def step_subclass(context, name):
    with context.cluster.connect() as c:
        c.execute("INSERT INTO iq.vocab (s, p, o) VALUES (%s, %s, %s)", (iri(name), cp.RDFS + "subClassOf", STORE + "Claim"))
        c.commit()


@when('an AuditClaim "{name}" with no subject_iri is committed')
def step_audit(context, name):
    commit(context, claim(name, cls=iri("test:AuditClaim"), drop=("subject_iri",)))


def blob(name):
    i = iri(name)
    return i, [(i, A, STORE + "Blob", False, None), (i, STORE + "sha", *lit("abc123"))]


def file_at(name, blob_name):
    i = iri(name)
    return i, [(i, A, STORE + "FileAt", False, None), (i, STORE + "path", *lit("docs/a.md")),
               (i, STORE + "blob", iri(blob_name), False, None)]


@when('one transaction commits a Blob "{b}" and a FileAt "{f}" whose blob is it')
def step_blob_file(context, b, f):
    commit(context, file_at(f, b), blob(b))       # the FileAt first: its Blob is typed later in the same transaction


@when('a FileAt "{f}" whose blob is "{other}" is committed')
def step_file_wrong(context, f, other):
    commit(context, file_at(f, other))


@when('"{name}" is put again without its object')
def step_again(context, name):
    commit(context, claim(name, drop=("object",)))


@then('"{name}" still has its object')
def step_still(context, name):
    with context.cluster.connect(autocommit=True) as c:
        assert c.execute("SELECT count(*) FROM iq.quad WHERE g = %s AND p = %s", (iri(name), STORE + "object")).fetchone()[0] == 1


@given("two Claims on different workers, the second with no subject_iri")
def step_two(context):
    seen = {}
    for n in range(200):
        name = "test:claim-w%d" % n
        seen.setdefault(context.cluster.node_of(iri(name)), name)
        if len(seen) == 2:
            break
    context.claims = list(seen.values())


@when("one transaction commits both")
def step_commit_both(context):
    a, b = context.claims
    commit(context, claim(a), claim(b, drop=("subject_iri",)))


@then("neither Claim answers")
def step_neither(context):
    for name in context.claims:
        step_nothing(context, name)


@when("the shapes make a Claim's extractor required")
def step_extractor(context):
    with context.cluster.connect() as c:
        n = c.execute("UPDATE iq.shape_property SET min_count = 1 WHERE graph = %s AND shape = %s AND path = %s",
                      (GRAPH, STORE + "Claim", STORE + "extractor")).rowcount
        c.commit()
    assert n == 1


def as_role(context, role):
    c = context.cluster.connect(role=role) if role != "iq_agent" else context.cluster.connect()
    context.open_conns = getattr(context, "open_conns", []) + [c]
    if role == "iq_agent":
        c.execute("SET ROLE iq_agent")
    return c


@then("as the agent role a triple written straight into the record is denied")
def step_agent_raw(context):
    c = as_role(context, "iq_agent")
    try:
        c.execute("INSERT INTO iq.quad (g, s, p, o) VALUES (%s, %s, %s, %s)", (iri("test:raw"), iri("test:raw"), A, STORE + "Claim"))
        raise AssertionError("the agent wrote around the shapes")
    except psycopg.errors.InsufficientPrivilege:
        c.rollback()


@then("as the agent role the governed write itself is denied")
def step_agent_put(context):
    c = as_role(context, "iq_agent")
    i, quads = claim("test:agent-try")
    try:
        cp.put(c, i, quads, body={"iri": i})
        raise AssertionError("the agent called the governed write")
    except psycopg.errors.InsufficientPrivilege:
        c.rollback()


@then("as the gate's writer a Claim with no subject_iri is refused by the shapes")
def step_gate_bad(context):
    c = as_role(context, "iq_gate")
    i, quads = claim("test:agent-bad", drop=("subject_iri",))
    cp.put(c, i, quads, body={"iri": i})
    try:
        c.commit()
        raise AssertionError("committed")
    except psycopg.errors.CheckViolation:
        c.rollback()


@then('as the gate\'s writer a Claim "{name}" with every required property commits')
def step_gate_ok(context, name):
    c = as_role(context, "iq_gate")
    i, quads = claim(name)
    cp.put(c, i, quads, body={"iri": i})
    c.commit()
    with context.cluster.connect(autocommit=True) as q:
        assert q.execute("SELECT count(*) FROM iq.datum WHERE iri = %s", (i,)).fetchone()[0] == 1


@when("{n:d} datums generated from the Store shapes, most broken one way, are checked by the cluster inside a transaction")
def step_generated(context, n):
    context.cases = shacl.generate(context.compiled, cp.TEST, n)
    with context.cluster.connect() as c:
        t0 = time.perf_counter()
        for i, quads in context.cases:
            cp.put(c, i, quads, body={"iri": i})
        t1 = time.perf_counter()
        context.got = shacl.report(c, [i for i, _ in context.cases])
        t2 = time.perf_counter()
        c.rollback()                                   # checked, never committed
    context.measures["%d generated datums: put / checked on demand" % n] = "%.1f s / %.1f s (%.0f ms a datum)" % (
        t1 - t0, t2 - t1, (t2 - t1) * 1000 / n)


@when("the same datums are checked by UC_OntologyPipeline's validator")
def step_oracle(context):
    data = "".join(shacl.ntriples(q) for _, q in context.cases)
    context.want = shacl.oracle(STORE_SHAPES.read_text(), "", data)


@then("the two reports are the same")
def step_same(context):
    only_o, only_c = sorted(context.want - context.got), sorted(context.got - context.want)
    assert not only_o and not only_c, ("oracle only", only_o[:5], "cluster only", only_c[:5])
    context.measures["violations, cluster = OntologyPipeline's validator"] = "%d = %d" % (len(context.got), len(context.want))


@then("the report has violations of every kind: {kinds}")
def step_kinds(context, kinds):
    seen = collections.Counter(v[3] for v in context.got)
    missing = [k.strip() for k in kinds.split(",") if not seen[k.strip()]]
    assert not missing, (missing, seen)
    context.measures["violations by kind"] = ", ".join("%s %d" % kv for kv in sorted(seen.items()))
