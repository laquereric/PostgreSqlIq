"""Steps for gate.feature.

@use_case Development/UseCase/UC_Citus
@facet Agent
@status Proposed
"""
import json
import shutil
import sys
import tempfile
from pathlib import Path

from behave import given, then, when

import citus_plane as cp
import gated_plane as gp

sys.path.insert(0, str(cp.ROOT / "Development/UseCase/UC_Progress/gated_mcp"))
import demo_gate  # noqa: E402
from dataplane import Datum  # noqa: E402
from ml_mcp import Gate  # noqa: E402

CLAIM = {"subject_iri": "iq:engine:postgres", "predicate": "iq:license", "object": "PostgreSQL",
         "derived_from": "iq:blob:abc", "source_span": "0:10"}


@given("a gate with the cluster's writing policy")
def step_gate(context):
    wd = Path(tempfile.mkdtemp(prefix="iqcg", dir="/tmp"))
    context.gate = demo_gate.start(wd, cp.HERE / "gate/iam.json")
    context.add_cleanup(context.gate.stop)
    context.add_cleanup(shutil.rmtree, wd, True)


@given("the gate's writer on the cluster")
def step_writer(context):
    blobs = Path(tempfile.mkdtemp(prefix="iqcb", dir="/tmp"))
    context.add_cleanup(shutil.rmtree, blobs, True)
    gate = Gate(context.gate.url, context.gate.token, demo_gate.AGENT, demo_gate.REALM)
    context.writer = gp.GatedWriter(context.cluster, gate, blobs)
    with context.cluster.connect() as c:
        c.execute("DELETE FROM iq.datum WHERE media_type IS NOT NULL")
        c.commit()
    context.add_cleanup(lambda: _clean(context))


def _clean(context):
    with context.cluster.connect() as c:
        c.execute("DELETE FROM iq.datum WHERE media_type IS NOT NULL")
        c.commit()


@then("only the cluster's owner and the gate's writer may call the governed write")
def step_who(context):
    out = context.cluster.admin(
        "SELECT string_agg(r.rolname, ',' ORDER BY r.rolname) FROM pg_roles r WHERE r.rolname LIKE 'iq%' AND "
        "has_function_privilege(r.rolname, 'iq.put_datum(text, text, jsonb, vector, jsonb, text, text, text[], text, text)', 'EXECUTE')")
    assert out == "iq,iq_gate", out


@when('the writer puts the value {body} as "{iri}" at "{tier}"')
def step_put(context, body, iri, tier):
    context.result = context.writer.put(Datum(iri, tier, body))


@then("the put is ok")
def step_ok(context):
    assert context.result.ok, context.result


@then("the datum on the cluster carries the operation the gate admitted")
def step_carries(context):
    with context.cluster.connect(autocommit=True) as c:
        op = c.execute("SELECT operation FROM iq.datum WHERE iri = %s", (cp.expand(context.result.iri),)).fetchone()[0]
    admitted = {d["operation_id"] for d in context.gate.decisions() if d.get("admitted") and d["tool"].startswith("dataplane:put")}
    assert op and op in admitted, (op, admitted)


@then('the put is refused as "{reason}"')
def step_refused(context, reason):
    assert not context.result.ok and context.result.reason == reason, context.result


@then('the cluster holds nothing at "{iri}"')
def step_nothing(context, iri):
    with context.cluster.connect(autocommit=True) as c:
        assert c.execute("SELECT count(*) FROM iq.datum WHERE iri = %s", (cp.expand(iri),)).fetchone()[0] == 0


@then("the gate's journal has the refusal")
def step_journal(context):
    refused = [d for d in context.gate.decisions() if not d.get("admitted") and d["tool"] == "dataplane:put:ux"]
    assert refused and refused[-1]["reason"] == "no_permit", refused


@when('the writer puts a Claim with no subject_iri as "{iri}" at "{tier}"')
def step_bad_claim(context, iri, tier):
    turtle = "@prefix store: <https://w3id.org/iq/store#> .\n<%s> a store:Claim ;\n" % cp.expand(iri) + " ;\n".join(
        '  store:%s "%s"' % (k, v) for k, v in CLAIM.items() if k != "subject_iri") + " ."
    try:
        context.result = context.writer.put(Datum(iri, tier, turtle, "text/turtle"))
        context.error = None
    except Exception as e:                                     # the shapes refuse at COMMIT, inside the adapter
        context.error = e


@then("the put fails on the shapes")
def step_shapes(context):
    import psycopg
    assert isinstance(context.error, psycopg.errors.CheckViolation), context.error
    admitted = [d for d in context.gate.decisions() if d.get("admitted") and d["tool"] == "dataplane:put:onto"]
    assert admitted, "the gate did not admit it first"


@when("the writer puts {n:d} graphs and is refused {m:d} journeys")
def step_many(context, n, m):
    for i in range(n):
        assert context.writer.put(Datum("iq:onto:demo:graph:g%d@1" % i, "Silver", json.dumps({"n": i}))).ok
    for i in range(m):
        assert not context.writer.put(Datum("iq:ux:journey:j%d" % i, "Silver", json.dumps({"n": i}))).ok


@then("the cluster agrees with the gate's journal")
def step_audit(context):
    a = context.writer.audit(context.gate.decisions())
    assert a["rows"] == 10 and not a["without_operation"] and not a["not_admitted"] and not a["admitted_puts_missing"], a
    context.measures["datums on the cluster / every one admitted by the gate"] = "%d / yes" % a["rows"]
