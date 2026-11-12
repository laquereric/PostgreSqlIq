"""Steps for reasoning.feature: chains and rules in the closure, proofs, and "not modelled".

@use_case Development/UseCase/UC_Citus
@facet Store
@test
@license LICENSE (IQ freeware terms 1.0.0; (c) CBI Business Transactions, LLC)
"""
import json

import psycopg
from behave import given, then, when

import citus_plane as cp
import reasoning as R

T = cp.TEST + "rz:"
APP, LIB, PKG, OTHER, ADV1, ADV2 = (T + n for n in ("app", "lib", "pkg", "other", "adv1", "adv2"))
BOB, ANN, CAL = T + "bob", T + "ann", T + "cal"
DEPENDS, EXPOSED, SEVERITY, STATUS, BLOCKED = (T + n for n in ("dependsOn", "exposedTo", "severity", "status", "blocked"))
BROTHER, PARENT, UNCLE = T + "brotherOf", T + "parentOf", T + "uncleOf"
PACKAGE, ARTIFACT = T + "Package", T + "Artifact"
RULE = T + "release-blocked"


def _c(context):
    if not getattr(context, "rz", None):
        context.rz = context.cluster.connect(autocommit=True)
        context.open_conns = getattr(context, "open_conns", []) + [context.rz]
    return context.rz


def _clean(c):
    c.execute("DELETE FROM iq.rule WHERE name LIKE %s", (T + "%",))
    c.execute("DELETE FROM iq.vocab WHERE s LIKE %s OR s LIKE '_:chain-%%'", (T + "%",))
    c.execute("DELETE FROM iq.datum WHERE iri LIKE %s", (T + "%",))


@given("a release vocabulary: dependsOn transitive, a Package an Artifact, an uncle a parent's brother")
def step_vocab(context):
    c = _c(context)
    _clean(c)
    c.execute("INSERT INTO iq.vocab VALUES (%s, %s, %s), (%s, %s, %s)",
              (DEPENDS, R.A, R.TRA, PACKAGE, R.SCO, ARTIFACT))
    c.execute("SELECT iq.put_chain(%s, %s)", (UNCLE, [BROTHER, PARENT]))


@given('the rule "release-blocked": a package exposed to an advisory that is critical and open is blocked')
def step_rule(context):
    body = [["?pkg", EXPOSED, "?adv"], ["?adv", SEVERITY, '"critical"'], ["?adv", STATUS, '"open"']]
    _c(context).execute("SELECT iq.put_rule(%s, %s, %s)", (RULE, json.dumps(body), json.dumps([["?pkg", BLOCKED, '"yes"']])))


@given("the stated facts: an app depends on a lib depending on a package, the package and one other exposed to "
       "advisories, one critical and open and one critical and closed, a brother and his sister's child")
def step_facts(context):
    c = _c(context)
    facts = {T + "deps": [(APP, DEPENDS, LIB, False), (LIB, DEPENDS, PKG, False), (PKG, R.A, PACKAGE, False)],
             T + "advisories": [(PKG, EXPOSED, ADV1, False), (ADV1, SEVERITY, "critical", True), (ADV1, STATUS, "open", True),
                                (OTHER, EXPOSED, ADV2, False), (ADV2, SEVERITY, "critical", True),
                                (ADV2, STATUS, "closed", True)],
             T + "family": [(BOB, BROTHER, ANN, False), (ANN, PARENT, CAL, False)]}
    with c.transaction():
        for g, triples in facts.items():
            c.execute("INSERT INTO iq.datum (iri, tier, body) VALUES (%s, 'Bronze', '{}')", (g,))
            for s, p, o, lit in triples:
                c.execute("INSERT INTO iq.quad VALUES (%s, %s, %s, %s, %s)", (g, s, p, o, lit))
    context.closure = R.closure(c)


@then("the closure holds the first package blocked, and not the second")
def step_blocked(context):
    assert context.closure.has(PKG, BLOCKED, "yes", True)
    assert not context.closure.has(OTHER, BLOCKED, "yes", True)


@then("the scoped closure of the first package says the same")
def step_scoped(context):
    scoped = R.closure(_c(context), [PKG])
    assert scoped.has(PKG, BLOCKED, "yes", True) and not scoped.has(OTHER, BLOCKED, "yes", True)


def _proof(context, s, p, o, lit=False):
    pf = R.proof(context.closure, s, p, o, lit)
    assert pf is not None, (s, p, o)
    return pf


@then('its proof is the rule "release-blocked", resting on the three stated facts about its advisory')
def step_rule_proof(context):
    pf = _proof(context, PKG, BLOCKED, "yes", True)
    assert pf["by"] == RULE, pf
    assert R.stated(pf) == {(PKG, EXPOSED, ADV1, False), (ADV1, SEVERITY, "critical", True),
                            (ADV1, STATUS, "open", True)}, R.stated(pf)


@then("the closure holds the brother as the child's uncle")
def step_uncle(context):
    assert context.closure.has(BOB, UNCLE, CAL)


@then("its proof is the property chain, resting on the brother and the parent facts as stated")
def step_chain_proof(context):
    pf = _proof(context, BOB, UNCLE, CAL)
    assert pf["by"] == "propertyChainAxiom", pf
    assert R.stated(pf) == {(BOB, BROTHER, ANN, False), (ANN, PARENT, CAL, False)}, R.stated(pf)


@then("the app's dependency on the package is proved by the transitive property, through the lib")
def step_transitive(context):
    pf = _proof(context, APP, DEPENDS, PKG)
    assert pf["by"] == "TransitiveProperty", pf
    assert R.stated(pf) == {(APP, DEPENDS, LIB, False), (LIB, DEPENDS, PKG, False), (DEPENDS, R.A, R.TRA, False)}


@then("the package being an Artifact is proved by subClassOf, from its being a Package")
def step_subclass(context):
    pf = _proof(context, PKG, R.A, ARTIFACT)
    assert pf["by"] == "subClassOf", pf
    assert R.stated(pf) == {(PKG, R.A, PACKAGE, False), (PACKAGE, R.SCO, ARTIFACT, False)}, R.stated(pf)


@then("a fact the closure does not hold has no proof")
def step_no_proof(context):
    assert R.proof(context.closure, OTHER, BLOCKED, "yes", True) is None


BAD = {
    "whose head names a variable its body does not bind": ([["?x", T + "p", "?y"]], [["?z", T + "q", "?y"]]),
    "whose body's atoms share no variable": ([["?x", T + "p", "?y"], ["?a", T + "q", "?b"]], [["?x", T + "r", "?y"]]),
    "with a literal as a subject": ([['"x"', T + "p", "?y"]], [["?y", T + "r", T + "z"]]),
}


@when("the rule {which} is put")
def step_put_bad(context, which):
    c = _c(context)
    try:
        if which == "that is a chain of one property":
            c.execute("SELECT iq.put_chain(%s, %s)", (T + "x", [T + "a"]))
        else:
            body, head = BAD[which]
            c.execute("SELECT iq.put_rule(%s, %s, %s)", (T + "bad", json.dumps(body), json.dumps(head)))
        context.put = None
    except psycopg.Error as e:
        context.put = str(e).split("\n")[0]


@then('it is refused "{reason}"')
def step_refused(context, reason):
    assert context.put and context.put.startswith(reason + ":"), context.put
    assert not _c(context).execute("SELECT 1 FROM iq.rule WHERE name = %s", (T + "bad",)).fetchone()


@then('asking who is "{p}" answers not modelled')
def step_not_modelled_p(context, p):
    assert R.ask(_c(context), T + p) == {"not_modelled": T + p}


@then('asking what is a "{cls}" answers not modelled')
def step_not_modelled_cls(context, cls):
    assert R.ask(_c(context), R.A, T + cls) == {"not_modelled": T + cls}


@then('asking who is "{p}" with the value "{v}" answers an empty list, the store modelling it')
def step_empty(context, p, v):
    assert R.ask(_c(context), T + p, v, True) == {"answers": []}
    assert R.ask(_c(context), T + p, "yes", True) == {"answers": [PKG]}
