"""Reasoning that answers with its proof, or says the store does not model the question (docs/architecture/
ONTOLOGY_ENGINEERING.md slice 7).

@use_case Development/UseCase/UC_Citus
@facet Store
@model
@license LICENSE (IQ freeware terms 1.0.0; (c) CBI Business Transactions, LLC)

iq.entail() derives; this says why. Over the closure (stated and inferred, as iq.entail() answers it):

    proof(c, s, p, o)    a fact's derivation, down to stated facts: {fact, by, from: [proofs]}, `by` the rule
                         ("stated" for a fact as written, the vocabulary's own: subClassOf, domain, range,
                         subPropertyOf, equivalentClass, the transitive and symmetric property, inverseOf, the property
                         chain; a rule held as data by its name). Each step is checked against the closure: every premise
                         must be in it, and a premise is proved in turn, never through the fact it proves. None when
                         the fact is not in the closure; {by: "unexplained"} for one the closure holds that these steps
                         cannot account for (sameAs and the functional properties are not explained here)
    ask(c, p, o=None)    the subjects of `p` (with `o`, of that value), from the closure; or {not_modelled: p} when
                         neither the vocabulary, a rule nor any datum uses `p` (and, for rdf:type, the class `o`): the
                         store has no word for the question, which is an answer, not an empty list

A fact is (s, p, o, lit): `lit` whether o is a literal.
"""
from __future__ import annotations

import json

RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
OWL = "http://www.w3.org/2002/07/owl#"
A, SCO, SPO, DOM, RNG = RDF + "type", RDFS + "subClassOf", RDFS + "subPropertyOf", RDFS + "domain", RDFS + "range"
EQC, INV, TRA, SYM = OWL + "equivalentClass", OWL + "inverseOf", OWL + "TransitiveProperty", OWL + "SymmetricProperty"
CHAIN, FIRST, REST = OWL + "propertyChainAxiom", RDF + "first", RDF + "rest"


class Closure:
    """The closure in memory, indexed: (s, p, o, lit) -> stated?"""

    def __init__(self, rows, rules=()):
        self.facts = {(s, p, o, bool(lit)): not inferred for s, p, o, lit, inferred in rows}
        self.by_p: dict = {}
        for f in self.facts:
            self.by_p.setdefault(f[1], []).append(f)
        self.rules = [(n, b if isinstance(b, list) else json.loads(b), h if isinstance(h, list) else json.loads(h))
                      for n, b, h in rules]
        self.chains = self._chains()

    def has(self, s, p, o, lit=False) -> bool:
        return (s, p, o, lit) in self.facts

    def objects(self, s, p):
        return [f[2] for f in self.by_p.get(p, ()) if f[0] == s and not f[3]]

    def _chains(self) -> list:
        out = []
        for p, _, head, _ in self.by_p.get(CHAIN, ()):
            props, node = [], head
            while node and len(props) < 4:
                firsts = self.objects(node, FIRST)
                if not firsts:
                    break
                props.append(firsts[0])
                rest = self.objects(node, REST)
                node = rest[0] if rest and rest[0] != RDF + "nil" else None
            if len(props) in (2, 3):
                out.append((p, props))
        return out


def closure(c, seeds=None) -> Closure:
    with c.transaction():
        rows = c.execute("SELECT s, p, o, lit, inferred FROM iq.entail(%s)" if seeds else
                         "SELECT s, p, o, lit, inferred FROM iq.entail()", (seeds,) if seeds else ()).fetchall()
    rules = c.execute("SELECT name, body, head FROM iq.rule ORDER BY name").fetchall()
    return Closure(rows, rules)


def _term(t: str, env: dict):
    """A rule's term under bindings: (value, lit) or None for an unbound variable."""
    if t.startswith("?"):
        return env.get(t)
    if t.startswith('"'):
        return (t[1:-1], True)
    return (t, False)


def _match(cl: Closure, atoms: list, env: dict):
    """Every binding of a rule body's atoms over the closure, extending `env`."""
    if not atoms:
        yield env
        return
    (s, p, o), rest = atoms[0], atoms[1:]
    ps = _term(p, env)
    for f in (cl.by_p.get(ps[0], ()) if ps else list(cl.facts)):
        e, ok = dict(env), True
        for t, v in ((s, (f[0], False)), (p, (f[1], False)), (o, (f[2], f[3]))):
            got = _term(t, e)
            if got is None:
                e[t] = v
            elif got != v:
                ok = False
                break
        if ok:
            yield from _match(cl, rest, e)


def _walk(cl: Closure, props: list, node, end, i: int, acc: list):
    """Each path from `node` along the chain's properties to `end` (o, lit): the facts it goes through."""
    for g in cl.by_p.get(props[i], ()):
        if g[0] != node:
            continue
        if i == len(props) - 1:
            if (g[2], g[3]) == end:
                yield acc + [g]
        elif not g[3]:
            yield from _walk(cl, props, g[2], end, i + 1, acc + [g])


def _steps(cl: Closure, f):
    """Each way the vocabulary or a rule makes `f` from facts of the closure: (rule, [premises])."""
    s, p, o, lit = f
    if p == A and not lit:
        for d in cl.objects(s, A):
            for rel in (SCO, EQC):
                if d != o and cl.has(d, rel, o):
                    yield rel.rsplit("#", 1)[1], [(s, A, d, False), (d, rel, o, False)]
            if d != o and cl.has(o, EQC, d):
                yield "equivalentClass", [(s, A, d, False), (o, EQC, d, False)]
        for prop, _, cls, _ in cl.by_p.get(DOM, ()):
            if cls == o:
                for g in cl.by_p.get(prop, ()):
                    if g[0] == s:
                        yield "domain", [g, (prop, DOM, o, False)]
        for prop, _, cls, _ in cl.by_p.get(RNG, ()):
            if cls == o:
                for g in cl.by_p.get(prop, ()):
                    if g[2] == s and not g[3]:
                        yield "range", [g, (prop, RNG, o, False)]
    if p in (SCO, SPO) and not lit:
        for m in cl.objects(s, p):
            if m not in (s, o) and cl.has(m, p, o):
                yield p.rsplit("#", 1)[1], [(s, p, m, False), (m, p, o, False)]
    for sub, _, sup, _ in cl.by_p.get(SPO, ()):
        if sup == p and sub != p and cl.has(s, sub, o, lit):
            yield "subPropertyOf", [(s, sub, o, lit), (sub, SPO, p, False)]
    if cl.has(p, A, TRA) and not lit:
        for m in cl.objects(s, p):
            if m not in (s, o) and cl.has(m, p, o):
                yield "TransitiveProperty", [(s, p, m, False), (m, p, o, False), (p, A, TRA, False)]
    if cl.has(p, A, SYM) and not lit and cl.has(o, p, s):
        yield "SymmetricProperty", [(o, p, s, False), (p, A, SYM, False)]
    if not lit:
        for q in cl.objects(p, INV):
            if cl.has(o, q, s):
                yield "inverseOf", [(o, q, s, False), (p, INV, q, False)]
        for q, _, inv, _ in cl.by_p.get(INV, ()):
            if inv == p and cl.has(o, q, s):
                yield "inverseOf", [(o, q, s, False), (q, INV, p, False)]
    for head, props in cl.chains:
        if head == p:
            for path in _walk(cl, props, s, (o, lit), 0, []):
                yield "propertyChainAxiom", path
    for name, body, heads in cl.rules:
        for h in heads:
            env: dict = {}
            ok = True
            for t, v in zip(h, ((s, False), (p, False), (o, lit))):
                got = _term(t, env)
                if got is None:
                    env[t] = v
                elif got != v:
                    ok = False
            if not ok:
                continue
            for e in _match(cl, body, env):
                yield name, [(_term(a[0], e)[0], _term(a[1], e)[0], *_term(a[2], e)) for a in body]


def proof(cl: Closure, s, p, o, lit=False, _path=frozenset()):
    """The derivation of (s, p, o) (see the module's doc), or None when the closure does not hold it."""
    f = (s, p, o, bool(lit))
    if f not in cl.facts:
        return None
    if cl.facts[f]:
        return {"fact": list(f), "by": "stated", "from": []}
    for by, premises in _steps(cl, f):
        if any(q == f or q in _path or q not in cl.facts for q in premises):
            continue
        subs = [proof(cl, *q, _path=_path | {f}) for q in premises]
        if all(x is not None and x["by"] != "unexplained" for x in subs):
            return {"fact": list(f), "by": by, "from": subs}
    return {"fact": list(f), "by": "unexplained", "from": []}


def stated(pf) -> set:
    """The stated facts a proof rests on."""
    if pf["by"] == "stated":
        return {tuple(pf["fact"])}
    return set().union(*(stated(x) for x in pf["from"])) if pf["from"] else set()


def modelled(c, term: str) -> bool:
    """Whether the vocabulary, a rule or any datum uses `term` (as a predicate, a subject or an object)."""
    if c.execute("SELECT 1 FROM iq.vocab WHERE s = %s OR p = %s OR o = %s LIMIT 1", (term, term, term)).fetchone():
        return True
    if c.execute("SELECT 1 FROM iq.rule WHERE body::text LIKE %s OR head::text LIKE %s LIMIT 1",
                 ('%' + json.dumps(term)[1:-1] + '%',) * 2).fetchone():
        return True
    return bool(c.execute("SELECT 1 FROM iq.quad WHERE p = %s OR (p = %s AND o = %s) LIMIT 1", (term, A, term)).fetchone())


def ask(c, p: str, o=None, lit=False) -> dict:
    """{answers: [subjects]} from the closure, or {not_modelled: term} (see the module's doc)."""
    if not modelled(c, p):
        return {"not_modelled": p}
    if p == A and o is not None and not modelled(c, o):
        return {"not_modelled": o}
    cl = closure(c)
    return {"answers": sorted({f[0] for f in cl.by_p.get(p, ()) if o is None or (f[2], f[3]) == (o, lit)})}
