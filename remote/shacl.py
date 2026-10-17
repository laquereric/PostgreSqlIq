"""SHACL shapes compiled to rows for shacl.sql, and an independent check of what the cluster decides.

    compile_shapes(ttl, graph)   the NodeShapes of a shapes graph as (targets, properties, closed) rows; any SHACL
                                 term outside the supported Core subset is refused (Unsupported names each one)
    load(conn, compiled)         replace that shapes graph in the cluster, in the caller's transaction
    oracle(shapes, vocab, data)  UC_OntologyPipeline's own SHACL core (graph.validate, over Oxigraph): the same
                                 shapes and data, judged by code that shares nothing with the SQL
    report(conn, iris)           the cluster's report for those datums (iq.shacl_check), in the oracle's terms

The shapes are IQ's own, derived from LinkML (GherkinIq/Facet/*/derived/*.shacl.ttl).

@use_case Development/UseCase/UC_Citus
@facet Store
@status Proposed
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
SH = "http://www.w3.org/ns/shacl#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
XSD = "http://www.w3.org/2001/XMLSchema#"
SHAPE_TERMS = {"targetClass", "property", "closed", "ignoredProperties", "name", "description", "order", "message"}
PROPERTY_TERMS = {"path", "minCount", "maxCount", "datatype", "nodeKind", "class", "in", "pattern", "minInclusive",
                  "maxInclusive", "order", "name", "description", "message"}
FACET_SHAPES = sorted((ROOT / "GherkinIq/Facet").glob("*/derived/*.shacl.ttl"))


class Unsupported(ValueError):
    def __init__(self, found):
        self.found = sorted(set(found))
        super().__init__("SHACL not supported here: " + ", ".join(self.found))


@dataclass
class Compiled:
    graph: str
    targets: list = field(default_factory=list)      # (shape, class)
    properties: list = field(default_factory=list)   # dict per property shape
    closed: list = field(default_factory=list)       # (shape, [ignored])


def term(node) -> str:
    """A term as iq.term() writes it: I:<iri> or L:<datatype>|<lexical>."""
    import pyoxigraph as ox
    if isinstance(node, ox.Literal):
        return "L:%s|%s" % (node.datatype.value, node.value)
    return "I:" + (node.value if isinstance(node, ox.NamedNode) else "_:" + node.value)


def compile_shapes(ttl: str, graph: str) -> Compiled:
    import pyoxigraph as ox
    st = ox.Store()
    st.load(ttl.encode("utf-8"), ox.RdfFormat.TURTLE)
    N = ox.NamedNode

    def objs(s, p):
        return [q.object for q in st.quads_for_pattern(s, N(p), None, None)]

    def rdf_list(head):
        out = []
        while head != N(RDF + "nil"):
            out += objs(head, RDF + "first")
            rest = objs(head, RDF + "rest")
            head = rest[0] if rest else N(RDF + "nil")
        return out

    out, bad = Compiled(graph), []
    shapes = sorted({q.subject for q in st.quads_for_pattern(None, N(RDF + "type"), N(SH + "NodeShape"), None)},
                    key=lambda n: n.value)
    for shape in shapes:
        for q in st.quads_for_pattern(shape, None, None, None):
            p = q.predicate.value
            if p.startswith(SH) and p[len(SH):] not in SHAPE_TERMS:
                bad.append("%s on %s" % ("sh:" + p[len(SH):], shape.value))
        for t in objs(shape, SH + "targetClass"):
            out.targets.append((shape.value, t.value))
        if any(o.value == "true" for o in objs(shape, SH + "closed")):
            ignored = [o.value for h in objs(shape, SH + "ignoredProperties") for o in rdf_list(h)]
            out.closed.append((shape.value, ignored))
        for i, ps in enumerate(objs(shape, SH + "property")):
            for q in st.quads_for_pattern(ps, None, None, None):
                p = q.predicate.value
                if p.startswith(SH) and p[len(SH):] not in PROPERTY_TERMS:
                    bad.append("%s on %s" % ("sh:" + p[len(SH):], shape.value))
            path = objs(ps, SH + "path")
            if not path or not isinstance(path[0], ox.NamedNode):
                bad.append("sh:path (a path expression) on %s" % shape.value)
                continue
            one = lambda c: (objs(ps, SH + c) or [None])[0]
            order = one("order")
            row = {"shape": shape.value, "ord": int(order.value) if order is not None else 1000 + i,
                   "path": path[0].value}
            for c, conv in (("minCount", int), ("maxCount", int), ("datatype", None), ("class", None),
                            ("pattern", None), ("minInclusive", float), ("maxInclusive", float)):
                v = one(c)
                row[c] = None if v is None else (conv(v.value) if conv else v.value)
            nk = one("nodeKind")
            row["nodeKind"] = nk.value[len(SH):] if nk is not None else None
            lst = one("in")
            row["in"] = [term(x) for x in rdf_list(lst)] if lst is not None else None
            out.properties.append(row)
    if bad:
        raise Unsupported(bad)
    return out


def load(c, comp: Compiled) -> None:
    for t in ("shape_target", "shape_property", "shape_closed"):
        c.execute("DELETE FROM iq.%s WHERE graph = %%s" % t, (comp.graph,))
    with c.cursor() as cur:
        cur.executemany("INSERT INTO iq.shape_target (graph, shape, target) VALUES (%s, %s, %s)",
                        [(comp.graph, s, t) for s, t in comp.targets])
        cur.executemany("INSERT INTO iq.shape_closed (graph, shape, ignored) VALUES (%s, %s, %s)",
                        [(comp.graph, s, ig) for s, ig in comp.closed])
        cur.executemany(
            "INSERT INTO iq.shape_property (graph, shape, ord, path, min_count, max_count, datatype, node_kind, class, "
            "in_values, pattern, min_incl, max_incl) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            [(comp.graph, r["shape"], r["ord"], r["path"], r["minCount"], r["maxCount"], r["datatype"], r["nodeKind"],
              r["class"], r["in"], r["pattern"], r["minInclusive"], r["maxInclusive"]) for r in comp.properties])


def report(c, iris) -> set:
    out = set()
    for iri in iris:
        for row in c.execute("SELECT focus, shape, path, constraint_name, detail FROM iq.shacl_check(%s)", (iri,)):
            out.add(tuple(row))
    return out


def oracle(shapes_ttl: str, vocab_ttl: str, data_ttl: str) -> set:
    """graph.validate over the same shapes, vocabulary and data: (focus, shape, path, constraint, detail)."""
    sys.path.insert(0, str(ROOT / "Development/UseCase/UC_OntologyPipeline"))
    import graph as og
    g = og.Graph("shacl-oracle")
    for layer, text in (("vocab", vocab_ttl), ("shapes", shapes_ttl), ("data", data_ttl)):
        if text.strip():
            g._load(g.graph_iri(layer), text, "turtle")
            g.journal.append({"layer": layer, "artefact": "oracle:" + layer})
    return {(v.focus, v.shape, v.path, v.constraint, v.detail) for v in og.validate(g, "data", shapes=("shapes",))}


# --- generated cases: every shape's instances, conforming or broken one way -----------------------------------------

LEX = {XSD + "string": "s", XSD + "float": "1.5", XSD + "double": "2.5", XSD + "integer": "7", XSD + "boolean": "true",
       XSD + "dateTime": "2026-10-17T00:00:00Z", XSD + "date": "2026-10-17"}
MUTATIONS = ("none", "drop", "twice", "datatype", "in", "nodeKind", "closed", "class")


def generate(comp: Compiled, ns: str, n: int, seed: int = 7) -> list:
    """n datums (iri, [(s, p, o, lit, dt)]) over the shapes' target classes: each conforms, or breaks one constraint
    its shape has (a required value dropped, a single value given twice, the wrong datatype, a value off its list, an
    IRI where a literal goes, a predicate the closed shape does not name, a value of the wrong class). Values a
    sh:class asks for are the generated instances of that class."""
    import random
    rnd = random.Random(seed)
    by_shape = {}
    for r in comp.properties:
        by_shape.setdefault(r["shape"], []).append(r)
    targets = [(s, t) for s, t in comp.targets]
    insts = {t: [] for _, t in targets}
    plan = [(i, *targets[i % len(targets)]) for i in range(n)]
    for i, _, t in plan:
        insts[t].append("%sgen-%d" % (ns, i))
    out = []
    for i, shape, target in plan:
        iri = "%sgen-%d" % (ns, i)
        props = by_shape.get(shape, [])
        quads = [(iri, RDF + "type", target, False, None)]
        for r in props:
            quads += [(iri, r["path"], *v) for v in _values(r, rnd, insts)]
        m = rnd.choice(MUTATIONS)
        quads = _mutate(m, iri, props, quads, rnd, insts, any(s == shape for s, _ in comp.closed), ns)
        out.append((iri, quads))
    return out


def _value(r, rnd, insts):
    if r["in"]:
        t = rnd.choice(r["in"])
        if t.startswith("I:"):
            return (t[2:], False, None)
        dt, lex = t[2:].split("|", 1)
        return (lex, True, dt)
    if r["class"]:
        pool = insts.get(r["class"]) or []
        return (rnd.choice(pool), False, None) if pool else None
    if r["datatype"]:
        return (LEX.get(r["datatype"], "x"), True, r["datatype"])
    if r["nodeKind"] in ("IRI", "BlankNodeOrIRI"):
        return ("urn:iq:value:%d" % rnd.randrange(1000), False, None)
    return ("x", True, XSD + "string")


def _values(r, rnd, insts):
    if not r["minCount"] and rnd.random() < 0.3:
        return []
    v = _value(r, rnd, insts)
    return [v] if v else []


def _mutate(m, iri, props, quads, rnd, insts, closed, ns):
    pick = lambda cond: [r for r in props if cond(r)]
    if m == "drop" and pick(lambda r: r["minCount"]):
        r = rnd.choice(pick(lambda r: r["minCount"]))
        return [q for q in quads if q[1] != r["path"]]
    if m == "twice" and pick(lambda r: r["maxCount"] == 1 and r["datatype"]):
        r = rnd.choice(pick(lambda r: r["maxCount"] == 1 and r["datatype"]))
        return [q for q in quads if q[1] != r["path"]] + [(iri, r["path"], "a", True, r["datatype"]),
                                                          (iri, r["path"], "b", True, r["datatype"])]
    if m == "datatype" and pick(lambda r: r["datatype"] and r["datatype"] != XSD + "string"):
        r = rnd.choice(pick(lambda r: r["datatype"] and r["datatype"] != XSD + "string"))
        return [q for q in quads if q[1] != r["path"]] + [(iri, r["path"], "not-a-number", True, XSD + "string")]
    if m == "in" and pick(lambda r: r["in"]):
        r = rnd.choice(pick(lambda r: r["in"]))
        return [q for q in quads if q[1] != r["path"]] + [(iri, r["path"], "OFF_THE_LIST", True, XSD + "string")]
    if m == "nodeKind" and pick(lambda r: r["nodeKind"] == "Literal"):
        r = rnd.choice(pick(lambda r: r["nodeKind"] == "Literal"))
        return [q for q in quads if q[1] != r["path"]] + [(iri, r["path"], "urn:iq:not-a-literal", False, None)]
    if m == "closed" and closed:
        return quads + [(iri, ns + "unnamed", "x", True, XSD + "string")]
    if m == "class" and pick(lambda r: r["class"]):
        r = rnd.choice(pick(lambda r: r["class"]))
        return [q for q in quads if q[1] != r["path"]] + [(iri, r["path"], "urn:iq:untyped", False, None)]
    return quads


def ntriples(quads: list) -> str:
    def t(o, lit, dt):
        if not lit:
            return "<%s>" % o
        return '"%s"^^<%s>' % (o.replace("\\", "\\\\").replace('"', '\\"'), dt or XSD + "string")
    return "".join("<%s> <%s> %s .\n" % (s, p, t(o, lit, dt)) for s, p, o, lit, dt in quads)
