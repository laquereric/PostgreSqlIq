"""Mutants of the reasoning: chains and rules in schema.sql's closure, and reasoning.py's proofs and "not modelled".
Each breaks one rule, runs features/reasoning.feature, and must make a scenario fail. The unmutated code runs first
and must pass; each source is restored at the end. Runs in place (IQ_MUTANTS_ISOLATED=1): the store is the one
IQ_CITUS_TOPOLOGY names, and the step files reach the monorepo from here.

    IQ_MUTANTS_ISOLATED=1 IQ_CITUS_TOPOLOGY=<topology.json> python3 reasoning_mutants.py [name ...]

@use_case Development/UseCase/UC_Citus
@facet Store
@test
@license LICENSE (IQ freeware terms 1.0.0; (c) CBI Business Transactions, LLC)
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SQL, PY = "schema.sql", "reasoning.py"
FEATURES = "features/reasoning.feature"

M = [
    # -- the closure
    ("a chain of two not applied", SQL, "WHERE ch.n = 2 AND NOT x.lit ON CONFLICT DO NOTHING", "WHERE false ON CONFLICT DO NOTHING"),
    ("a chain's links in the wrong order", SQL, "JOIN iq_closure x ON x.p = ch.p1\n            JOIN iq_closure y ON y.s = x.o AND y.p = ch.p2 WHERE ch.n = 2",
     "JOIN iq_closure x ON x.p = ch.p2\n            JOIN iq_closure y ON y.s = x.o AND y.p = ch.p1 WHERE ch.n = 2"),
    ("the rules held as data not applied", SQL, "        FOREACH stmt IN ARRAY rules LOOP\n            EXECUTE stmt;",
     "        FOREACH stmt IN ARRAY '{}'::text[] LOOP\n            EXECUTE stmt;"),
    ("a rule's literal not held to its value", SQL, "conds := conds || format('%s = %L AND b%s.lit', col, substr(t, 2, length(t) - 2), i);",
     "conds := conds || format('b%s.lit', i);"),
    ("a rule's shared variable not joined", SQL, "                    conds := conds || format('%s = %s', col, bound ->> t);",
     "                    conds := conds;"),
    ("an unsafe rule put", SQL, "            IF NOT t = ANY (vars) THEN\n                RAISE EXCEPTION 'unsafe_rule",
     "            IF false THEN\n                RAISE EXCEPTION 'unsafe_rule"),
    ("a disconnected rule put", SQL, "    IF cardinality(seen) < jsonb_array_length(body) THEN", "    IF false THEN"),
    ("a literal subject put", SQL, """(left(t, 1) = '"' AND (j < 2 OR length(t) < 2 OR right(t, 1) <> '"'))""",
     """(left(t, 1) = '"' AND (length(t) < 2 OR right(t, 1) <> '"'))"""),
    ("a chain of one put", SQL, "    IF cardinality(props) NOT IN (2, 3) THEN", "    IF cardinality(props) NOT IN (1, 2, 3) THEN"),

    # -- the proofs
    ("a stated fact not a leaf", PY, '        return {"fact": list(f), "by": "stated", "from": []}',
     '        pass'),
    ("a fact not in the closure proved", PY, "    if f not in cl.facts:\n        return None", "    if False:\n        return None"),
    ("a chain not explained", PY, "        if head == p:\n            for path in _walk", "        if False:\n            for path in _walk"),
    ("a rule not explained", PY, "    for name, body, heads in cl.rules:", "    for name, body, heads in []:"),
    ("the transitive property not explained", PY, "    if cl.has(p, A, TRA) and not lit:", "    if False:"),

    # -- not modelled
    ("an unknown word answered with an empty list", PY, '    if not modelled(c, p):\n        return {"not_modelled": p}',
     '    if False:\n        return {"not_modelled": p}'),
    ("an unknown class answered with an empty list", PY,
     '    if p == A and o is not None and not modelled(c, o):', "    if False:"),
]

if __name__ == "__main__":
    sys.path.insert(0, str(HERE.parents[2] / "Development" / "UseCase" / "UC_DxWorkFlow"))
    import mutants                      # the generic runner (UC_DxWorkFlow, slice 13)
    sys.exit(mutants.main_for(M, FEATURES, HERE, sys.argv[1:]))
