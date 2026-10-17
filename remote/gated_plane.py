"""The gate as the cluster's only writer (UC_Citus slice 5).

    agent --> GatedWriter.put(datum) --gate push (cpcp-gate /push)--> Decision
                    | admitted                                 | refused: nothing is written
                    v
              the data plane (sparql_citus adapters), connected as iq_gate -- the only application role with
              iq.put_datum -- the datum stamped with the gate's operation, held to the shapes at COMMIT

An agent's own role (iq_agent) reads and writes nothing. Every act -- put, get, query -- is asked of the gate under a
fresh operationId; `audit()` holds the cluster to the gate's journal: every datum the writer wrote carries an
operation the gate admitted, and every admitted put is on the cluster.

@use_case Development/UseCase/UC_Citus
@facet Agent
@status Proposed
"""
from __future__ import annotations

import sys
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
for p in (ROOT / "GherkinIq/Facet/Store", ROOT / "Development/UseCase/UC_MarkLogic/surface"):
    sys.path.insert(0, str(p))

from dataplane import BlobAdapter, Datum, LocalObjects, Plane  # noqa: E402
from ml_mcp import Gate  # noqa: E402
from sparql_citus import CitusGraphAdapter, CitusRecordAdapter, CitusStore  # noqa: E402


class GatedWriter:
    def __init__(self, cluster, gate: Gate, blobs: Path):
        self.cluster, self.gate = cluster, gate
        self.store = CitusStore(lambda: cluster.connect(role="iq_gate"))
        self.plane = Plane([CitusRecordAdapter(self.store), CitusGraphAdapter(self.store), BlobAdapter(LocalObjects(blobs))],
                           admit=self._admit)
        self.op = ""
        self.decisions: list = []

    def _admit(self, action: str, iri: str):
        d = self.gate.push(action, {"iri": iri}, self.op)
        self.decisions.append((self.op, action, iri, d))
        return None if d.get("admitted") else (d.get("reason") or "no_permit")

    def _next(self) -> str:
        self.op = "iq:op:" + uuid.uuid4().hex
        self.store.operation = self.op
        return self.op

    def put(self, d: Datum):
        return self.plane.put(d, self._next())

    def get(self, iri: str):
        return self.plane.get(iri, self._next())

    def sparql(self, query: str):
        return self.plane.sparql(query, self._next())

    def audit(self, journal: list) -> dict:
        """The cluster against the gate's journal (its Decisions): what the writer wrote, and under which operation."""
        admitted = {d.get("operationId") or d.get("operation_id") or "": d for d in journal if d.get("admitted")}
        with self.cluster.connect(autocommit=True) as c:
            rows = c.execute("SELECT iri, operation FROM iq.datum WHERE media_type IS NOT NULL").fetchall()
        puts = {op for op, action, _, d in self.decisions if action.startswith("dataplane:put:") and d.get("admitted")}
        return {"rows": len(rows),
                "without_operation": [i for i, op in rows if not op],
                "not_admitted": [i for i, op in rows if op and op not in admitted],
                "admitted_puts_missing": sorted(puts - {op for _, op in rows})}
