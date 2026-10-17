"""The Citus cluster as the scenarios reach it: a tunnel, two roles, the datum as one transaction, inference in SQL.

    topology()            what deploy-citus.sh put up (~/.iq/citus/topology.json, or IQ_CITUS_TOPOLOGY): the
                          coordinator's host, and each worker by the name Citus knows it as (`<nodename>:<port>`)
                          with its host and systemd unit -- one host by ports (slice 1) or one host each (slice 2)
    Cluster.open()        an SSH tunnel to the coordinator, the role iq's password read over ssh into memory (never
                          written here), the schema applied as postgres over ssh
    cluster.connect()     a psycopg connection as iq (not autocommit: the caller owns the transaction)
    cluster.admin(sql, node)   a node as postgres, over ssh: what only a superuser does (add a node, rebalance)
    cluster.host_sh(cmd, node) a root command on a node's host (stop a node the way a crash would)
    cluster.node_of(iri)  the worker holding the shard of `iri`

    put(c, iri, ...)      one datum -- document, text, vector, triples -- in the caller's transaction
    ask(c, question, inference)   a competency question of UC_DataPlatform/inference (?x in one triple pattern)

Every IRI is the full `https://iq.intentionalquality.com/ns#...`; answers come back shortened as compare.py's do.

@use_case Development/UseCase/UC_Citus
@facet Store
@status Proposed
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
INFERENCE = ROOT / "Development/UseCase/UC_DataPlatform/inference"
NS = "https://iq.intentionalquality.com/ns#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
OWL = "http://www.w3.org/2002/07/owl#"
PREFIXES = {"iq": NS, "rdf": RDF, "rdfs": RDFS, "owl": OWL, "prov": "http://www.w3.org/ns/prov#"}
TBOX = {RDFS + "subClassOf", RDFS + "subPropertyOf", RDFS + "domain", RDFS + "range", OWL + "inverseOf",
        OWL + "equivalentClass", OWL + "equivalentProperty"}
TEST = NS + "test:"
XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"
SSHO = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=accept-new"]
PG = "/usr/pgsql-17/bin"
COORD = "coordinator"


def topology() -> dict:
    p = Path(os.environ.get("IQ_CITUS_TOPOLOGY") or Path.home() / ".iq/citus/topology.json")
    if not p.exists():
        raise RuntimeError("no Citus cluster: UC_Citus/deploy-citus.sh up (or hosts-up)")
    return json.loads(p.read_text())


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class Cluster:
    topo: dict
    port: int = 0
    password: str = field(default="", repr=False)
    _sock: str = ""
    _others: dict = field(default_factory=dict, repr=False)

    @property
    def host(self) -> str:
        return self.topo["coordinator"]["host"]

    @property
    def mode(self) -> str:
        return self.topo["mode"]

    @property
    def spare(self) -> str:
        return self.topo["spare"]

    def node(self, key: str) -> dict:
        return self.topo["coordinator"] if key == COORD else self.topo["workers"][key]

    def name(self, key: str) -> str:
        return "coordinator" if key == COORD else self.topo["workers"][key]["name"]

    @classmethod
    def open(cls) -> "Cluster":
        c = cls(topology())
        c.port = _free_port()
        c._sock = tempfile.mktemp(prefix="iq-citus-", dir="/tmp")
        subprocess.run(["ssh", *SSHO, "-f", "-N", "-M", "-S", c._sock, "-o", "ExitOnForwardFailure=yes",
                        "-L", "127.0.0.1:%d:127.0.0.1:5432" % c.port, "root@" + c.host], check=True)
        c.password = c.host_sh("cut -d: -f2- /root/citus-iq.txt").strip()
        c.admin((HERE / "schema.sql").read_text())
        c.admin((HERE / "shacl.sql").read_text())
        return c

    def close(self) -> None:
        if self._sock:
            subprocess.run(["ssh", "-S", self._sock, "-O", "exit", "root@" + self.host], capture_output=True)
            self._sock = ""

    def connect(self, autocommit: bool = False, role: str = "iq"):
        """As iq (the owner the scenarios set up and check with), or as another login role -- iq_gate, the gate's
        writer -- whose password is read once from the coordinator into memory."""
        import psycopg
        pw = self.password if role == "iq" else self._password(role)
        return psycopg.connect(host="127.0.0.1", port=self.port, dbname="iq", user=role, password=pw,
                               autocommit=autocommit, connect_timeout=10)

    def _password(self, role: str) -> str:
        if role not in self._others:
            self._others[role] = self.host_sh("cut -d: -f2- /root/citus-%s.txt" % role.replace("iq_", "")).strip()
        return self._others[role]

    def host_sh(self, cmd: str, node: str = COORD, check: bool = True, timeout: int = 600) -> str:
        r = subprocess.run(["ssh", *SSHO, "root@" + self.node(node)["host"], cmd], capture_output=True, text=True,
                           timeout=timeout)
        if check and r.returncode:
            raise RuntimeError("host %s: %s -> %s" % (self.name(node), cmd[:120], r.stderr.strip()[-500:]))
        return r.stdout

    def admin(self, sql: str, node: str = COORD) -> str:
        """psql as postgres on a node, the statement on stdin (one value per line, unaligned)."""
        n = self.node(node)
        r = subprocess.run(["ssh", *SSHO, "root@" + n["host"],
                            "sudo -u postgres %s/psql -X -q -At -v ON_ERROR_STOP=1 -p %d -d iq" % (PG, n["port"])],
                           input=sql, capture_output=True, text=True, timeout=900)
        if r.returncode:
            raise RuntimeError("admin %s: %s -> %s" % (self.name(node), sql.strip()[:120], r.stderr.strip()[-800:]))
        return r.stdout.strip()

    def node_of(self, iri: str) -> str:
        """The worker (`nodename:port`) holding the shard of `iri`."""
        with self.connect(autocommit=True) as c:
            n, p = c.execute("SELECT nodename, nodeport FROM pg_dist_placement pl JOIN pg_dist_node n USING (groupid) "
                             "WHERE pl.shardid = get_shard_id_for_distribution_column('iq.datum', %s::text)",
                             (iri,)).fetchone()
        return "%s:%d" % (n, p)

    def restart_sh(self, node: str) -> str:
        """Shell: stop a node as a crash does (immediate: no checkpoint, no goodbye to clients), start it, wait."""
        n = self.node(node)
        return ("sudo -u postgres {pg}/pg_ctl -D /var/lib/pgsql/17/{u} stop -m immediate >/dev/null; "
                "systemctl reset-failed iq-citus@{u} 2>/dev/null; systemctl start iq-citus@{u}; "
                "for _ in $(seq 60); do sudo -u postgres {pg}/pg_isready -q -p {p} && exit 0; sleep 0.5; done; exit 1"
                ).format(pg=PG, u=n["unit"], p=n["port"])

    def crash(self, node: str) -> None:
        self.host_sh(self.restart_sh(node), node)

    def crash_once_prepared(self, node: str, watched: str, timeout: int = 30) -> subprocess.Popen:
        """Start, on `node`'s host, a watcher that crashes `node` the moment `watched` holds a prepared transaction
        (a two-phase commit between its PREPARE and its COMMIT PREPARED). Returns the running watcher."""
        w = self.node(watched)
        nodename = watched.rsplit(":", 1)[0]          # localhost (one host) or its private address (the VPC)
        poll = ("sudo -u postgres {pg}/psql -X -At -h {h} -p {p} -d iq -c 'SELECT count(*) FROM pg_prepared_xacts'"
                ).format(pg=PG, h=nodename, p=w["port"])
        script = ("end=$((SECONDS+%d)); until [ \"$(%s)\" -gt 0 ]; do [ $SECONDS -lt $end ] || exit 3; sleep 0.02; done; %s"
                  % (timeout, poll, self.restart_sh(node)))
        return subprocess.Popen(["ssh", *SSHO, "root@" + self.node(node)["host"], script],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def prepared(self, node: str) -> int:
        return int(self.admin("SELECT count(*) FROM pg_prepared_xacts", node))


# --- the platform, as datums -------------------------------------------------------------------------------------

def expand(term: str) -> str:
    if term == "a":
        return RDF + "type"
    if term.startswith("<") and term.endswith(">"):
        return term[1:-1]
    pfx, _, local = term.partition(":")
    return PREFIXES[pfx] + local


def short(v: str) -> str:
    return v[len(NS):] if v.startswith(NS) else v


@dataclass
class Platform:
    vocab: list          # (s, p, o) for iq.vocab
    datums: dict         # subject iri -> [(s, p, o, lit, datatype)]


def platform() -> Platform:
    """platform.ttl split as a store would hold it: the T-Box as the vocabulary, every other subject a datum."""
    import pyoxigraph as ox
    data = (INFERENCE / "platform.ttl").read_text(encoding="utf-8")
    vocab, datums = [], {}
    for t in ox.parse(data, format=ox.RdfFormat.TURTLE):
        s, p = t.subject.value, t.predicate.value
        lit = isinstance(t.object, ox.Literal)
        o = t.object.value
        if p in TBOX or (p == RDF + "type" and o.startswith(OWL)):
            vocab.append((s, p, o))
        else:
            datums.setdefault(s, []).append((s, p, o, lit, t.object.datatype.value if lit else None))
    return Platform(vocab, datums)


def embedding(iri: str) -> list:
    h = hashlib.sha256(iri.encode()).digest()
    return [round(b / 255.0, 4) for b in h[:3]]


def put(c, iri: str, quads: list, tier: str = "Silver", body: dict | None = None, emb: list | None = None) -> None:
    """One datum in the caller's transaction, through the governed write (iq.put_datum): its row (document, text,
    vector) and its triples, replaced whole, and held to the shapes at COMMIT. A triple is (s, p, o, lit[, dt])."""
    body = body if body is not None else {"iri": iri, "label": short(iri),
                                          "text": " ".join(short(q[2]) for q in quads if not q[3])}
    rows = [{"s": q[0], "p": q[1], "o": q[2], "lit": q[3],
             "dt": (q[4] if len(q) > 4 else None) or (XSD_STRING if q[3] else None)} for q in quads]
    c.execute("SELECT iq.put_datum(%s, %s, %s, %s::vector, %s)",
              (iri, tier, json.dumps(body), str(emb or embedding(iri)), json.dumps(rows)))


def load_platform(cluster: Cluster) -> Platform:
    pf = platform()
    with cluster.connect() as c:
        c.execute("DELETE FROM iq.vocab")
        with c.cursor() as cur:
            cur.executemany("INSERT INTO iq.vocab (s, p, o) VALUES (%s, %s, %s)", pf.vocab)
        c.execute("DELETE FROM iq.datum WHERE iri NOT LIKE %s", (TEST + "%",))
        for iri, quads in pf.datums.items():
            put(c, iri, quads)
        c.commit()
    return pf


def two_workers(cluster: Cluster) -> None:
    """The cluster as a run starts it: the spare drained and out, if an earlier run left it in."""
    if in_cluster(cluster, cluster.spare):
        remove_worker(cluster, cluster.spare)


def in_cluster(cluster: Cluster, key: str) -> bool:
    n, p = key.rsplit(":", 1)
    return cluster.admin("SELECT count(*) FROM pg_dist_node WHERE nodename = '%s' AND nodeport = %d" % (n, int(p))) == "1"


# Citus defers dropping a moved shard's old copy (pg_dist_cleanup). citus_remove_node does not purge the records of
# the group it removes, and a re-added node gets a new group, so they can never be done -- and the next move of
# those shards fails for ever ("the orphaned shard ... could not be cleaned up"). So: drain, clean up until nothing
# is left on the node, and only then remove it.
REMOVE = """
SELECT citus_drain_node('{name}', {port}, shard_transfer_mode => 'force_logical');
DO $$
DECLARE g int := (SELECT groupid FROM pg_dist_node WHERE nodename = '{name}' AND nodeport = {port});
BEGIN
    FOR i IN 1..60 LOOP
        CALL citus_cleanup_orphaned_resources();
        EXIT WHEN NOT EXISTS (SELECT 1 FROM pg_dist_cleanup WHERE node_group_id = g);
        PERFORM pg_sleep(1);
    END LOOP;
    IF EXISTS (SELECT 1 FROM pg_dist_cleanup WHERE node_group_id = g) THEN
        RAISE EXCEPTION 'orphaned shards left on {name}:{port}; not removing it';
    END IF;
END $$;
SELECT citus_remove_node('{name}', {port});
"""


def remove_worker(cluster: Cluster, key: str) -> None:
    n, p = key.rsplit(":", 1)
    cluster.admin(REMOVE.format(name=n, port=int(p)))


def add_worker_sql(key: str) -> str:
    n, p = key.rsplit(":", 1)
    return "SELECT citus_add_node('%s', %d);" % (n, int(p))


def clean_tests(cluster: Cluster) -> None:
    with cluster.connect() as c:
        for t in ("shape_target", "shape_property", "shape_closed"):
            c.execute("DELETE FROM iq.%s WHERE graph LIKE %%s" % t, (TEST + "%",))
        c.execute("DELETE FROM iq.datum WHERE iri LIKE %s", (TEST + "%",))
        c.execute("DELETE FROM iq.vocab WHERE s LIKE %s", (TEST + "%",))
        c.commit()


# --- the competency questions ------------------------------------------------------------------------------------

_PATTERN = re.compile(r"\{\s*(\S+)\s+(\S+)\s+(\S+)\s*\}")


def questions() -> list:
    """compare.py's eight questions: (id, text, rule, (s, p, o) with None for ?x, the expected answers)."""
    src = (INFERENCE / "compare.py").read_text(encoding="utf-8")
    block = src[src.index("STORES5 = "):src.index("]\n\n", src.index("QUESTIONS = [")) + 1]
    ns = {}
    exec(block, ns)                                    # the list literal only: compare.py's imports need a plane
    out = []
    for qid, text, rule, sparql, expected in ns["QUESTIONS"]:
        s, p, o = (None if t == "?x" else expand(t) for t in _PATTERN.search(sparql).groups())
        out.append((qid, text, rule, (s, p, o), set(expected)))
    return out


def ask(c, pattern: tuple, inference: bool = True) -> set:
    s, p, o = pattern
    src = "iq.entail()" if inference else \
        "(SELECT s, p, o, lit FROM iq.quad UNION SELECT s, p, o, false FROM iq.vocab)"
    col = "s" if s is None else "o"
    where, args = [], []
    for name, v in (("s", s), ("p", p), ("o", o)):
        if v is not None:
            where.append("t.%s = %%s" % name)
            args.append(v)
    rows = c.execute("SELECT DISTINCT t.%s FROM %s t WHERE %s" % (col, src, " AND ".join(where)), args).fetchall()
    return {short(r[0]) for r in rows}


def timed(fn, *a, **k):
    t0 = time.perf_counter()
    r = fn(*a, **k)
    return r, (time.perf_counter() - t0) * 1000


# --- scale: the platform copied K times, each copy its own component -----------------------------------------------

def _scaled(iri: str, i: int) -> str:
    """An instance IRI of copy i (engines and datums); classes and properties stay shared."""
    for kind in ("engine:", "datum:"):
        if iri.startswith(NS + kind):
            return "%stest:scale-c%d-%s" % (NS, i, iri[len(NS):])
    return iri


def load_copies(cluster: Cluster, k: int, start: int = 0) -> int:
    """Copies start..k-1 of the platform's datums, bulk-loaded (COPY) beside the original; the vocabulary is shared.
    Returns the quads written."""
    pf, n = platform(), 0
    with cluster.connect() as c:
        with c.cursor() as cur:
            with cur.copy("COPY iq.datum (iri, tier, body) FROM STDIN") as cp_:
                for i in range(start, k):
                    for iri in pf.datums:
                        cp_.write_row((_scaled(iri, i), "Silver", json.dumps({"iri": _scaled(iri, i)})))
            with cur.copy("COPY iq.quad (g, s, p, o, lit, dt) FROM STDIN") as cp_:
                for i in range(start, k):
                    for iri, quads in pf.datums.items():
                        for s, p, o, lit, dt in quads:
                            cp_.write_row((_scaled(iri, i), _scaled(s, i), p, o if lit else _scaled(o, i), lit, dt))
                            n += 1
        c.commit()
    return n
