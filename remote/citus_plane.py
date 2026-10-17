"""The Citus cluster as the scenarios reach it: a tunnel, two roles, the datum as one transaction, inference in SQL.

    Cluster.open()        an SSH tunnel to the coordinator (127.0.0.1:<port> -> host:5432), the role iq's password
                          read over ssh into memory (never written here), the schema applied
    cluster.connect()     a psycopg connection as iq (not autocommit: the caller owns the transaction)
    cluster.admin(sql)    the coordinator as postgres, over ssh: what only a superuser does (add a node, rebalance)
    cluster.host_sh(cmd)  a root command on the host (stop a node the way a crash would)

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
import shlex
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
SSHO = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=accept-new"]


def host() -> str:
    h = os.environ.get("IQ_CITUS_HOST") or (Path.home() / ".iq/citus/host").read_text().strip()
    if not h:
        raise RuntimeError("no Citus host: UC_Citus/deploy-citus.sh up")
    return h


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class Cluster:
    host: str
    port: int = 0
    password: str = field(default="", repr=False)
    _sock: str = ""

    @classmethod
    def open(cls) -> "Cluster":
        c = cls(host())
        c.port = _free_port()
        c._sock = tempfile.mktemp(prefix="iq-citus-", dir="/tmp")
        subprocess.run(["ssh", *SSHO, "-f", "-N", "-M", "-S", c._sock, "-o", "ExitOnForwardFailure=yes",
                        "-L", "127.0.0.1:%d:127.0.0.1:5432" % c.port, "root@" + c.host], check=True)
        c.password = c.host_sh("cut -d: -f2- /root/citus-iq.txt").strip()
        c.admin((HERE / "schema.sql").read_text())
        return c

    def close(self) -> None:
        if self._sock:
            subprocess.run(["ssh", "-S", self._sock, "-O", "exit", "root@" + self.host], capture_output=True)
            self._sock = ""

    def connect(self, autocommit: bool = False):
        import psycopg
        return psycopg.connect(host="127.0.0.1", port=self.port, dbname="iq", user="iq", password=self.password,
                               autocommit=autocommit, connect_timeout=10)

    def host_sh(self, cmd: str, check: bool = True) -> str:
        r = subprocess.run(["ssh", *SSHO, "root@" + self.host, cmd], capture_output=True, text=True, timeout=600)
        if check and r.returncode:
            raise RuntimeError("host: %s -> %s" % (cmd, r.stderr.strip()[-500:]))
        return r.stdout

    def admin(self, sql: str, port: int = 5432) -> str:
        """psql as postgres on a node, the statement on stdin (one value per line, unaligned)."""
        r = subprocess.run(["ssh", *SSHO, "root@" + self.host,
                            "sudo -u postgres /usr/pgsql-17/bin/psql -X -q -At -v ON_ERROR_STOP=1 -p %d -d iq" % port],
                           input=sql, capture_output=True, text=True, timeout=900)
        if r.returncode:
            raise RuntimeError("admin: %s -> %s" % (sql.strip()[:120], r.stderr.strip()[-800:]))
        return r.stdout.strip()

    def node_of(self, iri: str) -> int:
        """The worker port holding the shard of `iri`."""
        with self.connect(autocommit=True) as c:
            return c.execute("SELECT nodeport FROM pg_dist_placement pl JOIN pg_dist_node n USING (groupid) "
                             "WHERE pl.shardid = get_shard_id_for_distribution_column('iq.datum', %s::text)",
                             (iri,)).fetchone()[0]


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
    datums: dict         # subject iri -> [(s, p, o, lit)]


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
            datums.setdefault(s, []).append((s, p, o, lit))
    return Platform(vocab, datums)


def embedding(iri: str) -> list:
    h = hashlib.sha256(iri.encode()).digest()
    return [round(b / 255.0, 4) for b in h[:3]]


def put(c, iri: str, quads: list, tier: str = "Silver", body: dict | None = None, emb: list | None = None) -> None:
    """One datum in the caller's transaction: its row (document, text, vector) and its triples, replaced whole."""
    body = body if body is not None else {"iri": iri, "label": short(iri),
                                          "text": " ".join(short(q[2]) for q in quads if not q[3])}
    c.execute("DELETE FROM iq.datum WHERE iri = %s", (iri,))
    c.execute("INSERT INTO iq.datum (iri, tier, body, embedding) VALUES (%s, %s, %s, %s::vector)",
              (iri, tier, json.dumps(body), str(emb or embedding(iri))))
    with c.cursor() as cur:
        cur.executemany("INSERT INTO iq.quad (g, s, p, o, lit) VALUES (%s, %s, %s, %s, %s)",
                        [(iri, *q) for q in quads])


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
    """The cluster as a run starts it: w3 (:5435) drained and out, if an earlier run left it in."""
    if cluster.admin("SELECT count(*) FROM pg_dist_node WHERE nodeport = 5435") == "1":
        remove_worker(cluster, 5435)


# Citus defers dropping a moved shard's old copy (pg_dist_cleanup). citus_remove_node does not purge the records of
# the group it removes, and a re-added node gets a new group, so they can never be done -- and the next move of
# those shards fails for ever ("the orphaned shard ... could not be cleaned up"). So: drain, clean up until nothing
# is left on the node, and only then remove it.
REMOVE = """
SELECT citus_drain_node('localhost', {port}, shard_transfer_mode => 'force_logical');
DO $$
DECLARE g int := (SELECT groupid FROM pg_dist_node WHERE nodeport = {port});
BEGIN
    FOR i IN 1..60 LOOP
        CALL citus_cleanup_orphaned_resources();
        EXIT WHEN NOT EXISTS (SELECT 1 FROM pg_dist_cleanup WHERE node_group_id = g);
        PERFORM pg_sleep(1);
    END LOOP;
    IF EXISTS (SELECT 1 FROM pg_dist_cleanup WHERE node_group_id = g) THEN
        RAISE EXCEPTION 'orphaned shards left on :{port}; not removing it';
    END IF;
END $$;
SELECT citus_remove_node('localhost', {port});
"""


def remove_worker(cluster: Cluster, port: int) -> None:
    cluster.admin(REMOVE.format(port=port))


def clean_tests(cluster: Cluster) -> None:
    with cluster.connect() as c:
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
