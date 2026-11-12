"""This box's Postgres as a store the scenarios can run on: a database of its own, pgvector in it, and the topology of a
server reached directly (citus_plane.Cluster's `dsn`), written for IQ_CITUS_TOPOLOGY. The scenarios then apply the
schema themselves, as on any store.

    python3 local_store.py <topology.json> [db]          then   IQ_CITUS_TOPOLOGY=<topology.json> behave features/...

The box's Postgres is the pgembed under ~/.iq/pg (IQ_PGDATA), reached by its unix socket as postgres; the role iq
the schema is owned by is made if the box has none.

@use_case Development/UseCase/UC_Citus
@facet Store
@controller
@license LICENSE (IQ freeware terms 1.0.0; (c) CBI Business Transactions, LLC)
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

SOCK = Path(os.environ.get("IQ_PGDATA") or Path.home() / ".iq" / "pg")


def dsn(db: str, sock: Path = SOCK) -> str:
    return "postgresql://postgres@/%s?host=%s" % (db, sock)


def prepare(topology: Path, db: str = "iq_reasoning", sock: Path = SOCK) -> dict:
    import psycopg
    with psycopg.connect(dsn("postgres", sock), autocommit=True) as c:
        if not c.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,)).fetchone():
            c.execute('CREATE DATABASE "%s"' % db)
        c.execute("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'iq') THEN "
                  "CREATE ROLE iq LOGIN; END IF; END $$")
    with psycopg.connect(dsn(db, sock), autocommit=True) as c:
        c.execute("CREATE EXTENSION IF NOT EXISTS vector")
    topo = {"mode": "server", "dsn": dsn(db, sock)}
    Path(topology).write_text(json.dumps(topo, indent=1) + "\n")
    return topo


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__.split("\n\n")[1], file=sys.stderr)
        sys.exit(2)
    print(json.dumps(prepare(Path(sys.argv[1]), *sys.argv[2:3])))
