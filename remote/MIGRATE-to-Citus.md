# Migrating the server to Citus: the later step

@use_case Development/UseCase/UC_Citus
@facet Store
@status Proposed

IQ's remote store starts as the **server** shape: one plain PostgreSQL 17 (`deploy-citus.sh shape server`, the
default). It runs the same schema as the cluster and holds the same record. A server that already holds the record
becomes a **Citus cluster** by this runbook, on the same box. Flipping the switch does not do it, and
`deploy-citus.sh shape citus` refuses while a server is up. Nothing here has been run yet. `deploy-citus.sh` gets a
`migrate` command when the step is taken.

## Why it can be a migration, not a rebuild

- `install-citus.sh ROLE=server` installs the **Citus package** with PostgreSQL. It is neither preloaded nor created,
  so the server runs plain Postgres, and becoming a coordinator needs no new packages.
- The schema files (`schema.sql`, `shacl.sql`, `tiered.sql`, `trgm.sql`) run their Citus branches only when the Citus
  catalog exists. Applied again on the server once Citus is loaded, they **distribute the tables that are already
  there**. `create_distributed_table` moves the existing rows into shards. The record keeps its IRIs, and nothing
  is exported or reimported.
- The plane does not change. `sparql_citus.py` and `ClusterAdapter` speak to both shapes; on a cluster, a one-pattern
  query runs on the workers (`citus.enable_repartition_joins`).

## The steps

1. **Back up first.** `deploy-citus.sh backup-up` (pgBackRest into `iq-citus-backup`) and a restore point, or a
   `pg_dump -Fc` of `iq`. Note the counts: `SELECT count(*) FROM iq.datum` and the same for `iq.quad` and
   `iq.cluster_datum`.
2. **Put the server on a VPC** with three new hosts. Use `hosts-up`'s VPC and its worker installs
   (`ROLE=worker PRIVATE_IP=… SUBNET=…`), each worker taking `/root/citus-{postgres,iq}.txt` from the server, as
   `hosts-up` does from its coordinator.
3. **Load Citus on the server.** In `/var/lib/pgsql/17/coord/iq.conf` set `shared_preload_libraries = 'citus'`,
   listen on the private address as well, admit the subnet in `pg_hba.conf`, then restart `iq-citus@coord`. Next,
   `CREATE EXTENSION citus` in `iq`, followed by `citus_set_coordinator_host('<private ip>', 5432)`.
4. **Add the workers.** `citus_add_node` for w1 and w2, as `hosts-up` does, and keep w3 as the spare.
5. **Distribute.** Apply `schema.sql`, `shacl.sql`, `tiered.sql` (if the cold tier is up) and `trgm.sql` again as
   postgres. The Citus branches now run:
   - the foreign key `quad_of_datum` is dropped;
   - `iq.datum` and `iq.quad` are distributed by IRI, co-located;
   - the foreign key is added back;
   - `iq.vocab` and the shape tables become reference tables;
   - `iq.cluster_datum` is distributed by owner.
   Writes wait while a table is distributed, so do this in a quiet window.
6. **Rebalance** with `citus_rebalance_start()` once the shards are placed.
7. **Record the shape.** Write `topology.json` as `hosts-up` writes it (`mode: hosts`), then run
   `deploy-citus.sh shape citus`.
8. **Verify.** The counts from step 1 must be equal. Then run the remote suite (`features/`): the `@citus` scenarios
   now run (placement, two-phase commit, crashes, scale-out, `domains`) and must pass, as they did on the cluster
   in October 2026 (`ACID-hosts.md`).

**Rollback** is the backup from step 1, restored to a fresh server shape.

## What is measured, and what is not yet

The server shape is measured on PostgreSQL 17.9 (pgembed, loopback): `ACID-server.md`, `GATE-server.md`,
`INFERENCE-server.md`, `SEARCH-server.md`, `SHACL-server.md`, `TRGM-server.md` and `SHAPE-server.md`. Every scenario
that does not need Citus or the host passes: one datum as one transaction, the competency questions, inference in
the transaction, no lost update, the gate as the only writer, SHACL at commit, and BM25 and trigram search. The
GherkinIq data plane's 38 scenarios (`-D graph=citus`) pass against it too. The migration itself, and a server
shape on Vultr, have not been run.
