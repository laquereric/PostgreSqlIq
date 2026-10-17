# Citus: Postgres at scale with MarkLogic's transaction guarantees

@use_case Development/UseCase/UC_Citus
@facet Store
@status Proposed

**The question.** UC_DataPlatform's comparison left MarkLogic one thing no other store of ours does: a document, its
triples, its text and its vector committed in **one transaction**, with inference answered **at query time inside
that transaction** (never stale, never seeing another transaction's uncommitted work). Today the plane splits that
across a local pgembed ("working memory": the record, AGE, the gate's replay) and MotherDuck (the large analytic
store); neither scales the governed record *and* keeps those guarantees. Can a Postgres hosted on Vultr do both?
[Citus](https://github.com/citusdata/citus) (AGPL-3.0, all features open since 11.0; the AGPL is already accepted for
pg_search) shards Postgres across nodes and keeps it ACID with two-phase commit.

## The design

| MarkLogic gives | Citus does it by |
|---|---|
| document + triples + text + vector in one transaction | `iq.datum` (jsonb, generated tsvector, pgvector) and `iq.quad` **co-located by the datum's IRI**: a datum is always a single-shard, single-worker transaction -- plain Postgres ACID, no coordination |
| inference at query time, in the transaction | `iq.entail()`: IQ's RDFS-Plus rules (`UC_DataPlatform/inference/iq-rdfs-plus.rules`) as SQL, run to fixpoint in a temp table **inside the caller's transaction** -- it sees that transaction's own writes and nothing uncommitted of others; nothing is materialized, so nothing goes stale |
| the vocabulary everywhere a query runs | `iq.vocab` is a **reference table**: a copy on every node, changed by two-phase commit |
| a transaction across forests on different hosts | Citus's **two-phase commit**: a transaction touching two workers prepares on both before either commits |
| no lost updates | Postgres `REPEATABLE READ` on the shard: the later writer gets a serialization failure |
| durability | each node's WAL; data checksums on |
| scale by adding hosts, rebalancing online | `citus_add_node` + `citus_rebalance_start` (logical replication: writes go on during the move); `citus_drain_node` to scale in |

**Placement on the plane.** The Citus cluster becomes the **governed record at scale** (the Silver record and the
graph role in one store). Local pgembed stays the working memory (the gate's replay, the app's registry, offline).
MotherDuck stays an engine over the lake (DuckLake's catalog could move to the cluster later). MarkLogic stays the
parked workbench for what it alone does well: its own rulesets, document security per role, and the Progress
conversation.

**What is not the same, said plainly.**
- **No global snapshot across workers.** A read spanning shards on two workers, concurrent with a two-phase commit,
  can see one side committed and not the other (Citus documents this). MarkLogic's queries run at one timestamp
  across all forests. The design keeps every governed write to one datum = one shard, so this only touches
  multi-datum reads racing multi-datum writes.
- **Query-time inference costs per query.** `iq.entail()` recomputes the closure over the whole graph each call,
  pulled to the coordinator. That is MarkLogic's trade (current, slower), fine at vocabulary scale; a graph of
  millions of triples needs the closure scoped (by graph or by a seed set) -- a later slice.
- **AGE does not distribute.** AGE's label tables use inheritance, which Citus cannot shard; at scale the graph is the
  plain `iq.quad` table, and SPARQL goes through a translator to SQL (sparql_age's subset, retargeted) -- not yet.

## Slices

1. **On-demand cluster + the guarantees under test** (this slice). `deploy-citus.sh up|status|down|psql` launches
   a Rocky 9 vc2-2c-4gb (about $0.03/h, only :22 open), and `service/install-citus.sh` installs PostgreSQL 17
   (PGDG; the major version of IQ's pgembed), Citus 14.2 and pgvector, as four nodes on one host (coordinator, w1,
   w2, and a spare w3), roles `postgres` (ssh only) and `iq` (no superuser). `schema.sql` is the design above.
   `features/acid.feature`: 10 scenarios, run over an SSH tunnel; results in [`ACID.md`](ACID.md).
   **Built 2026-10-17: all 10 pass** against PostgreSQL 17.11, Citus 14.2, pgvector 0.8.7. What they found:
   - The eight competency questions answer exactly as MarkLogic's `rdfs-plus` does, **at query time, inside the
     transaction**: 32-40 ms per question over the tunnel (MarkLogic, same questions: 414-699 ms;
     `UC_Progress/INFERENCE.md`). The graph is tiny; the closure is recomputed whole per call, so this gap is about
     overhead, not scale (slice 4).
   - Uncommitted writes are seen by their own transaction's inference and by no other; a committed datum or a
     vocabulary change (a reference-table write, two-phase across every node) is answered by the next query with
     nothing recomputed.
   - A transaction over two workers whose second write is an orphan triple is refused **at COMMIT** (the deferred
     foreign key fails on PREPARE) and neither side lands; no prepared transaction is left behind.
   - `REPEATABLE READ` refuses the lost update; a worker stopped as a crash stops it comes back in 0.7 s through
     WAL recovery with the datum intact.
   - Adding a worker and rebalancing took 8.6 s with 110 datums written meanwhile, none failed; draining it 4.2 s
     with 52, none failed.
   - **A Citus trap, found by running add/drain twice:** `citus_remove_node` leaves the deferred-drop records of
     the removed node's moved shards in `pg_dist_cleanup`; the node, re-added, gets a new group, so they can never be
     done, and the next move of those shards retries for ever ("the orphaned shard ... could not be cleaned up").
     `citus_plane.remove_worker` drains, runs `citus_cleanup_orphaned_resources()` until nothing is left on the
     node, and only then removes it; the scenario now ends by asserting nothing is orphaned.
   - Each of three deliberate breaks is caught: no TransitiveProperty rule (the questions), the document committed
     apart from its triples (the failed datum and the two-worker scenarios), read committed instead of repeatable
     read (the lost update).
   - Schema objects are created by `postgres` over ssh: Citus copies a table owner's role to the workers, which
     only a superuser may do. The client role `iq` has data rights only.
2. **Hosts, not ports.** **Built 2026-10-17: all 12 scenarios pass on both topologies** ([`ACID-hosts.md`](ACID-hosts.md)).
   `deploy-citus.sh hosts-up` makes a Vultr VPC (10.31.0.0/24) and four instances -- `iq-citus-coord`, `-w1`,
   `-w2`, and a spare `-w3` -- one node each (`install-citus.sh` ROLE=coordinator|worker). Postgres listens on
   loopback and the private address; pg_hba and firewalld admit the subnet only, to 5432 only; the public side stays
   ssh-only. Passwords are made on the coordinator and piped to the workers over ssh (never on this disk).
   `topology.json` says what is up, and the same scenarios run on either shape. Two new scenarios cut a two-phase
   commit between PREPARE and COMMIT (a test trigger holds the first worker's PREPARE 4 s; a watcher on the host
   crashes a node the instant the second worker has prepared; the coordinator's own 2PC recovery paused):
   - **a worker crashes after PREPARE:** the commit is answered (4.4 s, Citus warns "failed to commit transaction
     on" the worker); the worker restarts by WAL recovery still holding the transaction prepared -- its datum
     invisible -- and `recover_prepared_transactions()` commits it: both datums answer, nothing left prepared.
   - **the coordinator crashes after PREPARE:** the client loses the connection; with no commit record, recovery
     rolls back both workers' prepared transactions: neither datum answers. All or nothing, across a crash.
   - Caught when the recovery is skipped (both fail). Shapes measured: one datum put 251 ms, recovery 1.3 s.
   - Found on the way: the cleanup of a held scenario failed because `DROP TRIGGER` on a distributed table needs
     `citus.enable_unsafe_triggers` as much as `CREATE` does; and an installer re-run must not touch roles on a
     worker already in the cluster ("operation is not allowed on this node").
   The fork [laquereric/citus](https://github.com/laquereric/citus) (2026-10-17) is where the
   `citus_remove_node` cleanup fix would go; the cluster still runs the packaged 14.2.
2b. **SHACL at commit** (from `UC_Postgres/SHACL.md`). **Built 2026-10-17: 15 scenarios pass on both topologies**
   ([`SHACL-hosts.md`](SHACL-hosts.md)). GraphDB checks each transaction against the shapes at commit and refuses
   what violates them; Postgres has no SHACL engine (pgRDF's -- forked as
   [laquereric/pgRDF](https://github.com/laquereric/pgRDF) -- is a stub until its 0.4). So: **shapes compiled to
   SQL, checked when the transaction commits.** `shacl.py` compiles IQ's own LinkML-derived shapes
   (`GherkinIq/Facet/*/derived/*.shacl.ttl`) into rows of three reference tables, refusing by name any SHACL outside
   the supported Core subset (targetClass with subclasses, min/maxCount, datatype, nodeKind, class, in, pattern,
   min/maxInclusive, closed). `shacl.sql` adds `iq.put_datum()`, the governed write (the plane's `put` now goes
   through it), which queues the datum; a deferred constraint trigger on the coordinator runs at COMMIT -- before
   Citus sends PREPARE to any worker -- and refuses the whole transaction (`check_violation`, the report as JSON in
   DETAIL) if any datum written breaks its shape. `iq.shacl_check(datum)` gives the same report on demand.
   - 9 of IQ's 11 shapes graphs compile (928 property shapes); ScapeShape (`sh:or`) and MarkovKernel (`sh:node`,
     `sh:sparql`) are refused, not half-enforced.
   - Each kind of break of a Claim is refused at COMMIT and nothing of it lands; a subclass instance is held to its
     superclass's shape; `sh:class` is checked across datums inside the transaction (a FileAt may point at a Blob
     typed later in the same transaction); removing a required triple is refused; a bad datum on one worker keeps a
     good one on the other out; a changed shape applies from the next commit; the role `iq_agent` cannot write
     around the shapes.
   - **Against UC_OntologyPipeline's own SHACL validator** (over Oxigraph, sharing no code): 200 datums generated
     from the Store shapes, most broken one way -- the same 139 violations, of all seven kinds.
   - Cost: a commit held to the shapes, 112 ms for one Claim over the tunnel; 45 ms a datum checked on demand.
   - Each of three breaks is caught (no check at commit: 12 scenarios fail; `sh:class` always true; `sh:closed`
     ignored).
   - Found: Citus refuses a transaction in which two users touch the same shards, so the write path's functions
     are `iq`'s (SECURITY DEFINER), not postgres's; and an error raised on a worker inside a function's `INSERT ...
     SELECT` comes back as XX000, so `put_datum` checks for missing terms itself.
   - Not yet: a focus node is held to its own datum's triples (the plane's one-datum-per-subject rule); `drop_datum`
     does not recheck datums that point at the dropped one with `sh:class`; `iq` itself still has direct DML (only
     `iq_agent` is held to the governed path) until the gate is the only writer (slice 5).
2c. **Hot Postgres, cold Iceberg** (from `UC_Postgres/TieredIceberg,md`). The ColdFront / PGAA pattern built from
   [pg_lake](https://github.com/Snowflake-Labs/pg_lake) (Apache-2.0) on the coordinator: `iq.event` is the hot tier
   (distributed, co-located with `iq.datum`, a partition a month), `iq.event_cold` the cold tier (an Iceberg table;
   pg_lake keeps the Iceberg catalog in Postgres, so a write to it commits or rolls back with the transaction),
   `iq.event_all` both as one -- to read, and to write (a late row for a month already moved goes to Iceberg) --
   and `iq.tier_out(t)` moves every month that ended before `t` in ONE transaction (copy into Iceberg, drop the
   partition). `deploy-citus.sh lake-build` compiles pg_lake (DuckDB, the Azure SDK) for PG17/el9 on a throwaway
   vc2-6c-16gb and keeps only the packed result; `lake-up` installs it with `pgduck_server` (unix socket only) and
   puts the cold tier on **Vultr Object Storage** (`lake-vultr`): the subscription that holds `iq-blobs`
   (UC_BlobStore, `ewr1.vultrobjects.com`), in a bucket of its own, `iq-lake` -- `iq-blobs` is never written. The
   subscription's S3 key is read from the Vultr API and piped over ssh into a root-only file on the coordinator (and
   pgduck_server's secret); it is never on this disk. (Vultr wants the payload hash signed: `x-amz-content-sha256`.)
   `LAKE_STORE=loopback` keeps the first run's SeaweedFS stand-in for a test with no bucket. `features/tiered.feature`
   (6 scenarios; skipped on a cluster without the cold tier). **Built 2026-10-17: all 6 pass on the VPC cluster, the
   cold tier on Vultr Object Storage**
   ([`TIERED-hosts.md`](TIERED-hosts.md)), pg_lake 3.5.7 preloaded beside Citus 14.2 on PostgreSQL 17.11:
   - an Iceberg write rolls back and commits with Postgres; its files on the store are Iceberg (metadata JSON,
     Avro manifests, Parquet);
   - moving two months (1,000 events) to Iceberg on Vultr takes 1.0-1.4 s in one transaction (0.7 s on loopback), and the events by month and a
     checksum over every event are unchanged; a move failing after the copy leaves every month hot and Iceberg
     without them;
   - a reader counting both tiers through the move sees 2,000 every time (24 reads); with the move split into two
     transactions it sees the rows twice -- caught;
   - a late event for a month already in Iceberg is written to Iceberg through `iq.event_all`;
   - DuckDB, reading the Iceberg metadata straight from the store (`iceberg_scan`, no Postgres), counts the same
     1,000 cold events: the cold tier is open files, readable by DuckDB / MotherDuck as the lake.
   - Each of three breaks is caught (drop without copy: 3 fail; the move as two transactions; late writes always
     hot).
   - Found: pg_lake checks the Iceberg location is empty even on `CREATE TABLE IF NOT EXISTS` of a table that
     exists; the PGDG build needs `with_llvm=no` (no clang) and `krb5-devel`; and the union view is local to the
     coordinator (Citus cannot distribute a view over a foreign table) -- which is where it is read.
   - Not yet: the move scheduled (pg_cron); a month brought back hot; a key scoped to `iq-lake` alone (Vultr's
     subscription key reaches every bucket in it, `iq-blobs` included).
3. **The plane's adapter.** A `CitusAdapter` for `dataplane.py` (record and graph roles in one), so the conformance
   suite (`GherkinIq/Facet/Store/features/dataplane.feature`) runs against the cluster; `plane_sparql` via the SQL
   translation.
4. **Scoped entailment and scale.** Closure per seed set; load the ProtegeIq vocabulary revisions and a WebScrape
   corpus; measure against MarkLogic's 10-20x query-time cost; pg_search (BM25) on distributed tables.
5. **Operations.** Park/restore like MarkLogic (snapshot per host), backups (pgBackRest to Blob), and the gate as
   the only writer.

## Run

    ./deploy-citus.sh up            # slice 1: one host (or hosts-up: a VPC and a host per node); ~5 min
    cd features && "$(../../UC_App/iq-python.sh --bin)" -m behave   # the 10 scenarios; writes ACID.md
    ./deploy-citus.sh down          # or hosts-down: destroy, which is what stops the billing
