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

## The store's shape: one server now, Citus later

The remote store has a **shape**, a switch: `deploy-citus.sh shape [server|citus]` (kept in `~/.iq/citus/shape`;
`SHAPE` overrides it).

- **`server`, the default,** is one plain PostgreSQL 17 on one Vultr instance (`iq-pg`): pgvector, pg_trgm, the
  gate's writer, SHACL at commit and query-time inference, the same schema with its tables local.
- **`citus`** is the cluster this page describes.

`up` builds the selected shape, and `install-citus.sh ROLE=server` installs Citus without loading it. The schema
files run their Citus branches only where the Citus catalog exists, so the one schema serves both shapes. A shape
that is up holds the record, so the switch refuses to move away from it. A server becomes a cluster by
[MIGRATE-to-Citus.md](MIGRATE-to-Citus.md), the later step, on the same box.

A topology may also name a server by `dsn` (no tunnel, no ssh), for a throwaway Postgres in tests. The scenarios
tagged `@citus` (placement, two-phase commit, crashes, scale-out, `domains`) are skipped on a server, and those
tagged `@host` (backups) are skipped on a server reached by DSN. Each skip is named in the report. **Up on Vultr
since 2026-10-27** as `iq-pg` (PostgreSQL 17.11, pgvector, pg_trgm, pg_search): 35 scenarios pass (`*-server.md`).
`search-up` preloads pg_search alone on a server; with Citus, pg_search goes after it.

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
2b. **SHACL at commit** (from `UC_PostgreSqliq/SHACL.md`). **Built 2026-10-17: 15 scenarios pass on both topologies**
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
2c. **Hot Postgres, cold Iceberg** (from `UC_PostgreSqliq/TieredIceberg,md`). The ColdFront / PGAA pattern built from
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
3. **The plane's adapter.** **Built 2026-10-17: the data plane's conformance suite passes on the cluster, 38 of 38**
   (`GherkinIq/Facet/Store/features/dataplane.feature -D graph=citus`: the same scenarios Oxigraph (36, 2 skipped),
   AGE (38) and MarkLogic pass; none skipped -- the two Postgres-only ones pass too). `sparql_citus.py`:
   `CitusRecordAdapter` and `CitusGraphAdapter`, both on the cluster's own tables through the governed write
   `iq.put_datum`, so a datum's row, its Turtle triples (in the named graph its IRI names, the new `iq.quad.ng`) and
   its prov:wasDerivedFrom edges (default graph) are one single-shard transaction, held to the shapes at COMMIT; the
   exact body, media type, derived_from and recorded_at are new `iq.datum` columns. `plane_sparql` is the
   sparql_age subset (SELECT/ASK, BGP, GRAPH <iri> / ?g, OPTIONAL as LEFT JOIN LATERAL, FILTER = / != / &&,
   DISTINCT, LIMIT/OFFSET) translated to one SQL query; the rest is refused as `unsupported_sparql`. Each of three
   breaks is caught (no provenance quads: 2 fail; GRAPH <iri> ignored; OPTIONAL as an inner join). Not yet: the
   quads a query touches are pulled to the coordinator before the joins (by its constant predicates) -- joins pushed
   to the workers, and `iq.entail()` behind `plane_sparql` (inference as MarkLogic's rdfs-plus), are slice 4.
4. **Scoped entailment and scale.** **Built 2026-10-17** ([`INFERENCE-hosts.md`](INFERENCE-hosts.md),
   [`SEARCH-hosts.md`](SEARCH-hosts.md)).
   - **`iq.entail(seeds)`** closes only what is connected to the seeds: the vocabulary in full, the instances of a
     seed class (typed by it, a subclass or an equivalent, or through a property whose domain or range types into
     it), then the component of the seeds breadth first -- never through the vocabulary (`rdf:type` into a class,
     subClassOf and the other T-Box edges), which would join every instance into one component. Every rule's body
     is joined through shared nodes or the vocabulary, so this is exact for every triple about a node it reaches.
   - **Checked against the whole closure**: for each competency question's anchor and the class Store on the
     platform, and for three random copies at each size of a graph of the platform copied 100, 1,000 and 4,000
     times (1,000 to 40,000 quads): the same triples about every node in scope. **The whole closure grows with the
     graph (0.79 s, 8.1 s, 36.1 s); the scoped closure stays 45 triples and 0.10-0.13 s at every size: 272x
     faster at 40,000 quads.**
   - **`plane_sparql` with inference** (`CitusStore(..., inference=True)`): over the scope the query's IRIs reach,
     the eight competency questions answer through the plane as on MarkLogic's rdfs-plus, 360-550 ms each over
     the tunnel (MarkLogic: 414-699 ms); without inference, empty. GRAPH with inference is refused.
   - **Where a query runs, measured at 200,000 quads**: one triple pattern on the workers (0.29-0.34 s against
     0.32-0.40 s pulled); a join pulled to the coordinator by its predicates (0.35 s against 4.0 s for Citus's
     repartition join). That is the default; `pushdown=True` forces the workers. Both give the same answers.
   - **BM25 on the distributed record** (`deploy-citus.sh search-up`: ParadeDB pg_search 0.26.2 preloaded on
     every node, a bm25 index on iq.datum built on every shard): a search ranks datums from every worker (151 ms);
     a datum's text is searchable in the transaction that writes it and by no other until it commits, and goes
     with a rollback -- MarkLogic's text-in-the-transaction. BM25's statistics are per shard: scores overlap
     across mention counts (one mention up to 0.85, two from 0.80) and the top ten shares 7 of 10 with one table's.
   - Breaks caught: the walk stopped at one hop (3 fail), seed classes not expanded, the walk let through the
     vocabulary (the size is no longer flat); a search's write committed early.
   - **Trigram search on the distributed record** (`deploy-citus.sh trgm-up`, `trgm.sql`, 2026-10-25): pg_trgm 1.6
     from postgresql17-contrib, and `datum_trgm`, a GiST `gist_trgm_ops` index on `iq.datum.raw`, on every shard.
     A misspelling ("lakehose") finds every datum that says "lakehouse", from every worker. The nearest come first,
     and each of the 8 shards' plans walks its own index (`features/trgm.feature`, [`TRGM-hosts.md`](TRGM-hosts.md)).
     pg_trgm's float4 default thresholds make GiST miss rows exactly at the threshold, so `trgm.sql` sets all three
     as doubles on every node's database (UC_PgliteIq slice 3 measured the bug).
   - Not yet: the ProtegeIq vocabulary and a WebScrape corpus as real loads; global BM25 statistics.
5. **Operations.** **Built 2026-10-17.**
   - **The gate is the only writer** ([`GATE-hosts.md`](GATE-hosts.md), `gated_plane.py`, `gate/iam.json`): an
     agent's role (`iq_agent`) reads and writes nothing -- not a raw INSERT, not `iq.put_datum`; the one
     application role with the governed write is `iq_gate` (password made on the coordinator by `gate-up`, in each
     node's .pgpass, never on this disk), held by `GatedWriter`, which asks the real Rust gate (cpcp-gate, UC_Progress's
     demo gate with this use case's policy) for every put, get and query under a fresh operationId and stamps the
     datum with it (`iq.datum.operation`). A put the gate refuses writes nothing; one it admits but the shapes refuse
     lands nothing; `audit()` finds every datum on the cluster admitted in the gate's journal and every admitted put on
     the cluster. 5 scenarios; the SHACL agent scenario now reads: the agent cannot write at all, the gate's writer
     cannot write around the shapes. Breaks caught (refusals ignored: 2 fail; the agent granted the write).
   - **Backups** ([`BACKUP-hosts.md`](BACKUP-hosts.md), `backup-up`, `service/install-backup.sh`): pgBackRest 2.59 on
     every node, WAL archived continuously and full backups, in a Vultr bucket of its own, `iq-citus-backup`. The
     drill: a datum before a `citus_create_restore_point`, one after; the worker holding them restored from its
     backup beside the live one, to that point, started on another port: it holds the first and not the second (35 s).
     Restoring to the latest instead of the point is caught.
   - **Park and restore** (`hosts-park`, `hosts-restore`; `autopark` + `service/install-autopark.sh` after 2 h with no
     client session, as UC_MarkLogic's): every node stopped cleanly (coordinator first), the four halted, snapshotted,
     destroyed; restored from the snapshots on the same VPC, each node listening on the private address it was given
     and renamed in Citus (`citus_update_node`, `citus_set_coordinator_host`). **Done once, for real**: parked in
     14.5 min (four snapshots, then four instances destroyed), restored in 9 min (new instances, every public address
     changed). A datum written before parking answers after; Citus, pgvector, pg_lake and pg_search come back; all
     47 scenarios of the seven features pass on the restored cluster, and the plane's conformance suite 38 of 38.
     Vultr gave the four the same private addresses again, so the renaming was not exercised this time.
   - Found by the drill after the restore: `pg_ctl start` returns once a restored server serves reads, while it is
     still replaying towards the point -- and ParadeDB Community refuses reads from a server in recovery. The
     drill now waits for the promotion (`pg_is_in_recovery()` false) before it reads.
   - Not yet: a key scoped to the backup and lake buckets (the subscription's key is on every node); backups on a
     schedule (pg_cron or a timer); restoring the whole cluster to a point, not one worker; autopark installed
     (`service/install-autopark.sh` is written; loading the LaunchAgent is the user's call).

## Run

    ./deploy-citus.sh up            # slice 1: one host (or hosts-up: a VPC and a host per node); ~5 min
    cd features && "$(../../../../Development/UseCase/UC_App/iq-python.sh --bin)" -m behave   # the 10 scenarios; writes ACID.md
    ./deploy-citus.sh down          # or hosts-down: destroy, which is what stops the billing
