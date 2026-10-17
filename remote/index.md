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
2. **Hosts, not ports.** Workers on their own instances over a Vultr VPC; the same scenarios, plus a two-phase
   commit interrupted by killing a worker between PREPARE and COMMIT (recovered by the coordinator).
3. **The plane's adapter.** A `CitusAdapter` for `dataplane.py` (record and graph roles in one), so the conformance
   suite (`GherkinIq/Facet/Store/features/dataplane.feature`) runs against the cluster; `plane_sparql` via the SQL
   translation.
4. **Scoped entailment and scale.** Closure per seed set; load the ProtegeIq vocabulary revisions and a WebScrape
   corpus; measure against MarkLogic's 10-20x query-time cost; pg_search (BM25) on distributed tables.
5. **Operations.** Park/restore like MarkLogic (snapshot per host), backups (pgBackRest to Blob), and the gate as
   the only writer.

## Run

    ./deploy-citus.sh up                                   # launch (if absent) + install; ~5 min
    cd features && "$(../../UC_App/iq-python.sh --bin)" -m behave   # the 10 scenarios; writes ACID.md
    ./deploy-citus.sh down                                 # destroy: stops the billing
