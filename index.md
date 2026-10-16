# Postgres

@use_case Development/UseCase/UC_Postgres
@facet Store
@status Proposed

**Postgres as IQ's durable-store standard.** pgembed (PostgreSQL 17 inside a Python wheel), started by IQ on the
laptop (`UC_App/iq-pg.sh`) and on the server (`iq-pg.service`), and found by every process through
`IQ_REGISTRY_DSN`. It holds the registry, identity (UC_IdentityAndAccess), the UC_WebScrape index, the gate's
`cpcp_operations` replay and UC_OntologyPipeline's cases. It is one of the four stores of UC_DataPlatform.

- [`Postgres.okf`](Postgres.okf): the store as it is (UC_DataPlatform slice 1): tables and owners, deployment,
  IRIs, strengths, limits (no extensions, no row-level security), fit for each consumer, open questions.

The code lives with its owners; this use case is the store's definition and its place on the data plane.
