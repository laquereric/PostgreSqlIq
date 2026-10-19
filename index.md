# PostgreSqlIq

@use_case Development/UseCase/UC_PostgreSqlIq
@facet Store
@status Proposed

**Postgres as IQ's durable-store standard.** pgembed (PostgreSQL 17 inside a Python wheel), started by IQ on the
laptop (`local/iq-pg.sh`) and on the server (`iq-pg.service`), and found by every process through
`IQ_REGISTRY_DSN`. It holds the registry, identity (UC_IdentityAndAccess), the UC_WebScrape index, the gate's
`cpcp_operations` replay and UC_OntologyPipeline's cases. It is one of the four stores of UC_DataPlatform.

- [`README.md`](README.md): what the PostgresIq repository holds -- LOCAL (pgembed + AGE + search on the desktop)
  and REMOTE (the same plus clustering), with CDC and offline modes run by a local and a remote manager.
- [`Postgres.okf`](Postgres.okf): the store as it is (UC_DataPlatform slice 1): tables and owners, deployment,
  IRIs, strengths, limits (no extensions, no row-level security), fit for each consumer, open questions.

This repository is IQ's Postgres, extracted from the IQ monorepo (2026-10-19) with its history, and vendored back
into IQ at `vendor/PostgreSqlIq` -- the code finds IQ three levels up (or `$IQ_REPO`) for the image python, the Vault
and the modules it shares (GherkinIq/Facet/Store, UC_LaptopCloudBridge).

| Folder | Was (in IQ) | What |
|---|---|---|
| `local/` | `UC_App/iq-pg.sh`, `UC_Postgres/age/`, `docs/architecture/PGEMBED_SETUP.okf` | LOCAL: the box's pgembed, AGE compiled into it; `UC_App/iq-pg.sh` stays as a shim |
| `remote/` | `UC_Citus` | REMOTE: the Citus cluster on Vultr -- ACID, SHACL at commit, search, tiered Iceberg, gate, backup, autopark |
| `cdc/` | `UC_CDC` | CDC: the box's Postgres streamed to the IQ Server through the slot `iq_cdc` |
