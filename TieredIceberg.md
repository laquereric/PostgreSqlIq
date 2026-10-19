@use_case Development/UseCase/UC_PostgreSqliq
@facet Agent

does the postgres ecosystem support iceberg tiered storage

Yes. A few products do true automatic hot/cold tiering to Iceberg, and several extensions give you Iceberg-backed tables that you can tier yourself.

Automatic tiering (hot Postgres, cold Iceberg)

pgEdge ColdFront. Its main feature is tiered tables: a partitioned Postgres table whose hot partitions age out to Iceberg on S3 after a retention window. Reads then union live Postgres data with cold S3 data, and writes go to the right tier. An archiver run on a cron moves rows from hot to cold, and the Iceberg catalog is Lakekeeper (a REST catalog).
EDB Postgres Distributed (PGD) with PGAA. Tiered storage is powered by the Postgres Analytics Accelerator extension. It offloads data partitions to object storage and queries them in place. Partitions move from local heap to the analytics storage based on an age threshold. The cold tier can be Iceberg or Delta Lake, and you can restore a table back into the local heap if you need transactional writes again.

Iceberg tables in Postgres (you handle the tiering)

pg_lake (Snowflake, open source). CREATE TABLE ... USING iceberg creates an Iceberg v2 table whose data files live on object storage, with Postgres acting as the catalog. Writes to it commit or roll back with the surrounding Postgres transaction. To tier, you move old partitions into Iceberg tables yourself, for example on a schedule with pg_cron.
pg_mooncake. It keeps a columnstore mirror of your Postgres tables in Iceberg that stays in sync with the source. This gives you an analytics copy, not offloading: the data still lives in Postgres too.
pg_duckdb. It can read Iceberg, and write through DuckDB. Write support for REST catalogs is newer than its read support.
Supabase ETL. It replicates Postgres tables to Iceberg through Supabase’s platform, the managed path for Supabase users.

If you want automatic age-based offload with transparent queries across both tiers, ColdFront and EDB PGD/PGAA are the direct fits. Otherwise, pg_lake plus partitioning and a scheduled move job is the most common do-it-yourself setup.

Sources:

pgEdge ColdFront – object store guide
pgEdge ColdFront – architecture
EDB PGD – Using tiered storage
EDB PGD – Setting up PGAA
pg_lake on GitHub
Postgres Meets the Lakehouse
pg_mooncake docs