# CDC: research note

@use_case Development/UseCase/UC_CDC
@facet Store

Postgres supports CDC natively through its write-ahead log (WAL), and a large set of tools is built on top of that.

Built into Postgres
Logical decoding. Turns WAL entries into row-level insert, update, and delete events. It needs wal_level = logical.
Replication slots. Track how far each consumer has read, so Postgres keeps the WAL until the consumer has it. This guarantees nothing is lost, but an idle or slow slot makes WAL pile up and can fill the disk, so it’s the main thing to monitor.
Output plugins, which format the decoded changes:
pgoutput is built in and used by native logical replication and by most modern tools.
wal2json emits JSON and is widely available on managed services.
test_decoding is built in and meant for debugging.
decoderbufs emits Protobuf and is used by Debezium.
Publications and subscriptions. Native Postgres-to-Postgres logical replication.
Failover slots (Postgres 17+). Logical slots can be synced to a standby, so CDC survives a failover.
Triggers and LISTEN/NOTIFY. The older, lighter approach: you write audit tables with triggers or send notifications. It adds write overhead and isn’t as robust as reading the WAL.
Extensions
pglogical. Logical replication with more features than the built-in kind (row filters, conflict handling); it predates and inspired native replication.
pg_failover_slots. Kept replication slots across failovers before Postgres 17 had that built in.
CDC tools that read from Postgres

Open source and self-hosted:

Debezium. The most widely deployed option. It reads the WAL through a replication slot and converts changes into its own standard event format. It is tied to Kafka; you route onward with Kafka Connect, or use Debezium Server to skip Kafka.
Sequin. Streams changes to queues and streaming platforms without needing Kafka. It runs as a single Docker container next to your database (MIT license).
PeerDB. Supports log-based CDC plus cursor-based and xmin-based modes, and is known for fast parallel initial loads. ClickHouse acquired it and it powers ClickHouse ClickPipes; its main destinations are ClickHouse and Postgres-to-Postgres.
Supabase ETL replicates to Iceberg and other targets, and Supabase Realtime pushes changes to clients.
Stream processors that read Postgres CDC directly: Apache Flink CDC, Materialize, and RisingWave.

Managed and commercial:

Cloud providers: AWS DMS, GCP Datastream, and Azure Data Factory work if you’re committed to one provider.
Confluent: hosted Debezium.
ELT and pipeline vendors: Fivetran, Airbyte, Estuary, Striim, and Qlik Replicate.
Choosing one
Need	Typical choice
Postgres to Postgres	Native publications and subscriptions, or pglogical
Events into Kafka	Debezium
Events into queues or webhooks without Kafka	Sequin
Analytics replica (ClickHouse, warehouse)	PeerDB/ClickPipes, Fivetran, Airbyte
Lakehouse / Iceberg	Supabase ETL, Flink CDC, or the pg_lake/mooncake options from earlier

Whatever you pick, monitor replication slot lag and WAL size, and drop slots that are no longer used.

Sources:

Sequin on GitHub
Choosing the right, real-time, Postgres CDC platform
How Debezium captures changes from Postgres
PeerDB overview