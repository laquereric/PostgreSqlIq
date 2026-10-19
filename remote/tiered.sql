-- @use_case Development/UseCase/UC_Citus
-- @facet Store
--
-- Tiered storage, hot Postgres and cold Iceberg (from UC_PostgreSqliq/TieredIceberg,md): the pattern of pgEdge
-- ColdFront and EDB PGAA, built from pg_lake. Applied as postgres after install-lake.sh; idempotent.
--
--   iq.event          the HOT tier: a Citus table distributed by iri (co-located with iq.datum), partitioned by
--                     month (iq.event_pYYYYMM); this is where writes go
--   iq.event_cold     the COLD tier: an Iceberg table (pg_lake) on the coordinator, its Parquet and metadata on S3;
--                     pg_lake keeps the Iceberg catalog in Postgres, so a write to it commits or rolls back with the
--                     transaction around it
--   iq.event_all      both tiers as one table to read -- and to write: a row for a month older than every hot
--                     partition goes to the cold tier (ColdFront's "writes go to the right tier")
--   iq.tier_out(t)    every month that ended before t moved hot -> cold in ONE transaction: copied into Iceberg, its
--                     partition dropped. A failure anywhere leaves every row where it was.
--   iq.event_month(m) the hot partition for the month of m

CREATE TABLE IF NOT EXISTS iq.event (
    iri  text NOT NULL,
    at   timestamptz NOT NULL,
    kind text NOT NULL,
    body jsonb NOT NULL
) PARTITION BY RANGE (at);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'iq.event'::regclass) THEN
        PERFORM create_distributed_table('iq.event', 'iri', colocate_with => 'iq.datum');
    END IF;
END $$;

-- not CREATE TABLE IF NOT EXISTS: pg_lake checks the Iceberg location is empty even when the table already exists
DO $$
BEGIN
    IF to_regclass('iq.event_cold') IS NULL THEN
        CREATE TABLE iq.event_cold (iri text NOT NULL, at timestamptz NOT NULL, kind text NOT NULL, body jsonb NOT NULL)
            USING iceberg;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION iq.event_month(m timestamptz) RETURNS text
LANGUAGE plpgsql AS $fn$
DECLARE
    lo date := date_trunc('month', m AT TIME ZONE 'UTC')::date;
    part text := 'event_p' || to_char(lo, 'YYYYMM');
BEGIN
    IF to_regclass('iq.' || part) IS NULL THEN
        EXECUTE format('CREATE TABLE iq.%I PARTITION OF iq.event FOR VALUES FROM (%L) TO (%L)',
                       part, lo::timestamptz AT TIME ZONE 'UTC', (lo + interval '1 month')::timestamptz AT TIME ZONE 'UTC');
    END IF;
    RETURN part;
END
$fn$;

CREATE OR REPLACE FUNCTION iq.tier_out(before timestamptz, fail_after_copy boolean DEFAULT false)
RETURNS TABLE (part text, moved bigint)
LANGUAGE plpgsql AS $fn$
DECLARE r record; n bigint;
BEGIN
    FOR r IN
        SELECT c.relname, to_date(substr(c.relname, 8), 'YYYYMM') AS lo
          FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_namespace s ON s.oid = c.relnamespace
         WHERE i.inhparent = 'iq.event'::regclass AND s.nspname = 'iq' AND c.relname ~ '^event_p[0-9]{6}$'
           AND (to_date(substr(c.relname, 8), 'YYYYMM') + interval '1 month') <= before AT TIME ZONE 'UTC'
         ORDER BY 2
    LOOP
        EXECUTE format('INSERT INTO iq.event_cold (iri, at, kind, body) SELECT iri, at, kind, body FROM iq.%I', r.relname);
        GET DIAGNOSTICS n = ROW_COUNT;
        IF fail_after_copy THEN
            RAISE EXCEPTION 'tier_out: failing on purpose after copying % rows of %', n, r.relname;
        END IF;
        EXECUTE format('DROP TABLE iq.%I', r.relname);
        part := r.relname; moved := n;
        RETURN NEXT;
    END LOOP;
END
$fn$;

CREATE OR REPLACE VIEW iq.event_all AS
    SELECT iri, at, kind, body, 'hot'::text AS tier FROM iq.event
    UNION ALL
    SELECT iri, at, kind, body, 'cold'::text AS tier FROM iq.event_cold;

-- runs as the writer (Citus refuses two users on the same shards in one transaction); the hot partitions are made
-- ahead by the operator (iq.event_month), as pg_partman or create_time_partitions would
CREATE OR REPLACE FUNCTION iq.event_route() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $fn$
DECLARE
    month timestamptz := date_trunc('month', NEW.at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
    part text := 'event_p' || to_char(NEW.at AT TIME ZONE 'UTC', 'YYYYMM');
BEGIN
    IF to_regclass('iq.' || part) IS NOT NULL THEN
        INSERT INTO iq.event (iri, at, kind, body) VALUES (NEW.iri, NEW.at, NEW.kind, NEW.body);
    ELSIF month < (SELECT min(to_date(substr(c.relname, 8), 'YYYYMM')) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
                    WHERE i.inhparent = 'iq.event'::regclass) THEN
        INSERT INTO iq.event_cold (iri, at, kind, body) VALUES (NEW.iri, NEW.at, NEW.kind, NEW.body);
    ELSE
        RAISE EXCEPTION 'no hot partition for %', part USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END
$fn$;
DROP TRIGGER IF EXISTS event_route ON iq.event_all;
CREATE TRIGGER event_route INSTEAD OF INSERT ON iq.event_all FOR EACH ROW EXECUTE FUNCTION iq.event_route();

GRANT SELECT, INSERT ON iq.event, iq.event_all TO iq;
GRANT SELECT, INSERT ON iq.event_cold TO iq;
