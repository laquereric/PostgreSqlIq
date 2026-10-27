-- @use_case Development/UseCase/UC_Citus
-- @facet Store
--
-- Trigram search over the distributed record (deploy-citus.sh trgm-up): pg_trgm from postgresql17-contrib (on every
-- node by install-citus.sh), created on the coordinator and propagated, and one GiST index with gist_trgm_ops on
-- iq.datum's text, built by Citus on every shard. GiST, not GIN, because it answers the nearest first: ORDER BY
-- raw <->> q LIMIT n walks the index on each shard (KNN), and the coordinator merges the shards' nearest. The same
-- index answers word similarity (raw %> q), LIKE and ILIKE. pg_trgm is 1.6 here, on the box (local/pg_trgm) and in
-- PGlite, so a trigram search has one answer on every tier.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS datum_trgm ON iq.datum USING gist (raw gist_trgm_ops);
-- pg_trgm's default thresholds are float4 literals (0.6f is 0.6000000238 as a double) and GiST's inner-page check
-- divides in float8 (3/5 = 0.5999999999999999778), so at the default a row exactly at the threshold is found by a
-- scan and missed by the index, as the tree happens to be built. Set as doubles on every node's database, the two
-- agree; the box (UC_WebScrape index.py) and PGlite set the same value per session.
ALTER DATABASE iq SET pg_trgm.similarity_threshold = 0.3;
ALTER DATABASE iq SET pg_trgm.word_similarity_threshold = 0.6;
ALTER DATABASE iq SET pg_trgm.strict_word_similarity_threshold = 0.5;
DO $$
BEGIN
    IF to_regclass('pg_catalog.pg_dist_partition') IS NOT NULL THEN   -- a Citus cluster's workers; a server has none
        PERFORM run_command_on_workers($cmd$ALTER DATABASE iq SET pg_trgm.similarity_threshold = 0.3$cmd$);
        PERFORM run_command_on_workers($cmd$ALTER DATABASE iq SET pg_trgm.word_similarity_threshold = 0.6$cmd$);
        PERFORM run_command_on_workers($cmd$ALTER DATABASE iq SET pg_trgm.strict_word_similarity_threshold = 0.5$cmd$);
    END IF;
END $$;
