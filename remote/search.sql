-- @use_case Development/UseCase/UC_Citus
-- @facet Store
--
-- BM25 over the distributed record (ParadeDB pg_search, deploy-citus.sh search-up): one bm25 index on iq.datum,
-- built by Citus on every shard. A search is answered by every worker and merged on the coordinator; the index is
-- transactional, so a datum's text is searchable in the transaction that writes it, by no other until it commits.
-- BM25's statistics (document count, term frequencies) are per shard -- a ranking across shards is close to, not
-- the same as, a ranking over one table (measured in features/search.feature).
CREATE INDEX IF NOT EXISTS datum_bm25 ON iq.datum USING bm25 (iri, raw) WITH (key_field = 'iri');
