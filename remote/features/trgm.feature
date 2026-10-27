# @use_case Development/UseCase/UC_Citus
# @facet Store
@trgm
Feature: Trigram search over the distributed record, nearest first on a GiST index
  pg_trgm 1.6 is on every tier: the cluster (postgresql17-contrib), the box (local/pg_trgm) and PGlite. On the
  cluster the index is GiST with gist_trgm_ops on iq.datum's text (trgm.sql), built by Citus on every shard, so a
  misspelled word finds what the record says and the nearest come first.

  Background:
    Given the Citus cluster on Vultr
    And the trigram index on the record
    And 60 datums of text on the plane, spread over the workers

  @citus
  Scenario: A misspelled word finds every datum that says the word, from every worker
    When the record is searched for words like "lakehose"
    Then every datum whose text says "lakehouse" is found, and no other
    And the datums found are held on every worker

  @citus
  Scenario: The nearest come first, walked on each shard's GiST index
    When the nearest 10 datums to "lakehose" are asked
    Then each of them says "lakehouse"
    And every shard's plan for it scans its "datum_trgm" index
    And every shard of iq.datum has the index as GiST with gist_trgm_ops
