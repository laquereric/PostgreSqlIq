# @use_case Development/UseCase/UC_Citus
# @facet Store
@search
Feature: BM25 search over the distributed record, in the transaction
  MarkLogic commits a document and its text index together. On the cluster the text index is ParadeDB's pg_search,
  a BM25 index on iq.datum that Citus builds on every shard.

  Background:
    Given the Citus cluster on Vultr
    And the BM25 index on the record
    And 60 datums of text on the plane, spread over the workers

  Scenario: A search ranks datums from every worker
    When the record is searched for "lakehouse"
    Then every datum whose text says "lakehouse" is found, and no other
    And the datums found are held on every worker
    And they are ranked by how often their text says it

  Scenario: Text is searchable in the transaction that writes it, and by no other until it commits
    Given a transaction that has put a datum saying "zeppelin" and not committed
    Then inside it a search for "zeppelin" finds the datum
    And another session's search for "zeppelin" finds nothing
    When the transaction rolls back
    Then no session's search for "zeppelin" finds anything

  Scenario: Ranked across shards, the order is close to one table's
    When the record is searched for "lakehouse" across the shards and in one table
    Then the two find the same datums
    And the share of the top ten they agree on is recorded
