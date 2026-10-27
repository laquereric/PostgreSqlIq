# @use_case Development/UseCase/UC_Citus
# @facet Store
Feature: Postgres at scale keeps MarkLogic's transaction guarantees
  MarkLogic commits a document, its triples, its text and its vector together, and answers inference at query time
  inside the transaction, so an inference is never stale and never sees another transaction's uncommitted work.
  These scenarios hold a Citus cluster on Vultr (a coordinator and two workers, PostgreSQL 17, Citus 14) to the
  same promises -- on one host (slice 1) or one host per node over a VPC (slice 2), whichever deploy-citus.sh put up -- with UC_DataPlatform's platform vocabulary and its eight competency questions, the ones MarkLogic
  already answered (UC_Progress/INFERENCE.md).

  Background:
    Given the Citus cluster on Vultr
    And the platform is put as datums, its vocabulary on every node

  @citus
  Scenario: The record is spread over the workers, each datum whole on one, the vocabulary on every node
    Then the datums' shards are on 2 workers
    And every datum's triples are on the worker that holds its document
    And the vocabulary is on every node

  Scenario: A datum is one transaction: its document, text, vector and triples
    When the datum "test:citus" is put as a RecordStore with the text "distributed postgres at scale"
    Then its document, a text search for "distributed", the nearest vector and its triples all answer "test:citus"

  Scenario: A datum that fails leaves nothing behind
    When the datum "test:broken" is put and its last triple is refused
    Then nothing of "test:broken" answers: no document, no text, no vector, no triples

  Scenario: The eight competency questions answer as MarkLogic's do
    Then with inference every competency question answers as on MarkLogic
    And without inference every competency question answers empty

  Scenario: Inference sees its own transaction's writes and no other's
    Given a transaction that has put "test:duckdb" as a RecordStore and not committed
    Then inside it "test:duckdb" is a Store
    And another session does not see "test:duckdb" as a Store
    When the transaction rolls back
    Then no session sees "test:duckdb" as a Store

  @citus
  Scenario: Inference is never stale: new data and a new vocabulary are answered at once
    When "test:duckdb" is committed as a RecordStore
    Then "test:duckdb" is a Store at once, with nothing recomputed
    When one transaction makes "test:ColumnStore" a subclass of Store and commits "test:motherduck" as a ColumnStore
    Then "test:motherduck" is a Store at once, with nothing recomputed
    And every node's copy of the vocabulary holds "test:ColumnStore"

  @citus
  Scenario: One transaction across two workers commits on both or on neither
    Given two test datums whose shards are on different workers
    When one transaction puts both
    Then both answer
    When one transaction puts a datum on one worker and, on the other, a triple whose datum does not exist
    Then the commit is refused
    And neither of the second transaction's writes answers

  @hold @citus
  Scenario: A two-phase commit whose worker crashes after PREPARE is finished by the coordinator
    Given two test datums whose shards are on different workers
    And the first worker holds its PREPARE for 4 seconds
    When one transaction puts both, and the second worker crashes the moment it has prepared
    Then the commit succeeds
    And the second worker came back holding the transaction prepared, neither committed nor rolled back
    When the coordinator recovers its two-phase commits
    Then both answer
    And no prepared transaction is left on any worker

  @hold @citus
  Scenario: A two-phase commit whose coordinator crashes after PREPARE commits nowhere
    Given two test datums whose shards are on different workers
    And the first worker holds its PREPARE for 4 seconds
    When one transaction puts both, and the coordinator crashes the moment the second worker has prepared
    Then the commit is lost with the connection
    When the coordinator recovers its two-phase commits
    Then neither answers
    And no prepared transaction is left on any worker

  Scenario: A concurrent update is not lost
    Given "test:tiered" is committed at tier "Silver"
    When a repeatable-read transaction reads it, another moves it to "Gold" and commits, and the first then moves it to "Bronze"
    Then the first is refused with a serialization failure
    And "test:tiered" is at tier "Gold"

  @citus
  Scenario: A committed datum survives a crash of the worker that holds it
    When "test:durable" is committed as a RecordStore
    And the worker holding it is stopped as a crash stops it, and started again
    Then "test:durable" answers with its document and its triples

  @citus
  Scenario: A worker is added and removed while writes go on
    (Run it twice: a worker removed with its moved shards' old copies not yet dropped broke the next add.)
    Given the third worker is not in the cluster
    When the third worker is added and the shards rebalanced while datums are being put
    Then the datums' shards are on 3 workers
    And every datum put during the move answers
    And with inference every competency question answers as on MarkLogic
    When the third worker is drained and removed while datums are being put
    Then the datums' shards are on 2 workers
    And every datum put during the move answers
    And no orphaned shard is left behind
