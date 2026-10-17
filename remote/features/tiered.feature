# @use_case Development/UseCase/UC_Citus
# @facet Store
@lake
Feature: Hot Postgres, cold Iceberg: tiered storage that keeps its transactions
  pgEdge ColdFront and EDB PGAA age a table's old partitions out of Postgres into Iceberg on object storage and read
  both tiers as one. Built here from pg_lake on the Citus coordinator: the hot tier a distributed table partitioned
  by month, the cold tier an Iceberg table whose catalog is Postgres, so moving a month is one transaction.

  Background:
    Given the Citus cluster on Vultr
    And the cold tier on the coordinator
    And events over the last four months, each month a hot partition

  Scenario: An Iceberg table commits and rolls back with Postgres, its files on object storage
    When an event is written to the cold tier and rolled back
    Then the cold tier does not have it
    When an event is written to the cold tier and committed
    Then the cold tier has it
    And the cold tier's files on object storage are Iceberg: metadata and Parquet

  Scenario: Moving the old months to Iceberg changes no answer
    When every month that ended before last month is moved to the cold tier
    Then those months' hot partitions are gone and the cold tier holds their rows
    And the events by month, and a checksum over every event, are as they were before the move

  Scenario: A move that fails leaves every row where it was
    When a move of the old months fails after copying them into Iceberg
    Then every month is still a hot partition and the cold tier holds none of them
    And the events by month, and a checksum over every event, are as they were before the move

  Scenario: A reader never sees an event twice or not at all while months move
    When the old months are moved while a reader counts every event, over and over
    Then every count the reader saw was the same

  Scenario: A late event for a month already in Iceberg goes to Iceberg
    Given every month that ended before last month is moved to the cold tier
    When a late event for the oldest month is written to both tiers as one
    Then it is in the cold tier and not the hot

  Scenario: The cold tier is open files: DuckDB reads it without Postgres
    Given every month that ended before last month is moved to the cold tier
    Then DuckDB, reading the Iceberg metadata straight from object storage, counts the same events as Postgres
