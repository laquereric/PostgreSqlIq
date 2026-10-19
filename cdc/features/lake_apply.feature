# The lake's catalog lives in the User/Group domain's local Postgres and reaches the global cluster by CDC; BI
# attaches the replica read-only. Here the box is the run's scratch Postgres (a real slot), the IQ Server is the
# bridge's relay in-process, the global cluster is a second scratch Postgres, and the Parquet files are a temporary
# directory both sides read (object storage, in production).
#
# @use_case Development/UseCase/UC_CDC
# @facet Gherkin

Feature: The lake's catalog, replayed into the global cluster
  As BI reading Gold from the global cluster
  I want the catalog the box's sweeps write replayed there by CDC
  So that what I read is what the box wrote, with no hosted engine in between

  Background:
    Given the lake's catalog published for CDC on a fresh box
    And the IQ Server is up for the lake

  Scenario: A table written on the box is read the same from the global replica
    When the box writes 3 rows into "auth_events"
    And CDC ships a cycle and the global side applies it
    Then a read-only reader of the replica counts 3 rows in "iq_main.auth_events"
    And the replica holds as many lake snapshots as the box

  Scenario: Later sweeps follow, updates and deletes included
    When the box writes 3 rows into "auth_events"
    And the box writes 5 rows into "auth_events"
    And the box writes 2 rows into "registry_accounts"
    And CDC ships a cycle and the global side applies it
    Then a read-only reader of the replica counts 5 rows in "iq_main.auth_events"
    And a read-only reader of the replica counts 2 rows in "iq_main.registry_accounts"
    And the replica holds as many lake snapshots as the box

  Scenario: Publishing never breaks the lake's writer
    When the box writes 3 rows into "auth_events"
    And the box writes 4 rows into "auth_events"
    Then every table in the box's catalog is REPLICA IDENTITY FULL

  Scenario: Applying twice is applying once
    When the box writes 3 rows into "auth_events"
    And CDC ships a cycle and the global side applies it
    And the global side applies again
    Then the second apply applied nothing
    And a read-only reader of the replica counts 3 rows in "iq_main.auth_events"

  Scenario: A statistic that CDC de-identified bounds nothing, so no row is lost to pruning
    When the box writes an account "pat@example.com"
    And CDC ships a cycle and the global side applies it
    Then the replica's statistics carry no identity token
    And a read-only reader of the replica finds "pat@example.com" in "iq_main.accounts"

  Scenario: A catalog written before it was published arrives by the snapshot
    Given the box wrote 3 rows into "auth_events" before the catalog was published
    When the catalog is published and CDC ships its snapshot
    And the global side applies it
    Then the apply started from the snapshot
    And a read-only reader of the replica counts 3 rows in "iq_main.auth_events"
