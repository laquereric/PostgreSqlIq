# @use_case Development/UseCase/UC_Citus
# @facet Store
@backup
Feature: Every node backed up to Vultr, and restorable to one moment across the cluster
  pgBackRest on every node archives its WAL continuously and keeps full backups in a Vultr bucket of its own.
  Citus writes one named restore point into every node's WAL at once, so all nodes can be brought back to the same
  moment. The drill restores a worker's backup beside the live one, to such a point, and reads it.

  Background:
    Given the Citus cluster on Vultr
    And every node is backed up to Vultr

  Scenario: Every node has a full backup and its WAL archived in the bucket
    Then every node's backup is ok, with a full backup and archived WAL

  Scenario: A restore point across the cluster brings a worker back to that moment
    Given a datum "test:before" committed on the first worker
    When a restore point is made across the cluster
    And a datum "test:after" is committed on the same worker
    And that worker's backup is restored to the restore point beside it
    Then the restored worker holds "test:before" and not "test:after"
