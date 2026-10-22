# @use_case Development/UseCase/UC_Citus
# @facet Store
Feature: The cluster's record, distributed by owner -- a domain's rows on one worker, its read a single-shard query
  UC_Group's rule 3 on Citus. iq.cluster_datum (schema.sql) holds one row per (owner, iri) and is distributed by
  owner; PostgreSqlIq cdc apply.py domain puts a domain's shipped rows there through dataplane.ClusterAdapter, on a
  plane bound to the domain's namespace, connected as iq through the tunnel.

  Scenario: two domains' slots applied: each domain's rows sit on one worker, and are read and restored from there
    Given the cluster's record is distributed by owner
    When a group's domain ships 6 rows and a user's domain ships 4, and the cluster applies each through its plane
    Then every row is on the cluster under its domain's owner
    And each domain's rows sit on the one worker that holds its owner's shard, and on no other
    And reading one domain's rows is a single-shard query
    And restoring each domain by owner selects exactly its rows, the same values
    And applying the slots again puts nothing new

  Scenario: a domain cannot ship another namespace's row onto the cluster
    Given the cluster's record is distributed by owner
    When a group's domain ships a row of a user's namespace among its own, and the cluster applies it
    Then that row is refused as "not_my_namespace", and the cluster holds nothing of that user
