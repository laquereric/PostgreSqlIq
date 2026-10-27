# @use_case Development/UseCase/UC_Citus
# @facet Store
@citus
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

  Scenario: a domain's own Postgres ships its slot onto the cluster continuously, its namespace only
    Given the cluster's record is distributed by owner
    And a domain's own Postgres, its slot narrowed to the group's namespace, holding 5 of the group's rows and 1 of another namespace
    When the shipper runs a cycle onto the cluster
    Then the cluster holds the group's 5 rows and nothing of the other namespace, and the domain's slot is empty
    When the domain's Postgres takes 2 more of the group's rows, and the shipper runs a cycle onto the cluster
    Then the cluster holds the group's 7 rows, and a further cycle ships nothing

  Scenario: the shipper as the service runs it: its own tunnel and slot, a resync, then each new row as it is written
    Given the cluster's record is distributed by owner
    And a domain's own Postgres, its slot narrowed to the group's namespace, holding 5 of the group's rows and 1 of another namespace
    When the service script ships that domain's namespace, every second
    Then within a minute the cluster holds the group's 5 rows and nothing of the other namespace
    When the domain's Postgres takes 2 more of the group's rows
    Then within a minute the cluster holds the group's 7 rows
    And stopping the service takes its tunnel down with it
