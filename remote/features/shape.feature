# @use_case Development/UseCase/UC_Citus
# @facet Store
Feature: The store's shape: one server now, a Citus cluster as the later migration step
  IQ's remote store has a shape, a switch (deploy-citus.sh shape): `server`, the default, is one plain PostgreSQL 17;
  `citus` is a Citus cluster. The schema is the same and matches whichever is up: a server keeps its tables local
  and its foreign key, a cluster distributes them. A shape that is up holds the record, so the switch never moves
  away from it -- a server becomes a cluster by MIGRATE-to-Citus.md, not by the switch. The switch's own rules run
  on a throwaway state directory; the last scenario reads the store that is up.

  Scenario: With nothing chosen, the shape is a server
    Given a state directory with nothing up
    When the shape is asked
    Then it says "shape: server (the default)"
    And it says "up: nothing"

  Scenario: A server that is up is never switched to Citus
    Given a state directory with a server up
    When the shape is set to "citus"
    Then it is refused, naming "MIGRATE-to-Citus.md"
    And the shape is still "server"

  Scenario: A cluster that is up is never switched to a server
    Given a state directory with a Citus cluster up across hosts
    And the shape was set to "citus"
    When the shape is set to "server"
    Then it is refused, naming "take it down or park it first"
    And the shape is still "citus"

  Scenario: With nothing up the switch moves either way, and a parked cluster stays parked
    Given a state directory with nothing up and a Citus cluster parked
    When the shape is set to "citus"
    Then the shape is still "citus"
    When the shape is set to "server"
    Then it says "the parked Citus cluster stays parked; a new server starts empty"
    And the shape is still "server"

  Scenario: The schema matches the shape that is up
    Then the record is local with its foreign key on a server, and distributed by IRI on a cluster
