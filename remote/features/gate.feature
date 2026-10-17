# @use_case Development/UseCase/UC_Citus
# @facet Agent
Feature: The gate is the cluster's only writer
  An agent reaches the record only through IQ's CpCp gate: every put, get and query is a push to the gate first,
  and the one application role that can write (iq_gate) is held by the gate's writer, not by the agent. These
  scenarios run the real Rust gate (cpcp-gate) with this use case's policy, as UC_Progress's gated demo does.

  Background:
    Given the Citus cluster on Vultr
    And a gate with the cluster's writing policy
    And the gate's writer on the cluster

  Scenario: An agent's own role writes nothing, not even through the governed write
    Then as the agent role a triple written straight into the record is denied
    And as the agent role the governed write itself is denied
    And only the cluster's owner and the gate's writer may call the governed write

  Scenario: A put the gate admits lands, carrying the gate's operation
    When the writer puts the value {"label": "vocab"} as "iq:onto:demo:graph:vocab@1" at "Silver"
    Then the put is ok
    And the datum on the cluster carries the operation the gate admitted

  Scenario: A put the gate refuses writes nothing
    When the writer puts the value {"journey": "signup"} as "iq:ux:journey:signup" at "Silver"
    Then the put is refused as "not_admitted: no_permit"
    And the cluster holds nothing at "iq:ux:journey:signup"
    And the gate's journal has the refusal

  Scenario: Admitted by the gate, refused by the shapes: nothing lands
    Given the Store facet's shapes are loaded
    When the writer puts a Claim with no subject_iri as "iq:onto:demo:claim:bad@1" at "Silver"
    Then the put fails on the shapes
    And the cluster holds nothing at "iq:onto:demo:claim:bad@1"

  Scenario: Every datum the writer wrote was admitted by the gate, and every admitted put is on the cluster
    When the writer puts 10 graphs and is refused 3 journeys
    Then the cluster agrees with the gate's journal
