# @use_case Development/UseCase/UC_Citus
# @facet Store
Feature: Inference at query time that costs the question, not the graph
  MarkLogic answers rdfs-plus at query time over everything a query could reach. iq.entail() did the same by
  closing the whole graph on every call, so its cost grew with the graph. iq.entail(seeds) closes only what is
  connected to the seeds -- and must answer exactly as the whole closure does about every node it reaches. The
  data plane's SPARQL can now ask with inference, and a query of one pattern runs on the workers.

  Background:
    Given the Citus cluster on Vultr
    And the platform is put as datums, its vocabulary on every node

  Scenario: The scoped closure says what the whole closure says, about every node it reaches
    Then for each competency question's anchor, and for the class Store, the scoped closure agrees with the whole

  Scenario: At scale the scoped closure stays the size of the question while the whole grows with the graph
    When the platform is copied 100, 1000 and 4000 times, each copy its own component
    Then at each size the scoped closure of a copy agrees with the whole closure about every node it reaches
    And the scoped closure of a copy is the same size at every size of the graph
    And at the largest size the scoped closure answers at least 20 times faster than the whole

  Scenario: The data plane's SPARQL answers with inference as MarkLogic does
    Given a data plane on the cluster whose graph answers with inference
    When the platform's Turtle is put on the plane as one graph
    Then every competency question answers through the plane as on MarkLogic
    And without inference the same questions answer empty

  Scenario: A query of one pattern runs on the workers, a join on the coordinator, and both answer as the other way
    Given a data plane on the cluster
    When the platform's Turtle is put on the plane as one graph
    Then every query without OPTIONAL answers the same on the workers as on the coordinator
    And a query of one pattern is planned on the workers, a join pulled to the coordinator
