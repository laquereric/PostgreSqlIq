# @use_case Development/UseCase/UC_Citus
# @facet Store
Feature: Rules compiled into the closure, answers with their proof, and "not modelled" as an answer
  The closure applies owl:propertyChainAxiom and rules held as data (iq.rule) with the vocabulary's own, at query
  time, in the caller's transaction; reasoning.py says why a fact holds, down to the facts as written, and tells a
  question the store has no word for from one whose answer is empty (docs/architecture/ONTOLOGY_ENGINEERING.md
  slice 7, after the article's measurement: rules applied by a model in prose stayed near half right, compiled they
  were always right). Runs on any store: the box's Postgres (local_store.py) as well as the server or the cluster.

  Background:
    Given a release vocabulary: dependsOn transitive, a Package an Artifact, an uncle a parent's brother
    And the rule "release-blocked": a package exposed to an advisory that is critical and open is blocked
    And the stated facts: an app depends on a lib depending on a package, the package and one other exposed to advisories, one critical and open and one critical and closed, a brother and his sister's child

  Scenario: A rule held as data blocks the package whose advisory is critical and open, and not the other
    Then the closure holds the first package blocked, and not the second
    And the scoped closure of the first package says the same
    And its proof is the rule "release-blocked", resting on the three stated facts about its advisory

  Scenario: A property chain derives the uncle, and its proof names the chain and the two facts it went through
    Then the closure holds the brother as the child's uncle
    And its proof is the property chain, resting on the brother and the parent facts as stated

  Scenario: The vocabulary's own rules answer with their proofs, down to the facts as written
    Then the app's dependency on the package is proved by the transitive property, through the lib
    And the package being an Artifact is proved by subClassOf, from its being a Package
    And a fact the closure does not hold has no proof

  Scenario Outline: A rule or a chain that cannot be applied soundly is refused, naming why
    When the rule <rule> is put
    Then it is refused "<reason>"

    Examples:
      | rule                                                            | reason            |
      | whose head names a variable its body does not bind               | unsafe_rule       |
      | whose body's atoms share no variable                             | disconnected_rule |
      | with a literal as a subject                                      | bad_rule          |
      | that is a chain of one property                                  | chain_length      |

  Scenario: A question the store has no word for is answered "not modelled", not with an empty list
    Then asking who is "releaseBlocked" answers not modelled
    And asking what is a "Spaceship" answers not modelled
    And asking who is "blocked" with the value "never" answers an empty list, the store modelling it
