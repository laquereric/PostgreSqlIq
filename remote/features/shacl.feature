# @use_case Development/UseCase/UC_Citus
# @facet Store
Feature: SHACL shapes enforced at commit, as GraphDB enforces them
  GraphDB (RDF4J's SHACL engine) checks each transaction at commit and refuses writes that break the shapes. Postgres
  has no SHACL engine yet (pgRDF's is a stub until its 0.4), so IQ's own shapes -- derived from LinkML for every
  facet -- are compiled to rows on every node and checked in SQL when the transaction commits, before any worker
  prepares. These scenarios hold the cluster to the Store facet's shapes, and to UC_OntologyPipeline's validator.

  Background:
    Given the Citus cluster on Vultr
    And the platform is put as datums, its vocabulary on every node
    And the Store facet's shapes are loaded

  Scenario: IQ's LinkML shapes compile, and what is not supported is refused by name
    Then 9 of the 11 shapes graphs compile
    And the ScapeShape shapes are refused for "sh:or" and the MarkovKernel shapes for "sh:sparql"

  Scenario: A Claim that conforms commits
    When a Claim "test:claim-ok" with every required property is committed
    Then "test:claim-ok" answers

  Scenario Outline: A Claim that breaks its shape is refused at COMMIT, and nothing of it lands
    When a Claim "test:claim-bad" broken by <change> is committed
    Then the commit is refused by the shapes, naming "<path>" for "<constraint>"
    And nothing of "test:claim-bad" answers

    Examples:
      | change                                | path         | constraint |
      | no subject_iri                        | subject_iri  | minCount   |
      | two objects                           | object       | maxCount   |
      | a confidence that is not a float      | confidence   | datatype   |
      | a trust_tier off its list             | trust_tier   | in         |
      | an IRI for its source_span            | source_span  | nodeKind   |
      | a property its closed shape does not name | test:extra | closed   |

  Scenario: An instance of a subclass is held to its superclass's shape
    Given the vocabulary makes "test:AuditClaim" a subclass of Claim
    When an AuditClaim "test:audit-1" with no subject_iri is committed
    Then the commit is refused by the shapes, naming "subject_iri" for "minCount"

  Scenario: sh:class is checked across datums, inside the transaction
    When one transaction commits a Blob "test:blob-1" and a FileAt "test:file-1" whose blob is it
    Then "test:blob-1" answers
    And "test:file-1" answers
    When a FileAt "test:file-2" whose blob is "test:file-1" is committed
    Then the commit is refused by the shapes, naming "blob" for "class"

  Scenario: Taking a required triple away is refused too
    Given a Claim "test:claim-ok" with every required property is committed
    When "test:claim-ok" is put again without its object
    Then the commit is refused by the shapes, naming "object" for "minCount"
    And "test:claim-ok" still has its object

  Scenario: Across workers, one datum that breaks its shape keeps the other out
    Given two Claims on different workers, the second with no subject_iri
    When one transaction commits both
    Then the commit is refused by the shapes, naming "subject_iri" for "minCount"
    And neither Claim answers

  Scenario: A changed shape is enforced from the next commit
    When the shapes make a Claim's extractor required
    And a Claim "test:claim-ok" with every required property is committed
    Then the commit is refused by the shapes, naming "extractor" for "minCount"

  Scenario: An agent cannot write around the shapes
    Then as the agent role a triple written straight into the record is denied
    And as the agent role a Claim with no subject_iri is refused by the shapes
    And as the agent role a Claim "test:agent-ok" with every required property commits

  Scenario: The cluster and UC_OntologyPipeline's validator agree, violation for violation
    When 200 datums generated from the Store shapes, most broken one way, are checked by the cluster inside a transaction
    And the same datums are checked by UC_OntologyPipeline's validator
    Then the two reports are the same
    And the report has violations of every kind: minCount, maxCount, datatype, class, in, nodeKind, closed
