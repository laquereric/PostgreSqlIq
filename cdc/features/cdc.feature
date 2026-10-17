# @use_case Development/UseCase/UC_CDC
# @facet Gherkin
Feature: The box's Postgres streamed to the IQ Server, only while it is up
  A change committed in the box's Postgres is read from the WAL through a logical slot (pgoutput), and leaves the
  slot only once the IQ Server holds it. While the server is down nothing is read and the WAL waits. Every change
  crosses as a de-identified record on the bridge's relay. Nothing starts the streamer but its user.

  Scenario: Setup makes Postgres capturable
    Given a scratch Postgres with wal_level replica
    When CDC is set up with restart allowed
    Then wal_level is logical and the slot WAL cap is 1GB
    And the publication covers schema public and the slot iq_cdc exists

  Scenario: Setup without restart says what it needs
    Given a scratch Postgres with wal_level replica
    When CDC is set up without restart
    Then setup answers restart_needed

  Scenario: One transaction's insert, update and delete reach the server typed, and the slot moves past them
    Given CDC is set up on a fresh notes table
    And the IQ Server is up
    When one transaction inserts note 1, updates it and deletes note 2
    And the streamer runs one cycle
    Then the server holds 3 records of service iq-cdc
    And they are cdc:insert, cdc:update, cdc:delete on public.notes in order
    And the inserted record has id 1 as a number and its tags as JSON
    And the slot has nothing left

  Scenario: A down server reads nothing and advances nothing; the change ships when it is up
    Given CDC is set up on a fresh notes table
    And the IQ Server is down
    When note 7 is inserted
    And the streamer runs one cycle
    Then the cycle says the server is down
    And the server holds 0 records of service iq-cdc
    And the slot still holds the change
    When the IQ Server comes up
    And the streamer runs one cycle
    Then the server holds 1 records of service iq-cdc
    And the slot has nothing left

  Scenario: A refused batch keeps the change in the slot; the retry lands it once
    Given CDC is set up on a fresh notes table
    And the IQ Server is up but refuses the next 1 batches
    When note 3 is inserted
    And the streamer runs one cycle
    Then the cycle names an error and advanced nothing
    And the slot still holds the change
    When the streamer runs one cycle
    Then the server holds 1 records of service iq-cdc
    And the slot has nothing left

  Scenario: Acknowledged but not advanced (a crash) -- the resend is recognised, not duplicated
    Given CDC is set up on a fresh notes table
    And the IQ Server is up
    When note 4 is inserted
    And the server receives the change but the slot is not advanced
    And the streamer runs one cycle
    Then the cycle sent 0 and found 1 already there
    And the server holds 1 records of service iq-cdc
    And the slot has nothing left

  Scenario: A failure mid-stream advances only past whole transactions the server holds
    Given CDC is set up on a fresh notes table
    And the IQ Server is up but refuses the 3rd batch
    And batches of 2 records
    When 4 transactions each insert 3 notes
    And the streamer runs one cycle
    Then the cycle sent 4
    And the slot advanced to the end of the first transaction
    When the streamer runs one cycle
    Then the server holds 12 records of service iq-cdc

  Scenario: Who never leaves the box
    Given CDC is set up on a fresh notes table
    And the IQ Server is up
    When a note is inserted by "Pat.Doe@Example.com" with actor "iq:identity:d77b333a2e4868e38b54c746" and a column named subject
    And the streamer runs one cycle
    Then the server holds 1 records of service iq-cdc
    And no stored record contains "@example.com" or "iq:identity:" or a subject key
    And the e-mail became the token of its identity

  Scenario: A token that cannot be had ships nothing
    Given CDC is set up on a fresh notes table
    And the IQ Server is up but the IdentityMap cannot be reached
    When a note is inserted by "pat@example.com"
    And the streamer runs one cycle
    Then the cycle names an error and advanced nothing
    And the server holds 0 records of service iq-cdc

  Scenario: Teardown stops holding WAL
    Given CDC is set up on a fresh notes table
    When CDC is torn down
    Then there is no slot iq_cdc and no publication iq_cdc

  Scenario: Nothing starts the streamer
    Then UC_CDC has no launchd job and no IQ app or service script starts iq-cdc
