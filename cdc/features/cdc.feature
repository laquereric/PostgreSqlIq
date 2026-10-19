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

  Scenario: Every row of a COPY reaches the server, though one WAL record carries them all
    Given CDC is set up on a fresh notes table
    And the IQ Server is up
    When notes 1 to 4 are copied in
    And the streamer runs one cycle
    Then the server holds 4 records of op cdc:insert
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

  Scenario: The snapshot ships the rows that were there before the slot, typed as the stream types them
    Given a notes table holding 3 notes before CDC is set up
    And the IQ Server is up
    When the snapshot runs
    Then it is done with 3 rows of public.notes
    And the server holds 3 records of op cdc:snapshot, with tags as JSON and no identity
    When note 20 is inserted
    And the streamer runs one cycle
    Then the server holds 1 records of op cdc:insert

  Scenario: A transaction committed between the drain and the snapshot is in the rows, and not streamed again
    Given a notes table holding 2 notes before CDC is set up
    And the IQ Server is up
    When note 50 is inserted
    And the snapshot runs with note 60 inserted between the drain and the snapshot
    Then the server holds 1 records of op cdc:insert
    And the server holds 4 records of op cdc:snapshot
    When the streamer runs one cycle
    Then the cycle passed over 1 change already in the snapshot and sent 0
    And the server holds 1 records of op cdc:insert
    And the slot has nothing left
    When note 70 is inserted
    And the streamer runs one cycle
    Then the server holds 2 records of op cdc:insert

  Scenario: A down server takes no snapshot
    Given a notes table holding 2 notes before CDC is set up
    And the IQ Server is down
    When the snapshot runs
    Then the snapshot says the server is down and left no state

  Scenario: A snapshot cut short pauses the stream; the re-run ships only what is missing
    Given a notes table holding 3 notes before CDC is set up
    And the IQ Server is up but refuses the 2rd batch
    And batches of 2 records
    When the snapshot runs
    Then the snapshot failed at its rows and the state is running
    When note 30 is inserted
    And the streamer runs one cycle
    Then the cycle is paused for the snapshot
    When the snapshot runs
    Then it is done with 4 rows of public.notes
    And the server holds 4 records of op cdc:snapshot
    And the server holds 1 records of op cdc:insert
    And the slot has nothing left

  Scenario: Running a finished snapshot again ships nothing new
    Given a notes table holding 3 notes before CDC is set up
    And the IQ Server is up
    When the snapshot runs
    And the snapshot runs
    Then the snapshot sent 0 and found 3 already there

  Scenario: Teardown forgets the snapshot with the slot
    Given a notes table holding 1 notes before CDC is set up
    And the IQ Server is up
    When the snapshot runs
    And CDC is torn down
    Then there is no snapshot state

  Scenario Outline: A streamed transaction is in the snapshot when the snapshot could see it
    Then xid <xid> is <seen> in snapshot "<snapshot>"

    Examples:
      | xid        | snapshot                      | seen    |
      | 740        | 750:760:752,755               | visible |
      | 752        | 750:760:752,755               | hidden  |
      | 753        | 750:760:752,755               | visible |
      | 760        | 750:760:752,755               | hidden  |
      | 4294967290 | 4294967300:4294967310:        | visible |
      | 5          | 4294967300:4294967310:        | visible |
      | 15         | 4294967300:4294967310:        | hidden  |
      | 4294967295 | 8589934590:8589934600:        | visible |

  Scenario: A stale token in the environment does not keep a rotated pairing down
    Given the environment holds an expired token and the paired file a live one
    When the server is probed
    Then it is up, and the transport and the tokenizer carry the file's token

  Scenario: An idle cycle stands the slot at the WAL's end, so behind means unshipped
    Given CDC is set up on a fresh notes table
    And the IQ Server is up
    When a table outside the publication takes 500 rows
    And the streamer runs one cycle
    Then the cycle advanced with nothing to send
    And the lag is ok and behind by less than 1 kB

  Scenario: The lag names what needs a hand
    Given CDC is set up on a fresh notes table
    And the IQ Server is up
    When note 40 is inserted
    And no cycle has run for 10 minutes
    Then the lag says behind
    When the streamer runs one cycle
    Then the lag is ok and behind by less than 1 kB
    And the lag's last cycle saw the server up and sent 1
    When the snapshot is left running
    Then the lag says snapshot incomplete
    When the snapshot runs
    Then the lag is at risk once the slot may hold only 1MB and a table outside the publication takes 20000 rows
    When CDC is torn down
    Then the lag says no slot

  Scenario: The Dashboard's Agents tab shows the same lag
    Given CDC is set up on a fresh notes table
    And the IQ Server is up
    When note 41 is inserted
    And the streamer runs one cycle
    Then the Agents tab's cdc value has the slot, the health and the last cycle

  Scenario: A row filter on the key ships only the rows it matches
    Given CDC is set up on a fresh notes table
    And the IQ Server is up
    When the filter on public.notes is "id < 100"
    Then the filter is set, and the publication names its tables instead of schema public
    When note 5 is inserted
    And note 500 is inserted
    And the streamer runs one cycle
    Then the server holds 1 records of op cdc:insert
    And the shipped insert is note 5

  Scenario: An update moving a row out of the filter ships as a delete, and back in as an insert
    Given CDC is set up on a fresh notes table
    And the IQ Server is up
    When the filter on public.notes is "body <> 'secret'" with a full replica identity
    And note 8 is inserted
    And note 8's body becomes "secret"
    And note 8's body becomes "public again"
    And the streamer runs one cycle
    Then the server holds 1 records of op cdc:delete
    And the server holds 2 records of op cdc:insert
    And the shipped delete carries the whole old row of note 8

  Scenario: A filter that would break the table's writers is refused, and nothing changes
    Given CDC is set up on a fresh notes table
    When the filter on public.notes is "body <> 'secret'"
    Then the filter is refused naming the replica identity
    And the publication still covers schema public with no filters
    And note 1 can still be inserted, updated and deleted

  Scenario: An expression Postgres will not filter on is refused
    Given CDC is set up on a fresh notes table
    When the filter on public.notes is "id < random() * 10"
    Then the filter is refused
    And the publication still covers schema public with no filters

  Scenario: The snapshot ships only the rows the filter matches
    Given a notes table holding 4 notes before CDC is set up
    And the IQ Server is up
    When the filter on public.notes is "id >= 3"
    And the snapshot runs
    Then it is done with 2 rows of public.notes
    And the server holds 2 records of op cdc:snapshot

  Scenario: Clearing a filter ships every row again; reset returns to schema public
    Given CDC is set up on a fresh notes table
    And the IQ Server is up
    When the filter on public.notes is "id < 100"
    And the filter on public.notes is cleared
    And note 600 is inserted
    And the streamer runs one cycle
    Then the server holds 1 records of op cdc:insert
    When the filters are reset
    Then the publication still covers schema public with no filters

  Scenario: A column list ships only the listed columns -- the rest never leave the box
    Given CDC is set up on a fresh notes table
    And the IQ Server is up
    When the columns of public.notes are "id,body"
    And a note is inserted by "Pat.Doe@Example.com" with actor "iq:identity:d77b333a2e4868e38b54c746" and a column named subject
    And note 5's body becomes "edited"
    And the streamer runs one cycle
    Then every shipped record of public.notes carries only id and body
    And the IdentityMap was asked once, for the e-mail in the body, and never for the unpublished author or actor

  Scenario: A column list without the key is refused, and nothing changes
    Given CDC is set up on a fresh notes table
    When the columns of public.notes are "body,tags"
    Then the column list is refused naming the replica identity
    And the publication still covers schema public with no filters
    And note 1 can still be inserted, updated and deleted

  Scenario: A filter and a column list are kept together
    Given CDC is set up on a fresh notes table
    When the filter on public.notes is "id < 100"
    And the columns of public.notes are "id,body"
    And the filter on public.notes is "id < 50"
    Then the publication has filter "(id < 50)" and columns id, body on public.notes
    When the columns of public.notes are cleared
    Then the publication has filter "(id < 50)" and no column list on public.notes

  Scenario: The snapshot ships only the listed columns
    Given a notes table holding 2 notes before CDC is set up
    And the IQ Server is up
    When the columns of public.notes are "id,tags"
    And the snapshot runs
    Then the server holds 2 records of op cdc:snapshot
    And every shipped record of public.notes carries only id and tags
