# CDC: the box's Postgres streamed to the IQ Server while it is up

@use_case Development/UseCase/UC_CDC
@facet Store
@status Proposed

**The question** (`README.md`, the research note): Postgres does CDC natively from its WAL, and a large set of
tools is built on that (Debezium, Sequin, PeerDB, ...). Which does IQ need to stream the box's own Postgres (the
store standard, pgembed at `~/.iq/pg`, `IQ_REGISTRY_DSN`) to the IQ Server (memory.intentionalquality.com)?

**The answer (slice 1): none of the tools.** The README's table puts "events into queues or webhooks without
Kafka" at Sequin, but IQ already has the destination: the [[UC_LaptopCloudBridge]] relay (`POST /events`,
`/events/have` on memory.*). So `cdc.py` uses only what Postgres has built in:

- **logical decoding** (`wal_level = logical`) through **pgoutput**. The image's pgembed ships pgoutput and neither
  wal2json nor test_decoding, so `cdc.py` decodes pgoutput protocol 1 itself (about 100 lines);
- a **publication** `iq_cdc` (default: every table in `public`, new tables included);
- a **replication slot** `iq_cdc` as the cursor, read over plain SQL (`pg_logical_slot_peek_binary_changes`).
  There is no replication connection and no psycopg2.

**The guarantees.**

- **Only while the IQ Server is up.** Each cycle asks `/whoami` with the paired credential
  (`~/.iq/harness-token`). Anything but a 200 reads nothing and advances nothing, so the WAL waits in the slot.
- **Nothing is lost.** The streamer peeks and never gets. It advances the slot (`pg_replication_slot_advance`) only
  past transactions whose every record the server has acknowledged. A refusal halfway through a batch keeps the
  rest of the transaction in the slot.
- **Nothing is doubled.** A record's operationId is `iq:op:` + sha256(system id, change LSN), so a change resent
  after a crash between the ack and the advance has the same canonical key, and `/events/have` drops it.
- **The person's identity never leaves the box.** Records go through the bridge Flusher's de-identification:
  `subject` is dropped, and `subject_iri` and any `iq:identity:` value are tokenized by the IdentityMap. In
  addition, any e-mail address in a captured value becomes the token of its identity. If no token can be had,
  nothing ships.
- **The disk is protected from an idle slot.** Setup sets `max_slot_wal_keep_size = 1GB`. Past that, Postgres
  invalidates the slot rather than fill the disk, and `status` and `run` say so.
- **The rows that were there first (slice 2).** The slot captures from its creation on. `iq-cdc.sh snapshot`
  ships what existed before, in line with the stream:
  1. it marks the snapshot running, so the streamer pauses;
  2. it drains the slot;
  3. it reads every published table in one `REPEATABLE READ` transaction, recording that transaction's
     `pg_current_snapshot()`, and ships each row as `cdc:snapshot`;
  4. it marks the snapshot done in `~/.iq/cdc/snapshot.json`.

  From then on the streamer advances over any transaction visible in that snapshot (it committed after the drain
  but before the snapshot, so its rows are already in), and ships the rest. Nothing is shipped twice and nothing is
  missed. A snapshot row's operationId is a function of its content, so a cut-short snapshot re-run ships only
  what the server lacks, and a re-run over unchanged rows ships nothing. Teardown forgets the snapshot along with
  the slot.
- **A rotated token does not stop it.** Each probe tries `$IQ_HARNESS_SESSION`, then re-reads
  `~/.iq/harness-token`, and the cycle ships with the first credential `/whoami` accepts.
- **Never autostarted.** There is no launchd job and no app hook. A scenario checks that nothing under UC_App,
  DeployIq, scripts or any plist names it.

**A record** (svc `iq-cdc`, so it lands at `auth/events/dt=<day>/svc=iq-cdc/<opid>.json` in iq-blobs, beside the
bridge's logs and swept by the same `md_observe`):

    {operation_id, op: cdc:insert|update|delete|truncate|snapshot, decision: captured, service: iq-cdc, at (commit, UTC),
     host, table, lsn, commit_lsn, xid, new, old}

Values are typed by the column's type: integers, floats and booleans become numbers and booleans, json and jsonb
are parsed, and everything else is Postgres's text. A delete's or update's `old` is the replica identity: the key
by default, or the whole row under `REPLICA IDENTITY FULL`.

**Use** (`iq-cdc.sh`, on the image's Python, against the box's Postgres):

    iq-cdc.sh setup [schema.table ...] [--restart]   # once; --restart lets it restart Postgres for wal_level
    iq-cdc.sh snapshot                               # once after setup: the rows that were there first
    iq-cdc.sh status                                 # slot position, WAL held, server up/down
    iq-cdc.sh once                                   # one cycle
    iq-cdc.sh run [--interval 10]                    # stream in the foreground until Ctrl-C
    iq-cdc.sh teardown                               # drop the slot + publication (stops holding WAL)

**Live (2026-10-17).** Setup was run on `~/.iq/pg` with `--restart`. The in-app gate reconnected by itself. A
gate journal row (`cpcp_operations`) was shipped, its actor arrived as an `iq:subj:` token, and `/events/have`
confirmed the server holds `auth/events/dt=2026-10-17/svc=iq-cdc/<opid>.json`. Against an unreachable server,
the cycle reported down and advanced nothing. Snapshot: the first run shipped all 550 `cpcp_operations` rows,
then got 401s. The re-run found those 550 on the server, shipped the 9 it lacked (`scrape_*` and new journal
rows), and finished (`2444:2444:` at 0/20643A8).

**Found.**
- The first live snapshot stopped on a 401. This shell's `$IQ_HARNESS_SESSION` held an older token that had
  expired, while `~/.iq/harness-token` had already been rotated, and the env copy was preferred. Every probe now
  falls back to the file.
- pgoutput's key-only old tuple (`K`) carries every column, with nulls for the non-key ones. Only the key is
  claimed.
- After `pg_ctl restart`, pgembed's handle no longer knows the postmaster, so its cleanup leaves it running. The
  scenarios stop it by its data directory.

`features/cdc.feature`: 26 scenarios, all passing in about 2 s (12 for the stream, 7 for the snapshot plus 8
xid-visibility examples, 1 for credentials), on a scratch pgembed cluster with the relay
in-process (nothing reaches the IQ Server):

    /Applications/IQ.app/Contents/Resources/python/bin/python3 -m behave features

**Limits.** In a table with no primary key, identical rows have the same content and so one snapshot record.

**Next.** A table filter by
publication row filter rather than whole tables; the slot's lag on the Dashboard.
