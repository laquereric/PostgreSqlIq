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
- **The lag, on the Dashboard (slice 3).** `cdc.lag()` reads `pg_replication_slots` only (never the slot, so a
  running streamer is not disturbed) and reports:
  - `behind`: WAL written since the slot's position. Each cycle now peeks up to the WAL flush position, and when
    that read reaches it with everything acknowledged, it moves the slot there. So an idle box shows no lag, and
    `behind` is genuinely unshipped.
  - `held`: WAL kept for the slot, against the cap and `safe_wal_size`.
  - the snapshot, and the last cycle (`~/.iq/cdc/cycle.json`: when, server up or down, sent).
  - a health word: `ok`, `behind` (WAL unshipped and no cycle in 5 minutes), `snapshot incomplete`, `at risk`
    (unreserved, or over half the cap), `lost`, or `no slot`.

  The Dashboard's Agents tab shows it as a `cdc` leaf beside the cost meter (green when ok, red when it needs a
  hand), with a detail panel (`UC_Dashboard/tabs/agents`: `cdc_lag()`, loaded under its own module name).
  `iq-cdc.sh status` prints the same health.
- **Only some rows (slice 4).** `iq-cdc.sh filter <table> "<where>"` sets a publication row filter. `--clear`
  removes it, `--reset` goes back to every table in `public`, and with no arguments it lists the filters.
  - **Named tables.** Postgres attaches a filter only to a table named in the publication, and filters across
    publications are OR'ed. So the first filter turns `iq_cdc` from "every table in `public`" into the tables it
    covers now. A new table is then not captured until `--reset`, and the Dashboard panel says so.
  - **Rows crossing the filter.** An update that moves a row out of the filter ships as a delete, and one that
    moves it back in ships as an insert. This is Postgres's own rewrite.
  - **The snapshot applies the same filter.**
  - **A filter never breaks a writer.** Postgres refuses every UPDATE and DELETE on a table whose filter names a
    column outside the replica identity, and the refusal comes at write time, so a live writer would break. Each
    change is therefore proved in its own transaction: the publication change plus a zero-row UPDATE and DELETE,
    which Postgres checks before touching any row. If refused, it is rolled back with Postgres's message.
    `--full` sets `REPLICA IDENTITY FULL` to allow any column (and makes a delete carry the whole old row). An
    expression Postgres will not filter on (`random()`, for example) is refused the same way.

- **Only some columns (slice 5).** `iq-cdc.sh columns <table> a,b,c` sets a publication column list, and
  `--clear` removes it. A column that is not published never leaves the box: pgoutput does not send it, the
  snapshot does not select it, and the IdentityMap is never asked to tokenize it. The list must include the
  replica identity. One without it would make Postgres refuse every UPDATE and DELETE on the table, so it is proved
  and rolled back like a filter. Filters and column lists are read back from `pg_publication_rel` (no list is
  distinct from a list naming every column, whose later columns are not published) and rewritten together, so
  setting one leaves the other as it is.

  Neither is applied live: the box's publication is still every table in `public`.
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

`features/cdc.feature`: 39 scenarios (40 with slice 6's COPY scenario), all passing in about 2.5 s (12 for the stream, 7 for the snapshot plus 8
xid-visibility examples, 1 for credentials, 3 for the lag including the Agents tab's value, 6 for row filters, 4 for column lists), on a scratch pgembed cluster with the relay
in-process (nothing reaches the IQ Server):

    /Applications/IQ.app/Contents/Resources/python/bin/python3 -m behave features

Lag live (2026-10-17): before a cycle under the new code, the Dashboard read `behind`, 6.7 MB. After one
`iq-cdc.sh once` it read `ok`, 8 kB behind. The panel was rendered in the IQ app's Dashboard card (pointed at a
scratch dashboard on :8031, then back to :8030).

**Limits.** In a table with no primary key, identical rows have the same content and so one snapshot record.

**The lake's catalog, replayed into the global cluster (slice 6, 2026-10-19).** The data plane's Gold is a DuckLake
(`GherkinIq/Facet/Store/lake.py`). Its catalog lives in the User/Group domain's local Postgres and reaches the
global Postgres cluster by CDC, and BI (UC_Superset) attaches the replica read-only. Three pieces:

- `cdc.publish_schema(dsn, "ducklake")` adds the schema to `iq_cdc` with every table `REPLICA IDENTITY FULL`.
  An event trigger (`iq_cdc_full_identity`) does the same for tables created there later. DuckLake's catalog
  tables mostly have no key, and Postgres refuses UPDATE and DELETE on a published table without an identity, so
  without this the lake's own writer would break. The lake writes with data inlining off, so rows go to Parquet and
  never into the catalog.
- `apply.py` is the far side. It replays one schema's records into a target schema: the latest snapshot as the base,
  then the stream past a watermark (`iq_cdc_apply.applied`), one target transaction per source transaction. An
  update or delete matches its whole old row, one row. CDC tokenizes every e-mail in a value, so a min/max statistic
  carrying a token no longer bounds its file; the applier writes it as NULL, so the reader scans that file rather
  than pruning it. `prepare_lake` has DuckLake create the target's catalog tables, then empties them.
- **A fix to the stream:** a COPY's rows share one WAL record and so one LSN. They all got the same operationId,
  and every row but one was dropped as "already held". The decoder now numbers rows under an LSN (`seq`), and the
  second and later rows' keys include it. A first row keeps the key it always had, so records already shipped
  keep theirs. DuckLake writes its catalog with COPY, which is how this came to light.

`features/lake_apply.feature` covers it in 6 scenarios: the box's scratch Postgres with a real slot, the relay
in-process, a second scratch Postgres as the global cluster, and DuckLake reading the replica read-only, as
Superset does. Plus `cdc.feature` "Every row of a COPY reaches the server", which fails on the old key. 46
scenarios in all. Not live: the box's publication does not include `ducklake` yet, and nothing runs `apply.py`
against the global cluster.

**Next.** Run `apply.py` where it reaches both the blobs and the global cluster. Narrow its read of
`auth/events/` to the days past the watermark, because today it lists the whole prefix.
