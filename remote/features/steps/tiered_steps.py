"""Steps for tiered.feature.

@use_case Development/UseCase/UC_Citus
@facet Store
@status Proposed
"""
import datetime as dt
import json
import threading
import time

import psycopg
from behave import given, then, when

import citus_plane as cp

N_PER_MONTH = 500
PGDUCK = "sudo -u postgres /usr/pgsql-17/bin/psql -X -At -h /var/run/iq-pgduck -p 5332 -c %s"


def months_back(k):
    today = dt.date.today().replace(day=1)
    y, m = today.year, today.month - k
    while m <= 0:
        y, m = y - 1, m + 12
    return dt.datetime(y, m, 1, tzinfo=dt.timezone.utc)


def ym(d):
    return d.strftime("%Y%m")


@given("the cold tier on the coordinator")
def step_lake(context):
    ok = context.cluster.admin("SELECT count(*) FROM pg_extension WHERE extname = 'pg_lake_table'")
    assert ok == "1", "no pg_lake on the coordinator: deploy-citus.sh lake-up"
    context.cluster.admin((cp.HERE / "tiered.sql").read_text())


@given("events over the last four months, each month a hot partition")
def step_events(context):
    adm = context.cluster.admin
    adm("DELETE FROM iq.event_cold; " + "".join(
        "DROP TABLE IF EXISTS iq.%s; " % p for p in adm(
            "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
            "WHERE i.inhparent = 'iq.event'::regclass").split()))
    context.months = [months_back(k) for k in (3, 2, 1, 0)]          # oldest first; the last is this month
    adm("".join("SELECT iq.event_month('%s');" % m.isoformat() for m in context.months))
    rows = []
    for m in context.months:
        for i in range(N_PER_MONTH):
            at = m + dt.timedelta(minutes=37 * i)
            rows.append((cp.NS + "test:event-%s-%d" % (ym(m), i), at, "response", json.dumps({"n": i, "month": ym(m)})))
    with context.cluster.connect() as c:
        with c.cursor() as cur:
            with cur.copy("COPY iq.event (iri, at, kind, body) FROM STDIN") as cp_:
                for r in rows:
                    cp_.write_row(r)
        c.commit()
    context.before = fingerprint(context)


def fingerprint(context):
    with context.cluster.connect() as c:
        by_month = c.execute("SELECT to_char(at AT TIME ZONE 'UTC', 'YYYYMM'), count(*) FROM iq.event_all "
                             "GROUP BY 1 ORDER BY 1").fetchall()
        digest = c.execute("SELECT md5(string_agg(iri || at::text || body::text, ',' ORDER BY iri)) FROM iq.event_all").fetchone()[0]
        c.rollback()
    return by_month, digest


def hot_parts(context):
    return sorted(context.cluster.admin(
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'iq.event'::regclass").split())


def cold_months(context):
    return context.cluster.admin("SELECT string_agg(DISTINCT to_char(at AT TIME ZONE 'UTC', 'YYYYMM'), ' ') FROM iq.event_cold").split()


def cold_event(name, at):
    return (cp.NS + "test:" + name, at, "late", json.dumps({"late": True}))


@when("an event is written to the cold tier and {outcome}")
def step_cold_write(context, outcome):
    with context.cluster.connect() as c:
        c.execute("INSERT INTO iq.event_cold (iri, at, kind, body) VALUES (%s, %s, %s, %s)",
                  cold_event("cold-probe", context.months[0]))
        c.commit() if outcome == "committed" else c.rollback()


@then("the cold tier {has} it")
def step_cold_has(context, has):
    with context.cluster.connect(autocommit=True) as c:
        n = c.execute("SELECT count(*) FROM iq.event_cold WHERE iri = %s", (cp.NS + "test:cold-probe",)).fetchone()[0]
    assert n == (1 if has == "has" else 0), n


@then("the cold tier's files on object storage are Iceberg: metadata and Parquet")
def step_files(context):
    loc = context.cluster.admin("SELECT metadata_location FROM iceberg_tables WHERE table_name = 'event_cold'")
    assert loc.startswith("s3://") and loc.endswith(".metadata.json"), loc
    files = context.cluster.host_sh(PGDUCK % "\"SELECT file FROM glob('%s/**')\"" % loc.rsplit("/metadata/", 1)[0])
    kinds = {f.rsplit(".", 1)[-1] for f in files.split()}
    assert {"json", "parquet", "avro"} <= kinds, kinds
    context.measures["cold tier files"] = "%d (%s), metadata %s" % (len(files.split()), ", ".join(sorted(kinds)), loc.split("/")[-1][:24])


def move(context, fail=False):
    cutoff = context.months[2]                                      # every month that ended before last month
    t0 = time.perf_counter()
    try:
        out = context.cluster.admin("SELECT part || ' ' || moved FROM iq.tier_out('%s', %s)" % (cutoff.isoformat(), str(fail).lower()))
        context.move_error = None
    except RuntimeError as e:
        out, context.move_error = "", str(e)
    context.move_s = time.perf_counter() - t0
    return out


@given("every month that ended before last month is moved to the cold tier")
@when("every month that ended before last month is moved to the cold tier")
def step_move(context):
    out = move(context)
    assert context.move_error is None, context.move_error
    context.measures.setdefault("moving 2 months (%d events) hot -> Iceberg, one transaction" % (2 * N_PER_MONTH),
                                "%.1f s" % context.move_s)
    context.moved = out


@then("those months' hot partitions are gone and the cold tier holds their rows")
def step_moved(context):
    old = ["event_p" + ym(m) for m in context.months[:2]]
    assert not set(old) & set(hot_parts(context)), hot_parts(context)
    assert cold_months(context) == [ym(m) for m in context.months[:2]], cold_months(context)


@then("the events by month, and a checksum over every event, are as they were before the move")
def step_same(context):
    assert fingerprint(context) == context.before, (fingerprint(context), context.before)


@when("a move of the old months fails after copying them into Iceberg")
def step_fail(context):
    move(context, fail=True)
    assert context.move_error and "failing on purpose" in context.move_error, context.move_error


@then("every month is still a hot partition and the cold tier holds none of them")
def step_all_hot(context):
    assert hot_parts(context) == ["event_p" + ym(m) for m in context.months], hot_parts(context)
    assert cold_months(context) == [], cold_months(context)


@when("the old months are moved while a reader counts every event, over and over")
def step_race(context):
    stop, counts, errors = threading.Event(), [], []

    def reader():
        with context.cluster.connect(autocommit=True) as c:
            while not stop.is_set():
                try:
                    counts.append(c.execute("SELECT count(*) FROM iq.event_all").fetchone()[0])
                except psycopg.Error as e:
                    errors.append(repr(e)[:200])
    t = threading.Thread(target=reader)
    t.start()
    time.sleep(0.5)
    move(context)
    time.sleep(0.5)
    stop.set()
    t.join()
    context.counts, context.read_errors = counts, errors
    assert context.move_error is None, context.move_error


@then("every count the reader saw was the same")
def step_counts(context):
    assert not context.read_errors, context.read_errors[:3]
    seen = collections_counter(context.counts)
    context.measures["reads during the move: counts seen"] = ", ".join("%d x%d" % kv for kv in sorted(seen.items()))
    assert len(seen) == 1 and next(iter(seen)) == 4 * N_PER_MONTH, seen


def collections_counter(xs):
    import collections
    return collections.Counter(xs)


@when("a late event for the oldest month is written to both tiers as one")
def step_late(context):
    with context.cluster.connect() as c:
        c.execute("INSERT INTO iq.event_all (iri, at, kind, body) VALUES (%s, %s, %s, %s)",
                  cold_event("late-1", context.months[0] + dt.timedelta(days=3)))
        c.commit()


@then("it is in the cold tier and not the hot")
def step_late_where(context):
    with context.cluster.connect(autocommit=True) as c:
        tiers = [r[0] for r in c.execute("SELECT tier FROM iq.event_all WHERE iri = %s", (cp.NS + "test:late-1",))]
    assert tiers == ["cold"], tiers


@then("DuckDB, reading the Iceberg metadata straight from object storage, counts the same events as Postgres")
def step_duckdb(context):
    loc = context.cluster.admin("SELECT metadata_location FROM iceberg_tables WHERE table_name = 'event_cold'")
    duck = context.cluster.host_sh(PGDUCK % "\"SELECT count(*) FROM iceberg_scan('%s')\"" % loc).strip()
    pg = context.cluster.admin("SELECT count(*) FROM iq.event_cold")
    assert duck == pg == str(2 * N_PER_MONTH), (duck, pg)
    context.measures["cold events: DuckDB iceberg_scan / Postgres"] = "%s / %s" % (duck, pg)
