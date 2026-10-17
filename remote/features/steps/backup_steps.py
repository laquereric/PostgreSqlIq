"""Steps for backup.feature.

@use_case Development/UseCase/UC_Citus
@facet Store
@status Proposed
"""
import json
import time

from behave import given, then, when

import citus_plane as cp

DRILL = "/var/lib/pgsql/17/drill"
PG = cp.PG


def stanza(context, node):
    host = context.cluster.host_sh("hostname", node).strip()
    return "%s-%s" % (host, context.cluster.node(node)["unit"])


def nodes(context):
    return [cp.COORD] + sorted(context.cluster.topo["workers"])


@given("every node is backed up to Vultr")
def step_backed(context):
    out = context.cluster.host_sh("command -v pgbackrest >/dev/null && echo yes || echo no").strip()
    if out != "yes":
        context.scenario.skip("no pgBackRest on this cluster (deploy-citus.sh backup-up)")


@then("every node's backup is ok, with a full backup and archived WAL")
def step_info(context):
    seen = []
    for node in nodes(context):
        st = stanza(context, node)
        info = json.loads(context.cluster.host_sh("sudo -u postgres pgbackrest --stanza=%s info --output=json" % st, node))[0]
        fulls = [b for b in info["backup"] if b["type"] == "full"]
        wal = info["archive"][0].get("max") if info["archive"] else None
        assert info["status"]["message"] == "ok" and fulls and wal, (st, info["status"], len(fulls), wal)
        seen.append("%s %d full, WAL to %s" % (context.cluster.name(node), len(fulls), wal[-8:]))
    context.measures["backups"] = "; ".join(seen)


def put_on(context, name, worker):
    """A datum named <name>-<n> that lands on `worker`; returns its IRI."""
    for n in range(500):
        iri = cp.NS + "%s-%d" % (name, n)
        if context.cluster.node_of(iri) == worker:
            with context.cluster.connect() as c:
                cp.put(c, iri, [(iri, cp.RDF + "type", cp.NS + "RecordStore", False)])
                c.commit()
            return iri
    raise AssertionError("no IRI on %s" % worker)


@given('a datum "{name}" committed on the first worker')
def step_before(context, name):
    context.worker = sorted(context.cluster.topo["workers"])[0]
    if context.worker == context.cluster.spare:
        context.worker = sorted(context.cluster.topo["workers"])[1]
    context.before = put_on(context, name, context.worker)


@when("a restore point is made across the cluster")
def step_point(context):
    context.point = "iq-drill-%d" % int(time.time())
    context.cluster.admin("SELECT citus_create_restore_point('%s')" % context.point)
    for node in nodes(context):                                   # the WAL holding the point, archived everywhere
        context.cluster.host_sh("sudo -u postgres pgbackrest --stanza=%s check" % stanza(context, node), node)


@when('a datum "{name}" is committed on the same worker')
def step_after(context, name):
    context.after = put_on(context, name, context.worker)


@when("that worker's backup is restored to the restore point beside it")
def step_restore(context):
    st = stanza(context, context.worker)
    t0 = time.perf_counter()
    context.cluster.host_sh(
        "set -e; rm -rf {d}; install -d -o postgres -g postgres -m 700 {d}; "
        "sudo -u postgres pgbackrest --stanza={st} --pg1-path={d} --type=name --target={rp} --target-action=promote restore; "
        "sudo -u postgres {pg}/pg_ctl -D {d} -l {d}/drill.log -w -t 180 -o \"-p 5499 -c listen_addresses=127.0.0.1 "
        "-c archive_mode=off\" start >/dev/null; "
        # pg_ctl returns once reads are served, still replaying towards the point: wait for the promotion (pg_search
        # Community also refuses reads while a server is in recovery)
        "for _ in $(seq 600); do [ \"$(sudo -u postgres {pg}/psql -X -At -p 5499 -d postgres -c 'SELECT pg_is_in_recovery()')\" = f ] "
        "&& exit 0; sleep 1; done; exit 1".format(d=DRILL, st=st, rp=context.point, pg=PG), context.worker, timeout=900)
    context.measures["drill: a worker restored to a restore point and started"] = "%.0f s" % (time.perf_counter() - t0)
    context.add_cleanup(lambda: context.cluster.host_sh(
        "sudo -u postgres %s/pg_ctl -D %s -m fast stop >/dev/null 2>&1; rm -rf %s" % (PG, DRILL, DRILL), context.worker, check=False))


@then('the restored worker holds "{a}" and not "{b}"')
def step_holds(context, a, b):
    def held(iri):
        shard = context.cluster.admin("SELECT get_shard_id_for_distribution_column('iq.datum', '%s'::text)" % iri)
        return context.cluster.host_sh(
            "sudo -u postgres %s/psql -X -At -p 5499 -d iq -c \"SELECT count(*) FROM iq.datum_%s WHERE iri = '%s'\""
            % (PG, shard, iri), context.worker).strip()
    assert held(context.before) == "1", "the restored worker lost what was committed before the point"
    assert held(context.after) == "0", "the restored worker has what was committed after the point"
    live = context.cluster.node_of(context.after)
    assert live == context.worker
