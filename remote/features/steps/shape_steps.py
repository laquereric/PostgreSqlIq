# @use_case Development/UseCase/UC_Citus
# @facet Store
"""The shape switch (deploy-citus.sh shape) on a throwaway HOME, and the schema against the store that is up."""
import json
import os
import subprocess
import tempfile
from pathlib import Path

from behave import given, then, when

DEPLOY = Path(__file__).resolve().parents[2] / "deploy-citus.sh"


def _state(context, topology=None, parked=False):
    context.home = tempfile.mkdtemp(prefix="iq-shape.")
    st = Path(context.home) / ".iq/citus"
    st.mkdir(parents=True)
    if topology:
        (st / "topology.json").write_text(json.dumps({"mode": topology, "coordinator": {"host": "192.0.2.1"}}))
    if parked:
        (st / "topology-parked.json").write_text(json.dumps({"mode": "hosts"}))


def _run(context, *args):
    env = dict(os.environ, HOME=context.home)
    env.pop("SHAPE", None)
    context.out = subprocess.run([str(DEPLOY), "shape", *args], env=env, capture_output=True, text=True)


@given("a state directory with nothing up")
def _nothing(context):
    _state(context)


@given("a state directory with a server up")
def _server(context):
    _state(context, "server")


@given("a state directory with a Citus cluster up across hosts")
def _cluster(context):
    _state(context, "hosts")


@given("a state directory with nothing up and a Citus cluster parked")
def _parked(context):
    _state(context, parked=True)


@given('the shape was set to "{shape}"')
def _was(context, shape):
    (Path(context.home) / ".iq/citus/shape").write_text(shape + "\n")


@when("the shape is asked")
def _ask(context):
    _run(context)


@when('the shape is set to "{shape}"')
def _set(context, shape):
    _run(context, shape)


@then('it says "{text}"')
def _says(context, text):
    assert text in context.out.stdout, context.out.stdout + context.out.stderr


@then('it is refused, naming "{text}"')
def _refused(context, text):
    assert context.out.returncode == 1 and text in context.out.stderr, (context.out.returncode, context.out.stderr)


@then('the shape is still "{shape}"')
def _still(context, shape):
    _run(context)
    assert context.out.stdout.splitlines()[0].startswith("shape: " + shape), context.out.stdout


@then("the record is local with its foreign key on a server, and distributed by IRI on a cluster")
def _schema(context):
    with context.cluster.connect(autocommit=True) as c:
        citus = c.execute("SELECT count(*) FROM pg_extension WHERE extname = 'citus'").fetchone()[0]
        fk = c.execute("SELECT count(*) FROM pg_constraint WHERE conname = 'quad_of_datum'").fetchone()[0]
        dist = None
        if citus:
            dist = c.execute("SELECT column_to_column_name(logicalrelid, partkey) FROM pg_dist_partition"
                             " WHERE logicalrelid = 'iq.datum'::regclass").fetchone()
    assert fk == 1, "no quad_of_datum"
    if context.cluster.mode == "server":
        assert citus == 0, "Citus loaded on the server shape"
    else:
        assert dist and dist[0] == "iri", dist
    context.measures["the record on this shape"] = "%s: %s" % (
        context.cluster.mode, "local, its foreign key kept" if not citus else "distributed by iri")
