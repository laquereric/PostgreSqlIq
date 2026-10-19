"""UC_CDC scenarios: a scratch pgembed Postgres for the run (never ~/.iq/pg) and an in-process relay per scenario
(UC_LaptopCloudBridge's EventRelay over a dict), so nothing reaches the IQ Server.

@use_case Development/UseCase/UC_CDC
@facet Gherkin
@status Proposed
"""
import os
import sys
import tempfile
from pathlib import Path

UC = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(UC))
sys.path.insert(0, str(UC.parents[2] / "Development/UseCase/UC_LaptopCloudBridge"))   # the steps build the relay and identity map

import cdc  # noqa: E402


def before_all(context):
    import pgembed
    context.pgdata = tempfile.mkdtemp(prefix="iqcdc")
    context.srv = pgembed.get_server(context.pgdata, cleanup_mode=None)
    context.srv.ensure_pgdata_inited()
    context.srv.ensure_postgres_running()
    context.dsn = "postgresql://postgres@/postgres?host=" + context.pgdata
    context.uc = UC


def after_all(context):
    import shutil
    import subprocess
    import pgembed
    try:
        cdc.teardown(context.dsn)
    except Exception:
        pass
    # the scenarios restart the cluster, so pgembed's handle no longer knows the postmaster: stop it by its data dir
    pg_ctl = os.path.join(os.path.dirname(pgembed.__file__), "pginstall", "bin", "pg_ctl")
    subprocess.run([pg_ctl, "-D", context.pgdata, "-m", "fast", "-w", "stop"], capture_output=True)
    shutil.rmtree(context.pgdata, ignore_errors=True)


def after_scenario(context, scenario):
    c = getattr(context, "conn", None)
    if c is not None:
        c.close()
        context.conn = None
