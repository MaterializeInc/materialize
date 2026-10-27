# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
E2E browser tests for the dataflow visualizer React components.
Tests the /memory, /hierarchical-memory, and /metrics-viz endpoints on port 6878.
"""

import time

from materialize import MZ_ROOT
from materialize.buildkite import is_in_buildkite, upload_artifact
from materialize.mzcompose.composition import Composition
from materialize.mzcompose.service import Service
from materialize.mzcompose.services.materialized import Materialized

# The cluster replica the tests visualize. `quickstart` holds the fixture index
# below and little else, which keeps the introspection queries behind the
# visualizer an order of magnitude cheaper than on `mz_catalog_server`, where
# they scan every builtin dataflow.
FIXTURE_CLUSTER = "quickstart"
FIXTURE_REPLICA = "r1"

# The index whose dataflow the tests expand. Without it they would have to pick
# whatever sits at the top of the dataflow table, and that is a moving target:
# the table is ordered by record count descending, a dataflow that arranges
# nothing reports NULL rather than zero there, and NULLs sort first. Those are
# the transient `introspection-subscribe-*` dataflows and the storage
# `command_sequencer`. A transient one can be gone by the time the page queries
# its operators, and the page then reports an unknown dataflow id.
#
# The index key is load-bearing as well. An arrangement embeds the debug
# formatting of its key in its operator name, so indexing a named column yields
# `ArrangeBy[[Column(0, "id")]]`. Those double quotes have to survive into a DOT
# label, which is what the quoted-name test asserts.
#
# So is indexing a view rather than the table directly: the /memory page shows
# the SQL that created a dataflow by resolving the index back to a view, and
# that lookup finds nothing for an index on a table.
FIXTURE_TABLE = "visualizer_fixture"
FIXTURE_VIEW = "visualizer_fixture_view"
FIXTURE_INDEX = "visualizer_fixture_idx"

# A second indexed view whose name carries both quote kinds: the /memory page
# only renders its SHOW CREATE VIEW panel if it escapes what it interpolates.
# The index name stays plain so the page's `Dataflow: <db>.<schema>.<index>`
# parse still succeeds.
FIXTURE_QUOTED_VIEW = "visualizer_quoted\"_'_view"
FIXTURE_QUOTED_INDEX = "visualizer_quoted_idx"

SERVICES = [
    Materialized(),
    Service(
        "playwright",
        {
            "mzbuild": "playwright",
            "volumes": [
                ".:/workdir",
            ],
            "environment": [
                "MZ_HOST=materialized",
                f"FIXTURE_CLUSTER={FIXTURE_CLUSTER}",
                f"FIXTURE_REPLICA={FIXTURE_REPLICA}",
                f"FIXTURE_VIEW={FIXTURE_VIEW}",
                f"FIXTURE_INDEX={FIXTURE_INDEX}",
                f"FIXTURE_QUOTED_VIEW={FIXTURE_QUOTED_VIEW}",
                f"FIXTURE_QUOTED_INDEX={FIXTURE_QUOTED_INDEX}",
            ],
        },
    ),
]


def workflow_default(c: Composition) -> None:
    """Run dataflow visualizer E2E tests"""
    c.up("materialized")
    create_fixture_dataflow(c)
    try:
        c.run("playwright", "/workdir/run-tests.sh")
    finally:
        # Upload Playwright traces if they exist (created on test failure)
        traces_path = (
            MZ_ROOT / "test" / "dataflow-visualizer" / "playwright-traces.tar.gz"
        )
        if traces_path.exists() and is_in_buildkite():
            upload_artifact(traces_path)


def create_fixture_dataflow(c: Composition) -> None:
    """Install the index the browser tests visualize, and wait for its dataflow.

    The DDL runs over pgwire as `materialize`. The visualizer's own `/api/sql`
    endpoint on the internal HTTP port runs as `anonymous_http_user`, which has
    no CREATE privilege, so the tests cannot set this up themselves.

    Idempotent, so that re-running the workflow against a composition that is
    already up does not have to start from `down -v`.
    """
    quoted_view_ident = '"' + FIXTURE_QUOTED_VIEW.replace('"', '""') + '"'
    c.sql(f"""
        CREATE TABLE IF NOT EXISTS {FIXTURE_TABLE} (id int, other int);
        CREATE VIEW IF NOT EXISTS {FIXTURE_VIEW} AS
            SELECT id, other FROM {FIXTURE_TABLE};
        CREATE INDEX IF NOT EXISTS {FIXTURE_INDEX}
            IN CLUSTER {FIXTURE_CLUSTER} ON {FIXTURE_VIEW} (id);
        CREATE VIEW IF NOT EXISTS {quoted_view_ident} AS
            SELECT id, other FROM {FIXTURE_TABLE};
        CREATE INDEX IF NOT EXISTS {FIXTURE_QUOTED_INDEX}
            IN CLUSTER {FIXTURE_CLUSTER} ON {quoted_view_ident} (id);
        INSERT INTO {FIXTURE_TABLE} VALUES (1, 1);
        """)

    # The introspection sources report the new dataflow only on their next tick,
    # so without this the tests can open a page whose table does not list it yet.
    deadline = time.time() + 60
    with c.sql_connection(
        startup_params={"cluster": FIXTURE_CLUSTER, "cluster_replica": FIXTURE_REPLICA}
    ) as conn:
        cursor = conn.cursor()
        for index in (FIXTURE_INDEX, FIXTURE_QUOTED_INDEX):
            while True:
                cursor.execute(
                    b"SELECT count(*) FROM mz_introspection.mz_records_per_dataflow "
                    b"WHERE name LIKE %s",
                    (f"Dataflow: %.{index}",),
                )
                row = cursor.fetchone()
                assert row is not None
                if row[0] > 0:
                    break
                if time.time() > deadline:
                    raise AssertionError(
                        f"index {index} did not show up in "
                        f"mz_records_per_dataflow on {FIXTURE_CLUSTER}.{FIXTURE_REPLICA}"
                    )
                time.sleep(1)
