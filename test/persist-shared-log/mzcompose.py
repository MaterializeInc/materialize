# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Smoke tests for Materialize running with the persist shared log as the
consensus backend (rpc:// scheme).
"""

import textwrap

from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.metadata_store import (
    METADATA_STORE,
    is_external_metadata_store,
    metadata_store_services,
)
from materialize.mzcompose.services.persist_shared_log import PersistSharedLog
from materialize.mzcompose.services.testdrive import Testdrive

# The shared log service needs its own persist backend. Use the metadata
# store when available, otherwise fall back to in-memory.
if is_external_metadata_store(METADATA_STORE):
    _shared_log = PersistSharedLog(
        blob_url="file:///mzdata/persist/blob",
        consensus_url=f"postgres://root@{METADATA_STORE}:26257?options=--search_path=shared_log_consensus",
        depends_on={METADATA_STORE: {"condition": "service_healthy"}},
    )
else:
    _shared_log = PersistSharedLog()

SERVICES = [
    _shared_log,
    Materialized(
        persist_consensus_url="rpc://persist-shared-log:6890",
    ),
    Testdrive(
        no_consistency_checks=True,
    ),
    *metadata_store_services(),
]


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Smoke test: boot Materialize with persist-shared-log consensus and run basic SQL."""

    c.up("persist-shared-log")
    c.up("materialized")

    c.testdrive(
        input=textwrap.dedent(
            """
            > CREATE TABLE t (a int)
            > INSERT INTO t VALUES (1), (2), (3)
            > SELECT sum(a) FROM t
            6

            > CREATE DEFAULT INDEX ON t
            > SELECT * FROM t ORDER BY a
            1
            2
            3

            > CREATE MATERIALIZED VIEW mv AS SELECT a * 2 AS doubled FROM t
            > SELECT * FROM mv ORDER BY doubled
            2
            4
            6

            > INSERT INTO t VALUES (4)
            > SELECT sum(a) FROM t
            10

            > DROP MATERIALIZED VIEW mv
            > DROP TABLE t
            """
        ).strip()
        + "\n"
    )


def workflow_restart(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Verify state survives a Materialize restart with persist-shared-log consensus."""

    c.up("persist-shared-log")
    c.up("materialized")

    c.testdrive(
        input=textwrap.dedent(
            """
            > CREATE TABLE survive (x int)
            > INSERT INTO survive VALUES (42)
            > SELECT * FROM survive
            42
            """
        ).strip()
        + "\n"
    )

    c.kill("materialized")
    c.up("materialized")

    c.testdrive(
        input=textwrap.dedent(
            """
            > SELECT * FROM survive
            42
            > INSERT INTO survive VALUES (99)
            > SELECT sum(x) FROM survive
            141
            > DROP TABLE survive
            """
        ).strip()
        + "\n"
    )
