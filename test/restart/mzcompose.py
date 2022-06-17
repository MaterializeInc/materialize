# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Kafka,
    Materialized,
    SchemaRegistry,
    Testdrive,
    Zookeeper,
)

testdrive_no_reset = Testdrive(name="testdrive_no_reset", no_reset=True)

SERVICES = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Materialized(),
    Testdrive(),
    testdrive_no_reset,
]


def workflow_github_8021(c: Composition) -> None:
    c.up("materialized")
    c.wait_for_materialized("materialized")
    c.run("testdrive", "github-8021.td")

    # Ensure MZ can boot
    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized("materialized")
    c.kill("materialized")


def workflow_audit_log(c: Composition) -> None:
    c.up("materialized")
    c.wait_for_materialized(service="materialized")

    # Create some audit log entries.
    c.sql("CREATE TABLE t (i INT)")
    c.sql("CREATE DEFAULT INDEX ON t")

    # occurred_at is a timestamp with microsecond precesion, so order by something
    # else also to avoid flakes in case two events share a timestamp.
    log = c.sql_query("SELECT * FROM mz_audit_events ORDER BY occurred_at, uuid")

    # Restart mz.
    c.kill("materialized")
    c.up("materialized")
    c.wait_for_materialized()

    # Verify the audit log entries are still present and have not changed.
    restart_log = c.sql_query(
        "SELECT * FROM mz_audit_events ORDER BY occurred_at, uuid"
    )
    if log != restart_log:
        print("initial audit log:", log)
        print("audit log after restart:", restart_log)
        raise Exception("audit logs not equal after restart")


def workflow_default(c: Composition) -> None:
    workflow_github_8021(c)
    workflow_audit_log(c)
