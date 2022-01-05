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

SERVICES = [
    Zookeeper(),
    Kafka(auto_create_topics=True),
    SchemaRegistry(),
    Materialized(),
    Testdrive(depends_on=["kafka", "schema-registry", "materialized"]),
]


def workflow_disable_user_indexes(c: Composition) -> None:
    """Test --disable-user-indexes mode."""
    # Create catalog with vanilla Materialize.
    c.run("testdrive-svc", "user-indexes-enabled.td")

    # Test semantics of disabling user indexes.
    with c.override(Materialized(options="--disable-user-indexes")):
        c.run("testdrive-svc", "--no-reset", "user-indexes-disabled.td")


def workflow_github_8021(c: Composition) -> None:
    """Test for crash described in GitHub issue #8021."""
    # Install views that previously caused Materialize to crash.
    c.up("materialized")
    c.sql(
        """
        CREATE TABLE t1 (f1 int4, f2 int4);
        CREATE VIEW v2 AS SELECT * FROM t1;
        CREATE INDEX i1 ON v2 (f1, f2);
        CREATE INDEX i2 ON t1 (f2);
    """
    )

    # Ensure Materialize can reboot.
    c.kill("materialized")
    c.up("materialized")


def workflow_all(c: Composition) -> None:
    """Run all other workflows in sequence."""
    for workflow in [workflow_disable_user_indexes, workflow_github_8021]:
        c.rm("materialized", "testdrive-svc")
        c.rm_volumes("mzdata", force=True)
        workflow(c)
