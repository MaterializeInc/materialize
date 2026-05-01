# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Reproducer for catalog/storage state divergence on ALTER TABLE ADD COLUMN.

A failpoint stalls execution after the catalog durably commits
Op::AlterAddColumn but before alter_table_desc evolves the persist schema.
SIGKILL during that window simulates OOM/power loss, leaving divergent state:
catalog at v(N+1) but persist schema still at v(N). Restart must recover.
"""

import threading
import time

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [
    Materialized(),
]


def workflow_default(c: Composition) -> None:
    with c.override(
        Materialized(
            additional_system_parameter_defaults={
                "enable_alter_table_add_column": "true",
            },
        ),
    ):
        c.up("materialized")

        c.sql("CREATE TABLE t (a INT)")
        c.sql("INSERT INTO t VALUES (1), (2), (3)")

        # Stall after catalog commit, before storage schema evolution.
        c.sql("SET failpoints = 'alter_table_before_storage=sleep(30000)'")

        # ALTER hangs on failpoint; SIGKILL during the window.
        alter_thread = threading.Thread(
            target=lambda: c.sql("ALTER TABLE t ADD COLUMN b INT"),
        )
        alter_thread.daemon = True
        alter_thread.start()
        time.sleep(3)
        c.kill("materialized")
        alter_thread.join(timeout=5)

        # Restart with divergent catalog/storage state.
        c.up("materialized")

        # Original data must survive.
        result = c.sql_query("SELECT count(*) FROM t")
        assert result[0][0] == 3, f"expected 3 rows, got {result[0][0]}"

        # Table should have the new column (catalog committed it).
        result = c.sql_query(
            "SELECT column_name FROM mz_columns "
            "WHERE id = (SELECT id FROM mz_tables WHERE name = 't') "
            "ORDER BY position"
        )
        columns = [row[0] for row in result]
        assert "b" in columns, f"expected column 'b', got {columns}"

        # INSERT using the new column must work.
        c.sql("INSERT INTO t (a, b) VALUES (4, 40)")

        # A subsequent ALTER must not fail from stale schema state.
        c.sql("ALTER TABLE t ADD COLUMN c INT")
