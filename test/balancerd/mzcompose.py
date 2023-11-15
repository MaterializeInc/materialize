# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pg8000.dbapi import ProgrammingError
from pg8000.exceptions import InterfaceError

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.mzcompose.services.materialized import Materialized

SERVICES = [
    Balancerd(),
    Materialized(
        # We do not do anything interesting on the Mz side
        # to justify the extra restarts
        sanity_restart=False
    ),
]


def workflow_default(c: Composition) -> None:
    c.down(destroy_volumes=True)

    for i, name in enumerate(c.workflows):
        if name == "default":
            continue
        with c.test_case(name):
            c.workflow(name)


def workflow_wide_result(c: Composition) -> None:
    """Test passthrough of wide rows"""
    # Start balancerd without Materialize
    c.up("balancerd", "materialized")

    cursor = c.sql_cursor(service="balancerd")
    cursor.execute("SELECT 'ABC' || REPEAT('x', 1024 * 1024 * 96) || 'XYZ'")
    rows = cursor.fetchall()
    assert len(rows) == 1
    cols = rows[0]
    assert len(cols) == 1
    col = cols[0]
    assert len(col) == (1024 * 1024 * 96) + (2 * 3)
    assert col.startswith("ABCx")
    assert col.endswith("xXYZ")


def workflow_long_result(c: Composition) -> None:
    """Test passthrough of long results"""
    c.up("balancerd", "materialized")

    cursor = c.sql_cursor(service="balancerd")
    cursor.execute(
        "SELECT 'ABC', generate_series, 'XYZ' FROM generate_series(1, 10 * 1024 * 1024)"
    )
    cnt = 0
    for row in cursor.fetchall():
        cnt = cnt + 1
        assert len(row) == 3
        assert row[0] == "ABC"
        assert row[2] == "XYZ"
    assert cnt == 10 * 1024 * 1024


def workflow_long_query(c: Composition) -> None:
    """Test passthrough of a long SQL query."""
    c.up("balancerd", "materialized")

    cursor = c.sql_cursor(service="balancerd")
    small_pad_size = 512 * 1024
    small_pad = "x" * small_pad_size
    cursor.execute(f"SELECT 'ABC{small_pad}XYZ';")
    rows = cursor.fetchall()
    assert len(rows) == 1
    cols = rows[0]
    assert len(cols) == 1
    col = cols[0]
    assert len(col) == small_pad_size + (2 * 3)
    assert col.startswith("ABCx")
    assert col.endswith("xXYZ")

    medium_pad_size = 1 * 1024 * 1024
    medium_pad = "x" * medium_pad_size
    try:
        cursor.execute(f"SELECT 'ABC{medium_pad}XYZ';")
        assert False, "execute() expected to fail"
    except ProgrammingError as e:
        assert "statement batch size cannot exceed 1000.0 KB" in str(e)
    except:
        assert False, "execute() threw an unexpected exception"

    large_pad_size = 512 * 1024 * 1024
    large_pad = "x" * large_pad_size
    try:
        cursor.execute(f"SELECT 'ABC{large_pad}XYZ';")
        assert False, "execute() expected to fail"
    except InterfaceError as e:
        assert "network error" in str(e)
    except:
        assert False, "execute() threw an unexpected exception"

    # Confirm that balancerd remains up
    cursor = c.sql_cursor(service="balancerd")
    cursor.execute("SELECT 1;")


def workflow_mz_restarted(c: Composition) -> None:
    """Existing connections should fail if Mz is restarted.
    This protects against the client not being informed
    that their transaction has been aborted on the Mz side
    """
    c.up("materialized", "balancerd")

    cursor = c.sql_cursor(service="balancerd")

    cursor.execute("CREATE TABLE restart_mz (f1 INTEGER)")
    cursor.execute("START TRANSACTION")
    cursor.execute("INSERT INTO restart_mz VALUES (1)")
    c.kill("materialized")
    c.up("materialized")
    try:
        cursor.execute("INSERT INTO restart_mz VALUES (2)")
        assert False, "execute() expected to fail"
    except InterfaceError as e:
        assert "network error" in str(e)
    except:
        assert False, "execute() threw an unexpected exception"

    # Future connections work
    c.sql_cursor(service="balancerd")


def workflow_balancerd_restarted(c: Composition) -> None:
    """Existing connections should fail if balancerd is restarted"""
    c.up("materialized", "balancerd")

    cursor = c.sql_cursor(service="balancerd")

    cursor.execute("CREATE TABLE restart_balancerd (f1 INTEGER)")
    cursor.execute("START TRANSACTION")
    cursor.execute("INSERT INTO restart_balancerd VALUES (1)")
    c.kill("balancerd")
    c.up("balancerd")
    try:
        cursor.execute("INSERT INTO restart_balancerd VALUES (2)")
        assert False, "execute() expected to fail"
    except InterfaceError as e:
        assert "network error" in str(e)
    except:
        assert False, "execute() threw an unexpected exception"

    # Future connections work
    c.sql_cursor(service="balancerd")


def workflow_mz_not_running(c: Composition) -> None:
    """New connections should fail if Mz is down"""
    c.up("balancerd", "materialized")
    c.kill("materialized")
    try:
        c.sql_cursor(service="balancerd")
        assert False, "connect() expected to fail"
    except ProgrammingError as e:
        assert any(
            expected in str(e)
            for expected in ["No route to host", "Connection timed out"]
        )
    except:
        assert False, "connect() threw an unexpected exception"

    # Things should work now
    c.up("materialized")
    c.sql_cursor(service="balancerd")


def workflow_user(c: Composition) -> None:
    """Test that the user is passed all the way to Mz itself."""
    c.up("balancerd", "materialized")

    # This is expected to succeed
    cursor = c.sql_cursor(service="balancerd", user="foo")

    try:
        cursor.execute("DROP DATABASE materialize CASCADE")
        assert False, "execute() expected to fail"
    except ProgrammingError as e:
        assert "must be owner of DATABASE materialize" in str(e)
    except:
        assert False, "execute() threw an unexpected exception"

    cursor.execute("SELECT current_user()")
    assert "foo" in str(cursor.fetchall())


def workflow_many_connections(c: Composition) -> None:
    c.up("balancerd", "materialized")

    cursors = []
    connections = 1000 - 10  #  Go almost to the limit, but not above
    print(f"Opening {connections} connections.")
    for i in range(connections):
        cursor = c.sql_cursor(service="balancerd")
        cursors.append(cursor)

    for cursor in cursors:
        cursor.execute("SELECT 'abc'")
        data = cursor.fetchall()
        assert len(data) == 1
        row = data[0]
        assert len(row) == 1
        col = row[0]
        assert col == "abc"
