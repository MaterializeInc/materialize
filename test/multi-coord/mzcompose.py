# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from dataclasses import dataclass
from typing import Callable

from materialize.mzcompose import Composition
from materialize.mzcompose.services import (
    Materialized,
    Testdrive,
)

SERVICES = [
    Materialized(name="materialized1", hostname="materialized",
                 options=['--sql-listen-addr=0.0.0.0:6875', '--http-listen-addr=0.0.0.0:6876',
                          '--internal-sql-listen-addr=0.0.0.0:6877', '--internal-http-listen-addr=0.0.0.0:6878'],
                 ports=["6875"]),
    Materialized(name="materialized2", hostname="materialized",
                 options=['--sql-listen-addr=0.0.0.0:15721', '--http-listen-addr=0.0.0.0:15722',
                          '--internal-sql-listen-addr=0.0.0.0:15723', '--internal-http-listen-addr=0.0.0.0:15724'],
                 ports=["15721"]),
    Testdrive(name="testdrive1", volumes=["mzdata:/mzdata"],
              materialize_url="postgres://materialize@materialized:6875"),
    Testdrive(name="testdrive2", volumes=["mzdata:/mzdata"],
              materialize_url="postgres://materialize@materialized:15721"),
]


def workflow_default(c: Composition) -> None:
    """
    TODO(jkosh44)
    """
    c.up("testdrive1", persistent=True)
    c.up("testdrive2", persistent=True)
    c.up("materialized1")
    c.wait_for_materialized("materialized1", port=6875)

    c.sql("CREATE VIEW v AS SELECT 1;", service="materialized1")
    view = c.sql_query("SELECT * FROM v;", service="materialized1")
    assert view[0][0] == 1
    c.sql("DROP CLUSTER default CASCADE;", service="materialized1")

    c.up("materialized2")
    c.wait_for_materialized("materialized2", port=15721, query="CREATE CLUSTER default REPLICAS (r1 (size '1'))", expected="any")

    c.sql("DROP VIEW v;", service="materialized2")
    c.sql("CREATE VIEW v AS SELECT 2;", service="materialized2")
    view = c.sql_query("SELECT * FROM v;", service="materialized2")
    assert view[0][0] == 2
    c.sql("DROP CLUSTER default CASCADE;", service="materialized2")

    c.sql("CREATE CLUSTER default REPLICAS (r1 (size '1'))", service="materialized1")
    view = c.sql_query("SELECT * FROM v;", service="materialized1")
    assert view[0][0] != 1
