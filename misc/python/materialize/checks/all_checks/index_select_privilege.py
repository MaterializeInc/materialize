# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


class IndexSelectPrivilege(Check):
    """`GRANT SELECT ON INDEX` survives restarts and upgrades, and still lets
    the grantee read the indexed view through the index."""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v26.50.0-dev")

    def _create_and_grant(self, i: int) -> str:
        return dedent(f"""
            > CREATE VIEW index_select_priv_v{i} AS SELECT f1 FROM index_select_priv_t WHERE f1 > {i}
            > CREATE INDEX index_select_priv_i{i} IN CLUSTER index_select_priv_cluster ON index_select_priv_v{i} (f1)
            > GRANT SELECT ON INDEX index_select_priv_i{i} TO index_select_priv_role
            """)

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
                $ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}
                GRANT CREATEROLE ON SYSTEM TO materialize

                > CREATE ROLE index_select_priv_role
                > CREATE CLUSTER index_select_priv_cluster SIZE 'scale=1,workers=1'
                > GRANT USAGE ON CLUSTER index_select_priv_cluster TO index_select_priv_role
                > CREATE TABLE index_select_priv_t (f1 INTEGER)
                > INSERT INTO index_select_priv_t VALUES (1), (2), (3), (4)
                """) + self._create_and_grant(0))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent("""
                    > INSERT INTO index_select_priv_t VALUES (5)
                    """) + self._create_and_grant(1)),
            Testdrive(dedent("""
                    > INSERT INTO index_select_priv_t VALUES (6)
                    """) + self._create_and_grant(2)),
        ]

    def validate(self) -> Testdrive:
        # `postgres-execute` fails the check if any statement errors, so each
        # read below proves the grantee can still read the view through its
        # index.
        reads = "\n".join(f"SELECT * FROM index_select_priv_v{i}" for i in range(3))
        return Testdrive(dedent("""
                > SELECT name, privilege_type FROM mz_internal.mz_show_object_privileges WHERE grantee = 'index_select_priv_role' AND object_type = 'index'
                index_select_priv_i0 SELECT
                index_select_priv_i1 SELECT
                index_select_priv_i2 SELECT

                > SELECT count(*) FROM index_select_priv_v2
                4

                $ postgres-execute connection=postgres://index_select_priv_role@${testdrive.materialize-sql-addr}
                SET cluster = index_select_priv_cluster
                """) + reads + "\n")
