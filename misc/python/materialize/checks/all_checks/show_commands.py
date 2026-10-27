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


class ShowCommands(Check):
    """SHOW commands with no coverage elsewhere: SHOW OBJECTS, SHOW COLUMNS,
    SHOW ROLE MEMBERSHIP, SHOW PRIVILEGES, SHOW CLUSTER REPLICAS, and the
    SHOW CREATE forms for tables, types, materialized views, and indexes.

    The SHOW CREATE statements are executed without asserting on their
    output, which changes formatting between versions.
    """

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE SCHEMA show_cmds_schema
            > CREATE TABLE show_cmds_schema.show_cmds_table (f1 INT NOT NULL, f2 STRING)
            > CREATE VIEW show_cmds_schema.show_cmds_view AS SELECT f1 FROM show_cmds_schema.show_cmds_table
            > CREATE MATERIALIZED VIEW show_cmds_schema.show_cmds_mv AS SELECT count(*) AS c FROM show_cmds_schema.show_cmds_table
            > CREATE INDEX show_cmds_index ON show_cmds_schema.show_cmds_table (f1)
            > CREATE TYPE show_cmds_schema.show_cmds_type AS LIST (ELEMENT TYPE = int4)

            > CREATE ROLE show_cmds_r1
            > CREATE ROLE show_cmds_r2
            > GRANT show_cmds_r1 TO show_cmds_r2
            > GRANT SELECT ON show_cmds_schema.show_cmds_table TO show_cmds_r1

            > CREATE CLUSTER show_cmds_cluster REPLICAS (show_cmds_replica (SIZE 'scale=1,workers=1'))
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > CREATE TABLE show_cmds_schema.show_cmds_table2 (f1 INT)
                > CREATE ROLE show_cmds_r3
                > GRANT show_cmds_r2 TO show_cmds_r3
                """,
                """
                > CREATE VIEW show_cmds_schema.show_cmds_view2 AS SELECT 1 AS c
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SHOW OBJECTS FROM show_cmds_schema
            show_cmds_table table ""
            show_cmds_table2 table ""
            show_cmds_view view ""
            show_cmds_view2 view ""
            show_cmds_mv materialized-view ""
            show_cmds_index index ""
            show_cmds_type type ""

            > SHOW COLUMNS FROM show_cmds_schema.show_cmds_table
            f1 false integer ""
            f2 true text ""

            > SHOW ROLE MEMBERSHIP FOR show_cmds_r2
            show_cmds_r1 show_cmds_r2 mz_system

            > SELECT count(*) > 0 FROM (SHOW PRIVILEGES FOR show_cmds_r1)
            true

            # TODO(SQL-531): Re-enable once SHOW CLUSTER REPLICAS
            # (mz_show_cluster_replicas) stops dropping replicas. The view joins
            # mz_cluster_replicas.id against mz_comments.id and filters on
            # object_type in a WHERE clause. Replica ids and catalog-item ids are
            # separate allocators that both render as u<N>, so when a commented
            # non-replica object shares a replica's numeric id the join pulls in
            # its comment and the filter drops the replica. In parallel runs
            # show_cmds_replica collides with another check's commented type.
            # > SHOW CLUSTER REPLICAS WHERE cluster = 'show_cmds_cluster'
            # show_cmds_cluster show_cmds_replica scale=1,workers=1 true ""

            $ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
            SHOW CREATE TABLE show_cmds_schema.show_cmds_table
            SHOW CREATE MATERIALIZED VIEW show_cmds_schema.show_cmds_mv
            SHOW CREATE INDEX show_cmds_schema.show_cmds_index
            SHOW CREATE VIEW show_cmds_schema.show_cmds_view

            # SHOW CREATE TYPE was added in v0.160.0 (mz_version_num 16000), so
            # the Self-Managed upgrade scenarios run validate() against older
            # bases that cannot parse it.
            $[version>=16000] postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
            SHOW CREATE TYPE show_cmds_schema.show_cmds_type
            """))
