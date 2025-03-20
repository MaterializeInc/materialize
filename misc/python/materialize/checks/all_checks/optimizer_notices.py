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

SCHEMA = "optimizer_notices"


class OptimizerNotices(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                f"""
                $postgres-execute connection=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                ALTER SYSTEM SET enable_mz_notices TO true
                > DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;
                > CREATE SCHEMA {SCHEMA};
                > CREATE TABLE {SCHEMA}.t1(x INTEGER, y INTEGER);
                > CREATE INDEX t1_idx ON {SCHEMA}.t1(x, y);
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                f"""
                # emits one "index too wide" notice
                > CREATE MATERIALIZED VIEW {SCHEMA}.mv1 AS SELECT x, y FROM {SCHEMA}.t1 WHERE x = 5;
                """,
                f"""
                # emits one "index too wide" notice and one "index key empty" notice
                > CREATE VIEW {SCHEMA}.v1 AS SELECT x, y FROM {SCHEMA}.t1 WHERE x = 7;
                > CREATE INDEX v1_idx ON {SCHEMA}.v1();
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                f"""
                > SELECT o.type, o.name, replace(n.notice_type, ' ', '␠')
                  FROM mz_internal.mz_notices n
                  JOIN mz_catalog.mz_objects o ON (o.id = n.object_id)
                  JOIN mz_catalog.mz_schemas s ON (s.id = o.schema_id)
                  WHERE s.name = '{SCHEMA}';
                index             v1_idx  Empty␠index␠key
                index             v1_idx  Index␠too␠wide␠for␠literal␠constraints
                materialized-view mv1     Index␠too␠wide␠for␠literal␠constraints
                """
            )
        )
