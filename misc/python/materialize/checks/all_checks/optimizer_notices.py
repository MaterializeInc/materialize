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

SCHEMA = "check_optimzier_notices"


class OptimizerNotices(Check):
    def _can_run(self, e: Executor) -> bool:
        return self.base_version >= MzVersion.parse_mz("v0.80.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > DROP SCHEMA IF EXISTS {sch} CASCADE;
                > CREATE SCHEMA {sch};
                > CREATE TABLE {sch}.t1(x INTEGER, y INTEGER);
                > CREATE INDEX t1_idx ON {sch}.t1(x, y);
                """
            ).format(sch=SCHEMA)
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s).format(sch=SCHEMA))
            for s in [
                """
                # emits one "index too wide" notice
                > CREATE MATERIALIZED VIEW {sch}.mv1 AS SELECT x, y FROM {sch}.t1 WHERE x = 5;
                """,
                """
                # emits one "index too wide" notice one "index key empty" notice
                > CREATE VIEW {sch}.v1 AS SELECT x, y FROM {sch}.t1 WHERE x = 7;
                > CREATE INDEX v1_idx ON {sch}.v1();
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT o.type, o.name, replace(n.notice_type, ' ', '␠') FROM mz_internal.mz_notices_redacted n JOIN mz_catalog.mz_objects o ON (o.id = n.object_id);
                index             v1_idx Empty␠index␠key
                index             v1_idx Index␠too␠wide␠for␠literal␠constraints
                materialized-view mv1    Index␠too␠wide␠for␠literal␠constraints
                """
            )
        )
