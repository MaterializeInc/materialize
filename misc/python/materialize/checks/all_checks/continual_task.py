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
from materialize.checks.common import KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD
from materialize.checks.executors import Executor
from materialize.mz_version import MzVersion


def schemas() -> str:
    return dedent(KAFKA_SCHEMA_WITH_SINGLE_STRING_FIELD)


class AuditLogCT(Check):
    """Continual Task for audit logging"""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version > MzVersion.parse_mz("v0.127.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                > CREATE TABLE t_input (key INT);
                > INSERT INTO t_input VALUES (1);
                > CREATE MATERIALIZED VIEW anomalies AS SELECT sum(key)::INT FROM t_input;
                > CREATE CONTINUAL TASK audit_log (count INT) ON INPUT anomalies AS (
                    INSERT INTO audit_log SELECT * FROM anomalies WHERE sum IS NOT NULL;
                  )
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                > INSERT INTO t_input VALUES (2), (3);
                """,
                """
                > INSERT INTO t_input VALUES (4), (5), (6);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM audit_log
                1
                6
                21
           """
            )
        )


class StreamTableJoinCT(Check):
    """Continual Task for stream table join"""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version > MzVersion.parse_mz("v0.127.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                > CREATE TABLE big (key INT);
                > CREATE TABLE small (key INT, val STRING);
                > INSERT INTO small VALUES (1, 'v1');
                > INSERT INTO small VALUES (2, 'v2');
                > INSERT INTO small VALUES (3, 'v3');
                > INSERT INTO small VALUES (4, 'v4');
                > INSERT INTO small VALUES (5, 'v5');
                > CREATE CONTINUAL TASK stj (key INT, val STRING) ON INPUT big AS (
                    INSERT INTO stj SELECT b.key, s.val FROM big b JOIN small s ON b.key = s.key;
                  )
                > INSERT INTO big VALUES (1), (2), (3), (4), (5)
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                > UPDATE small SET val = 'v' || val;
                > INSERT INTO big VALUES (1), (2), (3), (4), (5)
                """,
                """
                > UPDATE small SET val = 'v' || val;
                > INSERT INTO big VALUES (1), (2), (3), (4), (5)
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM stj
                1 v1
                2 v2
                3 v3
                4 v4
                5 v5
                1 vv1
                2 vv2
                3 vv3
                4 vv4
                5 vv5
                1 vvv1
                2 vvv2
                3 vvv3
                4 vvv4
                5 vvv5
           """
            )
        )


class UpsertCT(Check):
    """Continual Task for upserts"""

    def _can_run(self, e: Executor) -> bool:
        return self.base_version > MzVersion.parse_mz("v0.127.0-dev")

    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                > CREATE TABLE append_only (key INT, val INT);
                > CREATE CONTINUAL TASK upsert (key INT, val INT) ON INPUT append_only AS (
                    DELETE FROM upsert WHERE key IN (SELECT key FROM append_only);
                    INSERT INTO upsert SELECT key, max(val) FROM append_only GROUP BY key;
                  )
                > INSERT INTO append_only VALUES (1, 2), (1, 1)
                """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                > INSERT INTO append_only VALUES (1, 3), (2, 4)
                """,
                """
                > INSERT INTO append_only VALUES (1, 5), (2, 6), (3, 7);
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > INSERT INTO append_only VALUES (3, 8);

                > SELECT * FROM upsert
                1 5
                2 6
                3 8
           """
            )
        )
