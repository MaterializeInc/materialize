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


class PersistCommitter(Check):
    """Verifies that shard state written through the in-envd persist committer
    survives envd restart and upgrade."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE persist_committer_t (a int);
            > INSERT INTO persist_committer_t VALUES (1), (2), (3);
            > CREATE MATERIALIZED VIEW persist_committer_mv AS
                SELECT count(*) FROM persist_committer_t;
            """))

    def manipulate(self) -> list[Testdrive]:
        # This list MUST be of length 2.
        return [
            Testdrive(dedent(s))
            for s in [
                "> INSERT INTO persist_committer_t VALUES (4);",
                "> INSERT INTO persist_committer_t VALUES (5);",
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            $ set-sql-timeout duration=60s

            > SELECT * FROM persist_committer_mv;
            5
            """))
