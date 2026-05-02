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

# CTs were introduced in v0.127.0, feature-flagged off in v26.21.0,
# and fully removed from the parser in v26.23.0.
CT_MIN_VERSION = MzVersion.parse_mz("v0.127.0-dev")
CT_REMOVED_VERSION = MzVersion.parse_mz("v26.21.0-dev")
CT_PARSER_REMOVED_VERSION = MzVersion.parse_mz("v26.23.0-dev")


class ContinualTaskMigration(Check):
    """Test that continual task removal is handled on upgrade.

    When upgrading from a version that had CTs to one that removed them,
    verify that CT syntax is rejected and no CT objects remain in the catalog.

    Note: we cannot create CTs from this check because the new testdrive binary
    parses SQL locally and rejects CT syntax. The catalog migration that removes
    existing CTs is tested in v81_to_v82.rs unit tests instead.
    """

    def _can_run(self, e: Executor) -> bool:
        # Only meaningful when upgrading from a version that had CTs
        # to a version that removed them.
        return (
            self.base_version >= CT_MIN_VERSION
            and self.base_version < CT_REMOVED_VERSION
        )

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE ct_migration_input (key INT);
            > INSERT INTO ct_migration_input VALUES (1);
            > CREATE MATERIALIZED VIEW ct_migration_mv AS SELECT sum(key)::INT AS s FROM ct_migration_input;
        """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent("""
                > INSERT INTO ct_migration_input VALUES (2), (3);
            """)),
            Testdrive(dedent("""
                > INSERT INTO ct_migration_input VALUES (4), (5);
            """)),
        ]

    def validate(self) -> Testdrive:
        if self.current_version >= CT_PARSER_REMOVED_VERSION:
            # Parser no longer recognizes CONTINUAL keyword at all.
            return Testdrive(dedent("""
                > SELECT count(*) FROM mz_objects WHERE type = 'continual-task';
                0

                ! CREATE CONTINUAL TASK ct_should_fail (x INT) ON INPUT ct_migration_mv AS (
                    INSERT INTO ct_should_fail SELECT * FROM ct_migration_mv;
                  )
                contains:Expected DATABASE

                > SELECT s FROM ct_migration_mv;
                15
            """))
        elif self.current_version >= CT_REMOVED_VERSION:
            # Feature-flagged off but parser still recognizes the syntax.
            return Testdrive(dedent("""
                > SELECT count(*) FROM mz_objects WHERE type = 'continual-task';
                0

                ! CREATE CONTINUAL TASK ct_should_fail (x INT) ON INPUT ct_migration_mv AS (
                    INSERT INTO ct_should_fail SELECT * FROM ct_migration_mv;
                  )
                contains:CREATE CONTINUAL TASK is not available

                > SELECT s FROM ct_migration_mv;
                15
            """))
        else:
            # Still on old version: basic objects should work.
            return Testdrive(dedent("""
                > SELECT s FROM ct_migration_mv;
                15
            """))
