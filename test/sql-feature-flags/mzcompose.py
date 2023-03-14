# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import argparse
import os
import tempfile
from textwrap import dedent, indent

from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import Materialized, Testdrive

SERVICES = [
    Materialized(unsafe_mode=False),
    Testdrive(no_reset=True, seed=1),
]

MZ_SYSTEM_CONNECTION_URL = (
    "postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}"
)
USER_CONNECTION_URL = (
    "postgres://materialize:materialize@${testdrive.materialize-sql-addr}"
)


def header(test_name: str, drop_schema: bool) -> str:
    """Generate a TD header for a SQL feature test scenario."""
    header = dedent(
        f"""
        # Feature test for SQL feature test: {test_name}
        #####################################{'#' * len(test_name)}
        """
    )
    # Re-create schema (optional).
    if drop_schema:
        header += dedent(
            f"""
            > DROP SCHEMA IF EXISTS public CASCADE;
            > CREATE SCHEMA public /* {test_name} */;
            """
        )
    # Create connections.
    header += dedent(
        f"""
        $ postgres-connect name=user url={USER_CONNECTION_URL}
        $ postgres-connect name=mz_system url={MZ_SYSTEM_CONNECTION_URL}
        """
    )
    return header.strip()


def statement_error(statement: str, error_msg: str) -> str:
    """Generate a TD command that asserts that `statement` fails with `error_msg`."""
    return "\n".join(
        [
            indent(statement.strip(), prefix="  ").replace("  ", "! ", 1),
            f"contains:{error_msg}",
        ]
    )


def statement_ok(statement: str) -> str:
    """Generate a TD command that executes `statement`."""
    return indent(statement.strip(), prefix="  ").replace("  ", "> ", 1)


def query_ok(query: str) -> str:
    """Generate a TD command that asserts that a query does not fail."""
    return "\n".join(
        [
            "$ postgres-execute connection=user",
            query.strip(),
        ]
    )


def alter_system_set(name: str, value: str) -> str:
    """Generate a TD command that atlers a system parameter."""
    return dedent(
        f"""
        $ postgres-execute connection=mz_system
        ALTER SYSTEM SET {name} = '{value}';
        """
    ).strip()


class FeatureTestScenario:
    """
    A base class for all feature test scenarios.

    Each scenario is a `FeatureTestScenario` defined in this file. All
    subclasses are included in the `default` mzcompose workflow by default.
    """

    @classmethod
    def phase1(cls) -> str:
        return "\n\n".join(
            [
                # Include the header.
                header(f"{cls.__name__} (phase 1)", drop_schema=True),
                # We cannot create item #1 when the feature is turned off (default).
                statement_error(cls.create_item(ordinal=1), cls.feature_error()),
                # Turn the feature on.
                alter_system_set(cls.feature_name(), "on"),
                # We can create item #1 when the feature is turned on.
                statement_ok(cls.create_item(ordinal=1)),
                # We can query item #1 when the feature is turned on.
                query_ok(cls.query_item(ordinal=1)),
                # Turn the feature off.
                alter_system_set(cls.feature_name(), "off"),
                # We cannot create item #2 when the feature is turned off.
                statement_error(cls.create_item(ordinal=2), cls.feature_error()),
            ]
        )

    @classmethod
    def phase2(cls) -> str:
        return "\n\n".join(
            [
                # Include the header.
                header(f"{cls.__name__} (phase 2)", drop_schema=False),
                # We can query item #1 when the feature is turned on. Ensures
                # that catalog rehydration ignores SQL-level feature flags.
                query_ok(cls.query_item(ordinal=1)),
                # We can drop item #1.
                statement_ok(cls.drop_item(ordinal=1)),
                # We cannot create item #2 when the feature is turned off.
                # Ensures that the feature flag is respected for new items.
                statement_error(cls.create_item(ordinal=2), cls.feature_error()),
            ]
        )

    @classmethod
    def feature_name(cls) -> str:
        """The name of the feature flag under test."""
        assert False, "feature_name() must be overriden"

    @classmethod
    def feature_error(cls) -> str:
        """The error expected when the feature is disabled."""
        assert False, "feature_error() must be overriden"

    @classmethod
    def create_item(cls, ordinal: int) -> str:
        """A SQL statement that creates an item that depends on the feature."""
        assert False, "create_item() must be overriden"

    @classmethod
    def drop_item(cls, ordinal: int) -> str:
        """A SQL statement that drops an item that depends on the feature."""
        assert False, "drop_item() must be overriden"

    @classmethod
    def query_item(cls, ordinal: int) -> str:
        """A SQL query referencing an item that depends on the feature."""
        assert False, "query_item() must be overriden"


class WithMutuallyRecursive(FeatureTestScenario):
    @classmethod
    def feature_name(cls) -> str:
        return "enable_with_mutually_recursive"

    @classmethod
    def feature_error(cls) -> str:
        return "`WITH MUTUALLY RECURSIVE` syntax is not enabled"

    @classmethod
    def create_item(cls, ordinal: int) -> str:
        return dedent(
            f"""
            CREATE VIEW wmr_{ordinal:02d} AS WITH MUTUALLY RECURSIVE
                foo (a int, b int) AS (SELECT 1, 2 UNION SELECT a, 7 FROM bar),
                bar (a int) as (SELECT a FROM foo)
            SELECT * FROM bar;
            """
        )

    @classmethod
    def drop_item(cls, ordinal: int) -> str:
        return f"DROP VIEW wmr_{ordinal:02d}"

    @classmethod
    def query_item(cls, ordinal: int) -> str:
        return f"SELECT * FROM wmr_{ordinal:02d}"


def run_test(c: Composition, args: argparse.Namespace) -> None:
    c.up("materialized")
    c.up("testdrive", persistent=True)

    scenarios = (
        [globals()[args.scenario]]
        if args.scenario
        else FeatureTestScenario.__subclasses__()
    )

    # To add a new scenario create a new FeatureTestScenario subclass
    for scenario in scenarios:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=c.path,
            prefix=f"{scenario.__name__}-phase1-",
        ) as tmp:
            tmp.write(scenario.phase1())
            tmp.flush()
            c.exec("testdrive", os.path.basename(tmp.name))

        c.stop("materialized")
        c.up("materialized")

        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=c.path,
            prefix=f"{scenario.__name__}-phase2-",
        ) as tmp:
            tmp.write(scenario.phase2())
            tmp.flush()
            c.exec("testdrive", os.path.basename(tmp.name))


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--scenario", metavar="SCENARIO", type=str, help="Scenario to run."
    )
    run_test(c, parser.parse_args())
