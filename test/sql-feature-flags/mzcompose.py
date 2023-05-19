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
from materialize.mzcompose.services import Materialized, Redpanda, Testdrive

SERVICES = [
    Redpanda(),
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
    """Generate a TD command that sets a system parameter."""
    return dedent(
        f"""
        $ postgres-execute connection=mz_system
        ALTER SYSTEM SET {name} = '{value}';
        """
    ).strip()


def alter_system_reset(name: str) -> str:
    """Generate a TD command that resets a system parameter."""
    return dedent(
        f"""
        $ postgres-execute connection=mz_system
        ALTER SYSTEM RESET {name};
        """
    ).strip()


def alter_system_reset_all() -> str:
    """Generate a TD command that reset all system parameters."""
    return dedent(
        """
        $ postgres-execute connection=mz_system
        ALTER SYSTEM RESET ALL;
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
                cls.initialize(),
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
                cls.initialize(),
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
    def phase3(cls) -> str:
        return "\n\n".join(
            [
                # Include the header.
                header(f"{cls.__name__} (phase 3)", drop_schema=False),
                # Because we have restarted, we need to ensure that we're getting
                # the parameter's default value, which will be "on".
                alter_system_reset(cls.feature_name()),
                cls.initialize(),
                # The feature is immediately turned on because it's a default parameter.
                statement_ok(cls.create_item(ordinal=1)),
                query_ok(cls.query_item(ordinal=1)),
                # We can drop item #1.
                statement_ok(cls.drop_item(ordinal=1)),
            ]
        )

    @classmethod
    def reset_all(cls) -> str:
        return "\n\n".join(
            [
                cls.initialize(),
                # The feature is immediately turned on because it's a default parameter.
                statement_ok(cls.create_item(ordinal=1)),
                query_ok(cls.query_item(ordinal=1)),
                # We can drop item #1.
                statement_ok(cls.drop_item(ordinal=1)),
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
    def initialize(cls) -> str:
        """Any SQL statements that must be executed before the statement under test."""
        return ""

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
        return "WITH MUTUALLY RECURSIVE is not supported"

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


class FormatJson(FeatureTestScenario):
    @classmethod
    def feature_name(cls) -> str:
        return "enable_format_json"

    @classmethod
    def feature_error(cls) -> str:
        return "FORMAT JSON is not supported"

    @classmethod
    def initialize(cls) -> str:
        return "> CREATE CONNECTION IF NOT EXISTS kafka_conn_for_format_json TO KAFKA (BROKER '${testdrive.kafka-addr}')"

    @classmethod
    def create_item(cls, ordinal: int) -> str:
        return dedent(
            f"""
            CREATE SOURCE kafka_source_{ordinal:02d}
                FROM KAFKA CONNECTION kafka_conn_for_format_json (TOPIC 'bar')
                FORMAT JSON
                WITH (SIZE '1');
            """
        )

    @classmethod
    def drop_item(cls, ordinal: int) -> str:
        return f"DROP SOURCE kafka_source_{ordinal:02d};"

    @classmethod
    def query_item(cls, ordinal: int) -> str:
        # Test cannot spin up infra for this feature to be tested, but we just want to verify it
        # plans successfully.
        return "SELECT true;"


def run_test(c: Composition, args: argparse.Namespace) -> None:
    c.up("redpanda", "materialized")
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

        materialized = Materialized(
            unsafe_mode=False,
            additional_system_parameter_defaults=[
                "{}=on".format(scenario.feature_name()),
            ],
        )

        with c.override(materialized):
            c.stop("materialized")
            c.up("materialized")

            with tempfile.NamedTemporaryFile(
                mode="w",
                dir=c.path,
                prefix=f"{scenario.__name__}-phase3-",
            ) as tmp:
                tmp.write(scenario.phase3())
                tmp.flush()
                c.exec("testdrive", os.path.basename(tmp.name))

    # Dedicated test for ALTER SYSTEM RESET ALL
    with tempfile.NamedTemporaryFile(
        mode="w",
        dir=c.path,
        prefix="phase-reset-all-",
    ) as tmp:
        tmp_buf = [header("(phase reset-all)", drop_schema=False)]
        for scenario in scenarios:
            # Turn all features off.
            tmp_buf.append(alter_system_set(scenario.feature_name(), "off"))

        # Run ALTER SYSTEM RESET ALL
        tmp_buf.append(alter_system_reset_all())
        for scenario in scenarios:
            # Write each scenarios reset all data
            tmp_buf.append(scenario.reset_all())

        # Create MZ config with all features set on by default
        materialized = Materialized(
            unsafe_mode=False,
            additional_system_parameter_defaults=list(
                map(lambda scenario: "{}=on".format(scenario.feature_name()), scenarios)
            ),
        )
        with c.override(materialized):
            c.stop("materialized")
            c.up("materialized")
            tmp.write("\n\n".join(tmp_buf))
            tmp.flush()
            c.exec("testdrive", os.path.basename(tmp.name))


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    parser.add_argument(
        "--scenario", metavar="SCENARIO", type=str, help="Scenario to run."
    )
    run_test(c, parser.parse_args())
