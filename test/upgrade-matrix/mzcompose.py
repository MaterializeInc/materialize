# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import os
import random
import sys
import time
from collections.abc import Generator

import networkx as nx

from materialize import buildkite
from materialize.util import all_subclasses
from materialize.version_list import (
    get_published_minor_mz_versions,
)

# mzcompose may start this script from the root of the Mz repository,
# so we need to explicitly add this directory to the Python module search path
sys.path.append(os.path.dirname(__file__))

from nodes import (
    BeginUpgradeScenario,
    BeginVersion,
    ChecksInitialize,
    ChecksManipulate1,
    ChecksManipulate2,
    ChecksValidate,
    EndUpgradeScenario,
    EndVersion,
    Node,
)

from materialize.checks.actions import Action
from materialize.checks.all_checks import *  # noqa: F401 F403
from materialize.checks.checks import Check
from materialize.checks.executors import MzcomposeExecutor
from materialize.checks.scenarios import *  # noqa: F401 F403
from materialize.checks.scenarios import Scenario
from materialize.mz_version import MzVersion
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.debezium import Debezium
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.ssh_bastion_host import SshBastionHost
from materialize.mzcompose.services.testdrive import Testdrive as TestdriveService

SERVICES = [
    Cockroach(setup_materialize=True),
    Postgres(),
    Redpanda(auto_create_topics=True),
    Debezium(redpanda=True),
    Minio(setup_materialize=True),
    Materialized(
        external_minio=True, catalog_store="stash"
    ),  # Overriden inside Platform Checks
    TestdriveService(
        default_timeout="300s",
        no_reset=True,
        seed=1,
        entrypoint_extra=[
            "--var=replicas=1",
            f"--var=default-replica-size={Materialized.Size.DEFAULT_SIZE}-{Materialized.Size.DEFAULT_SIZE}",
            f"--var=default-storage-size={Materialized.Size.DEFAULT_SIZE}-1",
        ],
    ),
    SshBastionHost(),
]


def setup(c: Composition) -> None:
    c.up("testdrive", persistent=True)
    c.up("cockroach", "redpanda", "postgres", "debezium", "ssh-bastion-host")


def teardown(c: Composition) -> None:
    c.down(destroy_volumes=True)


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    """Test sequences of upgrades across the entire possible matrix of upgrade scenarios.

    Thie framework works by building a graph that contains the Mz versions and Checks actions
    to perform as nodes, as well as the allowed upgrade paths and other required dependencies
    (as edges). All valid simple paths within the graph are upgrade scenarios that need to be
    exercised and we pick a subset of them to run.
    """

    c.silent = True
    parser.add_argument(
        "--check",
        metavar="CHECK",
        type=str,
        action="append",
        help="Check(s) to run.",
        default=["PgCdc", "SinkUpsert"],
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Just print the upgrade paths that will be tested.",
    )

    parser.add_argument(
        "--min-version",
        metavar="VERSION",
        type=MzVersion.parse_without_prefix,
        default=MzVersion.parse_without_prefix("0.39.0"),
        help="Minimum Mz version to involve.",
    )

    parser.add_argument(
        "--num-scenarios",
        type=int,
        default=50,
        help="Number of upgrade scenarios to test",
    )

    parser.add_argument("--seed", metavar="SEED", type=str, default=str(time.time()))

    parser.add_argument("--scenario-id", type=int, default=None)

    args = parser.parse_args()

    shard = buildkite.get_parallelism_index()
    shard_count = buildkite.get_parallelism_count()

    print(f"--- Random seed is {args.seed}, shard is {shard} of {shard_count} shards.")
    random.seed(args.seed)

    all_checks = {check.__name__: check for check in all_subclasses(Check)}
    checks = (
        [all_checks[check] for check in args.check]
        if args.check
        else list(all_subclasses(Check))
    )

    print(f"--- Checks to use: {checks}")

    executor = MzcomposeExecutor(composition=c)

    versions_in_ascending_order = get_published_minor_mz_versions(
        newest_first=False, include_filter=lambda v: v >= args.min_version
    )

    print(
        "--- Testing upgrade scenarios involving the following versions: "
        + " ".join([str(v) for v in versions_in_ascending_order])
    )

    for id, upgrade_scenario in enumerate(
        get_upgrade_scenarios(
            versions=versions_in_ascending_order,
            num_scenarios=args.num_scenarios,
        )
    ):
        if args.scenario_id is not None and id != args.scenario_id:
            continue

        if id % shard_count != shard:
            continue

        print(f"--- Testing upgrade scenario with id {id}: {upgrade_scenario}")

        class GeneratedUpgradeScenario(Scenario):
            # The minimum version referenced in the upgrade scenario
            def base_version(self) -> MzVersion:
                return min(
                    [
                        node.version
                        for node in upgrade_scenario
                        if isinstance(node, BeginVersion) and node.version is not None
                    ]
                )

            # Convert the Nodes from the path into Checks Actions
            def actions(self) -> list[Action]:
                actions = []
                for node in upgrade_scenario:
                    actions.extend(node.actions(self))
                return actions

        scenario = GeneratedUpgradeScenario(checks=checks, executor=executor)
        if not args.dry_run:
            teardown(c)
            setup(c)
            scenario.run()
            teardown(c)


def random_simple_paths(
    G: nx.DiGraph, source: Node, target: Node
) -> Generator[list[Node], None, None]:
    current: Node = source
    path: list[Node] = [source]

    def reset() -> None:
        nonlocal current, path
        current = source
        path = [source]

    reset()
    while True:
        all_out_neighbours = [e[1] for e in G.out_edges(current)]
        new_out_neighbours = [n for n in all_out_neighbours if n not in path]
        if len(new_out_neighbours) == 0:
            # Dead end, start from the beginning
            reset()
            continue

        next = random.choice(new_out_neighbours)

        path = path + [next]
        if next == target:
            yield path
            reset()
        else:
            current = next


def get_upgrade_scenarios(
    versions: list[MzVersion], num_scenarios: int
) -> list[list[Node]]:
    g = nx.DiGraph()

    # Nodes for the start and end of the upgrade scenario
    scenario_begin = BeginUpgradeScenario()
    scenario_end = EndUpgradeScenario()
    g.add_nodes_from([scenario_begin, scenario_end])

    # Nodes for starting and stopping a particular version
    # We also add a node that starts the HEAD version,
    # without a node to stop it.
    versions_begin = [BeginVersion(version=v) for v in versions] + [
        BeginVersion(version=None)
    ]
    g.add_nodes_from(versions_begin)

    versions_end = [EndVersion(version=v) for v in versions]
    g.add_nodes_from(versions_end)

    # Nodes for the individual phases of the Check framework
    checks_initialize = ChecksInitialize()
    checks_manipulate1 = ChecksManipulate1()
    checks_manipulate2 = ChecksManipulate2()
    checks_validate = ChecksValidate()
    checks_phases = [
        checks_initialize,
        checks_manipulate1,
        checks_manipulate2,
        checks_validate,
    ]
    g.add_nodes_from(checks_phases)

    # Now that we have specified all the available operations in terms of Nodes,
    # we define the allowed sequences of events in terms of Edges between them.

    # Allow starting and stopping the same version with no action required in-between
    for i in range(len(versions_end)):
        g.add_edge(versions_begin[i], versions_end[i])

    # Allow upgrades from X to X+1
    for v in range(len(versions_begin) - 1):
        g.add_edge(versions_end[v], versions_begin[v + 1])

    # Allow multiple Check phases to run within the same version
    for i in range(0, len(checks_phases) - 2):
        g.add_edge(checks_phases[i], checks_phases[i + 1])

    # Allow a Check phase to run immediately after upgrading to version X
    for i in range(len(versions_begin)):
        for checks_phase in checks_phases[:-1]:
            g.add_edge(versions_begin[i], checks_phase)

    # Allow a Check phase to run immediately before stopping version X
    for i in range(len(versions_end)):
        for checks_phase in checks_phases[:-1]:
            g.add_edge(checks_phase, versions_end[i])

    # Allow the test to start at any previous version:
    for i in range(len(versions_begin) - 1):
        g.add_edge(scenario_begin, versions_begin[i])

    # Validate will run only after the HEAD version has started
    g.add_edge(versions_begin[-1], checks_validate)

    # And the test can only end after Validate has run
    g.add_edge(checks_validate, scenario_end)

    # Now the Nodes are properly connected, so walk the graph.
    # Some of the paths will not valid upgrade scenarios,
    # so perform additional validation
    valid_paths = []
    for potential_path in random_simple_paths(g, scenario_begin, scenario_end):
        # Verify that all Check phases are present
        checks_phases_present = [
            phase for phase in potential_path if phase in checks_phases
        ]
        if checks_phases_present != checks_phases:
            continue

        # Check that BeginVersion and EndVersion are properly matched
        consistent_restarts = True
        last_started_version = None
        for phase in potential_path:
            if isinstance(phase, BeginVersion):
                last_started_version = phase.version
            elif isinstance(phase, EndVersion):
                assert (
                    last_started_version is not None
                ), f"last_started_version: {last_started_version}"
                assert phase.version is not None
                if last_started_version != phase.version:
                    # Path jumps from version to version without a EndVersion
                    consistent_restarts = False

        if consistent_restarts:
            valid_paths.append(potential_path)

        if len(valid_paths) >= num_scenarios:
            break

    return valid_paths
