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
from typing import Generator, List

import networkx as nx

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
from materialize.mzcompose import Composition, WorkflowArgumentParser
from materialize.mzcompose.services import (
    Cockroach,
    Debezium,
    Materialized,
    Postgres,
    Redpanda,
)
from materialize.mzcompose.services import Testdrive as TestdriveService
from materialize.util import MzVersion, released_materialize_versions

# All released Materialize versions, in order from most to least recent.
all_versions = released_materialize_versions()

SERVICES = [
    Cockroach(setup_materialize=True),
    Postgres(),
    Redpanda(auto_create_topics=True),
    Debezium(redpanda=True),
    Materialized(),  # Overriden inside Platform Checks
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
]


def setup(c: Composition) -> None:
    c.up("testdrive", persistent=True)
    c.up("cockroach", "redpanda", "postgres", "debezium")


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
        type=MzVersion.parse,
        default=MzVersion.parse("0.39.0"),
        help="Minimum Mz version to involve.",
    )

    parser.add_argument(
        "--max-version-gap",
        type=int,
        default=1,
        help="Maximum number of versions to skip",
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

    print(f"--- Random seed is {args.seed}")
    random.seed(args.seed)

    checks = (
        [globals()[check] for check in args.check]
        if args.check
        else Check.__subclasses__()
    )

    print(f"--- Checks to use: {checks}")

    executor = MzcomposeExecutor(composition=c)

    versions = list(reversed([v for v in all_versions if v >= args.min_version]))
    print(
        "--- Testing upgrade scenarios involving the following versions: "
        + " ".join([str(v) for v in versions])
    )

    for id, upgrade_scenario in enumerate(
        get_upgrade_scenarios(
            versions=versions,
            max_version_gap=args.max_version_gap,
            num_scenarios=args.num_scenarios,
        )
    ):
        if args.scenario_id is not None and id != args.scenario_id:
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
            def actions(self) -> List[Action]:
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
) -> Generator[List[Node], None, None]:
    current: Node = source
    path: List[Node] = [source]

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
    versions: List[MzVersion], max_version_gap: int, num_scenarios: int
) -> List[List[Node]]:
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

    # Allow upgrades from X-N to X for N >= 1 ... max_version_gap
    for skipped_count in range(1, max_version_gap + 1):
        if len(versions_begin) >= skipped_count:
            for v in range(len(versions_begin) - skipped_count):
                g.add_edge(versions_end[v], versions_begin[v + skipped_count])

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
