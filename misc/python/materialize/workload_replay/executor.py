# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Test and benchmark execution for workload replay.
"""

from __future__ import annotations

import pathlib
import posixpath
import random
import threading
import time
from collections import defaultdict
from typing import Any

from materialize.docker import image_registry
from materialize.mzcompose.composition import Composition, Service
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.test_result import (
    FailedTestExecutionError,
    TestFailureDetails,
)
from materialize.util import PropagatingThread
from materialize.workload_replay.config import (
    LOCATION,
    SEED_RANGE,
    cluster_replica_sizes,
)
from materialize.workload_replay.data import (
    create_ingestions,
    create_initial_data_external,
    create_initial_data_requiring_mz,
    import_captured_data_initial,
    import_captured_data_streaming,
)
from materialize.workload_replay.objects import (
    apply_settings,
    run_create_objects_part_1,
    run_create_objects_part_2,
)
from materialize.workload_replay.replay import continuous_queries
from materialize.workload_replay.stats import (
    compare_table,
    docker_stats,
    plot_docker_stats_compare,
    print_replay_stats,
)
from materialize.workload_replay.util import print_workload_stats, resolve_tag


def wait_for_freshness(c: Composition) -> None:
    print("Waiting for freshness")
    time.sleep(10)
    prev_lagging: set[str] = set()
    while True:
        lagging: set[str] = {
            entry[0]
            for entry in c.sql_query(
                """
            SELECT o.name
            FROM mz_internal.mz_materialization_lag l
            JOIN mz_objects o ON o.id = l.object_id
            WHERE o.name NOT LIKE 'mz_%'
              AND o.id NOT IN (SELECT id FROM mz_sinks)
              AND (l.global_lag IS NULL OR l.global_lag > INTERVAL '10 seconds')
            ORDER BY l.global_lag DESC NULLS FIRST
            LIMIT 5;"""
            )
        }
        if lagging:
            if lagging != prev_lagging:
                print(f"  Lagging: {', '.join(sorted(lagging))}")
                prev_lagging = lagging
            time.sleep(5)
        else:
            break
    print("Freshness complete")


def test(
    c: Composition,
    workload: dict[str, Any],
    file: pathlib.Path,
    factor_initial_data: float,
    factor_ingestions: float,
    factor_queries: float,
    runtime: int,
    verbose: bool,
    create_objects: bool,
    initial_data: bool,
    early_initial_data: bool,
    run_ingestions: bool,
    run_queries: bool,
    max_concurrent_queries: int,
    run_apply_settings: bool = True,
    seed: str | int = 0,
) -> dict[str, Any]:
    """Run a single workload test."""
    print(f"--- {posixpath.relpath(file, LOCATION)}")
    services = set()

    for schemas in workload["databases"].values():
        for objs in schemas.values():
            for connection in objs["connections"].values():
                if connection["type"] == "postgres":
                    services.add("postgres")
                elif connection["type"] == "mysql":
                    services.add("mysql")
                elif connection["type"] == "sql-server":
                    services.add("sql-server")
                elif connection["type"] in ("kafka", "confluent-schema-registry"):
                    services.update(["kafka", "schema-registry", "zookeeper"])
                elif connection["type"] == "ssh-tunnel":
                    services.add("ssh-bastion-host")
                elif connection["type"] == "iceberg-catalog":
                    pass  # handled by setup_polaris_for_iceberg in objects.py
                elif connection["type"] == "aws-privatelink":
                    pass  # can't run outside of cloud
                elif connection["type"] == "aws":
                    pass  # handled together with iceberg-catalog when present
                else:
                    raise ValueError(f"Unhandled connection type {connection['type']}")

    print(f"Required services for connections: {services}")

    c.up(
        "materialized",
        *services,
        Service("testdrive", idle=True),
    )
    print(f"Console available: http://127.0.0.1:{c.port('materialized', 6874)}")

    threads = []
    stop_event = threading.Event()
    stats: dict[str, Any] = {
        "queries": {"total": 0, "failed": 0, "slow": 0},
        "ingestions": {"total": 0, "failed": 0, "slow": 0},
    }
    if create_objects:
        start_time = time.time()
        if run_apply_settings:
            apply_settings(c, workload, verbose)
        run_create_objects_part_1(c, services, workload, verbose, run_apply_settings)
        if not early_initial_data:
            run_create_objects_part_2(c, services, workload, verbose)
        stats["object_creation"] = time.time() - start_time

    captured_initial_data_dir = workload.get("captured_initial_data_dir")
    captured_continuous_data_dir = workload.get("captured_continuous_data_dir")
    created_data = False
    if initial_data:
        print("Creating initial data")
        stats["initial_data"] = {"docker": [], "time": 0.0}
        stats_thread = PropagatingThread(
            target=docker_stats,
            name="docker-stats",
            args=(stats["initial_data"]["docker"], stop_event),
        )
        stats_thread.start()
        try:
            start_time = time.time()
            if captured_initial_data_dir:
                print("Using captured initial data")
                if early_initial_data:
                    created_data = import_captured_data_initial(
                        c,
                        workload,
                        captured_initial_data_dir,
                        factor_initial_data,
                        seed,
                        requires_mz=False,
                    )
                else:
                    created_data = import_captured_data_initial(
                        c,
                        workload,
                        captured_initial_data_dir,
                        factor_initial_data,
                        seed,
                    )
            else:
                created_data = create_initial_data_external(
                    c,
                    workload,
                    factor_initial_data,
                    random.Random(random.randrange(SEED_RANGE)),
                )
            if early_initial_data:
                obj_start = time.time()
                run_create_objects_part_2(c, services, workload, verbose)
                stats["object_creation"] += time.time() - obj_start
            if captured_initial_data_dir and early_initial_data:
                created_data_requiring_mz = import_captured_data_initial(
                    c,
                    workload,
                    captured_initial_data_dir,
                    factor_initial_data,
                    seed,
                    requires_mz=True,
                )
                created_data = created_data or created_data_requiring_mz
            elif not captured_initial_data_dir:
                created_data_requiring_mz = create_initial_data_requiring_mz(
                    c,
                    workload,
                    factor_initial_data,
                    random.Random(random.randrange(SEED_RANGE)),
                )
                created_data = created_data or created_data_requiring_mz
            stats["initial_data"]["time"] = time.time() - start_time
            if not created_data:
                del stats["initial_data"]
        finally:
            stop_event.set()
            stats_thread.join()
            stop_event.clear()
    elif early_initial_data:
        start_time = time.time()
        run_create_objects_part_2(c, services, workload, verbose)
        stats["object_creation"] += time.time() - start_time

    # Wait for all user objects to hydrate before starting queries.
    print("Waiting for hydration")
    prev_not_hydrated: list[str] = []
    while True:
        not_hydrated: list[str] = [
            entry[0]
            for entry in c.sql_query(
                """
            SELECT DISTINCT name
                FROM (
                  SELECT o.name
                  FROM mz_objects o
                  JOIN mz_internal.mz_hydration_statuses h
                    ON o.id = h.object_id
                  WHERE NOT h.hydrated
                    AND o.name NOT LIKE 'mz_%'
                    AND o.id NOT IN (SELECT id FROM mz_sinks)

                  UNION ALL

                  SELECT o.name
                  FROM mz_objects o
                  JOIN mz_internal.mz_compute_hydration_statuses h
                    ON o.id = h.object_id
                  WHERE NOT h.hydrated
                    AND o.name NOT LIKE 'mz_%'
                    AND o.id NOT IN (SELECT id FROM mz_sinks)
                ) x
                ORDER BY 1;"""
            )
        ]
        if not_hydrated:
            if not_hydrated != prev_not_hydrated:
                print(f"  Not yet hydrated: {', '.join(not_hydrated)}")
                prev_not_hydrated = not_hydrated
            time.sleep(1)
        else:
            break
    print("Hydration complete")

    wait_for_freshness(c)

    if run_ingestions:
        print("Starting continuous ingestions")
        if captured_continuous_data_dir:
            print("Using captured continuous data")
            threads.extend(
                import_captured_data_streaming(
                    c,
                    workload,
                    captured_continuous_data_dir,
                    1.0,
                    factor_ingestions,
                    seed,
                    stop_event,
                )
            )
        else:
            threads.extend(
                create_ingestions(
                    c,
                    workload,
                    stop_event,
                    factor_ingestions,
                    verbose,
                    stats["ingestions"],
                )
            )
    if run_queries and workload["queries"]:
        print("Starting continuous queries")
        stats["queries"]["timings"] = []
        stats["queries"]["errors"] = defaultdict(list)
        threads.append(
            PropagatingThread(
                target=continuous_queries,
                name="queries",
                args=(
                    c,
                    workload,
                    stop_event,
                    factor_queries,
                    verbose,
                    stats["queries"],
                    random.Random(random.randrange(SEED_RANGE)),
                    max_concurrent_queries,
                ),
            )
        )
    if threads:
        stats["docker"] = []
        threads.append(
            PropagatingThread(
                target=docker_stats,
                name="docker-stats",
                args=(stats["docker"], stop_event),
            )
        )
        for thread in threads:
            thread.start()

        try:
            stop_event.wait(timeout=runtime)
        finally:
            stop_event.set()
            for thread in threads:
                thread.join()
            print_replay_stats(stats)
    else:
        print("No continuous ingestions or queries defined, skipping phase")

    return stats


def benchmark(
    c: Composition,
    file: pathlib.Path,
    workload: dict,
    compare_against: str,
    factor_initial_data: float,
    factor_ingestions: float,
    factor_queries: float,
    runtime: int,
    verbose: bool,
    seed: str,
    early_initial_data: bool,
    max_concurrent_queries: int,
    run_apply_settings: bool = True,
) -> None:
    """Run a benchmark comparing two versions of Materialize."""
    import random

    services = [
        "materialized",
        "postgres",
        "mysql",
        "sql-server",
        "kafka",
        "schema-registry",
        "zookeeper",
        "ssh-bastion-host",
        "testdrive",
    ]

    # When scale_data is false, use 100% initial data
    settings = workload.get("settings", {})
    if not settings.get("scale_data", True):
        factor_initial_data = 1.0

    print_workload_stats(file, workload)

    tag = resolve_tag(compare_against)
    print(f"-- Running against materialized:{tag} (reference)")
    random.seed(seed)
    with c.override(
        Materialized(
            image=f"{image_registry()}/materialized:{tag}",
            cluster_replica_size=cluster_replica_sizes,
            ports=[6875, 6874, 6876, 6877, 6878, 6880, 6881, 26257],
            environment_extra=["MZ_NO_BUILTIN_CONSOLE=0"],
            additional_system_parameter_defaults={"enable_rbac_checks": "false"},
        )
    ):
        stats_old = test(
            c,
            workload,
            file,
            factor_initial_data,
            factor_ingestions,
            factor_queries,
            runtime,
            verbose,
            True,
            True,
            early_initial_data,
            True,
            True,
            max_concurrent_queries,
            run_apply_settings,
            seed,
        )
        old_version = c.query_mz_version()
    try:
        c.kill(*services)
    except:
        pass
    c.rm(*services, destroy_volumes=True)
    c.rm_volumes("mzdata")
    print("-- Running against current materialized")
    random.seed(seed)
    with c.override(
        Materialized(
            image=None,
            cluster_replica_size=cluster_replica_sizes,
            ports=[6875, 6874, 6876, 6877, 6878, 6880, 6881, 26257],
            environment_extra=["MZ_NO_BUILTIN_CONSOLE=0"],
            additional_system_parameter_defaults={"enable_rbac_checks": "false"},
        )
    ):
        stats_new = test(
            c,
            workload,
            file,
            factor_initial_data,
            factor_ingestions,
            factor_queries,
            runtime,
            verbose,
            True,
            True,
            early_initial_data,
            True,
            True,
            max_concurrent_queries,
            run_apply_settings,
            seed,
        )
        new_version = c.query_mz_version()
    try:
        c.kill(*services)
    except:
        pass
    c.rm(*services, destroy_volumes=True)
    c.rm_volumes("mzdata")
    filename = posixpath.relpath(file, LOCATION)

    print(f"-- Comparing {old_version} against {new_version}")
    plot_docker_stats_compare(
        stats_old=stats_old,
        stats_new=stats_new,
        file=filename,
        old_version=old_version,
        new_version=new_version,
    )
    failures: list[TestFailureDetails] = []
    failures.extend(compare_table(filename, stats_old, stats_new))

    if "errors" in stats_old["queries"]:
        new_errors = []
        for error, occurrences in stats_new["queries"]["errors"].items():
            if error in stats_old["queries"]["errors"]:
                continue
            # XX000: Evaluation error: invalid input syntax for type uuid: invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `V` at 4: "005V"
            if "invalid input syntax for type uuid" in error:
                continue
            new_errors.append(f"{error} in queries: {occurrences}")
        if new_errors:
            failures.append(
                TestFailureDetails(
                    message=f"Workload {filename} has new errors",
                    details="\n".join(new_errors),
                    test_class_name_override=filename,
                )
            )

    if failures:
        raise FailedTestExecutionError(errors=failures)
