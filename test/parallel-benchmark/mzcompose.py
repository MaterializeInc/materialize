# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Benchmark with scenarios combining closed and open loops, can run multiple
actions concurrently, measures various kinds of statistics.
"""

import gc
import os
import time
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy
from matplotlib.markers import MarkerStyle

from materialize import MZ_ROOT, buildkite
from materialize.mzcompose.composition import Composition, WorkflowArgumentParser
from materialize.mzcompose.services.balancerd import Balancerd
from materialize.mzcompose.services.cockroach import Cockroach
from materialize.mzcompose.services.kafka import Kafka as KafkaService
from materialize.mzcompose.services.kgen import Kgen as KgenService
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.minio import Minio
from materialize.mzcompose.services.mysql import MySql
from materialize.mzcompose.services.mz import Mz
from materialize.mzcompose.services.postgres import Postgres
from materialize.mzcompose.services.redpanda import Redpanda
from materialize.mzcompose.services.schema_registry import SchemaRegistry
from materialize.mzcompose.services.testdrive import Testdrive
from materialize.mzcompose.services.zookeeper import Zookeeper
from materialize.mzcompose.test_result import (
    FailedTestExecutionError,
    TestFailureDetails,
)
from materialize.parallel_benchmark.framework import (
    LoadPhase,
    Measurement,
    Scenario,
    State,
)
from materialize.parallel_benchmark.scenarios import *  # noqa: F401 F403
from materialize.test_analytics.config.test_analytics_db_config import (
    create_test_analytics_config,
)
from materialize.test_analytics.data.parallel_benchmark import (
    parallel_benchmark_result_storage,
)
from materialize.test_analytics.test_analytics_db import TestAnalyticsDb
from materialize.util import PgConnInfo, all_subclasses, parse_pg_conn_string
from materialize.version_list import resolve_ancestor_image_tag

PARALLEL_BENCHMARK_FRAMEWORK_VERSION = "1.1.0"


def known_regression(scenario: str, other_tag: str) -> bool:
    return False


REGRESSION_THRESHOLDS = {
    "queries": None,
    "qps": 1.2,
    "max": None,
    "min": None,
    "avg": 1.2,
    "p50": 1.2,
    "p95": 1.3,
    "p99": None,
    "p99_9": None,
    "p99_99": None,
    "p99_999": None,
    "p99_9999": None,
    "p99_99999": None,
    "p99_999999": None,
    "std": None,
    "slope": None,
}

SERVICES = [
    Zookeeper(),
    KafkaService(),
    SchemaRegistry(),
    Redpanda(),
    Cockroach(setup_materialize=True),
    Minio(setup_materialize=True),
    KgenService(),
    Postgres(),
    MySql(),
    Balancerd(),
    # Overridden below
    Materialized(),
    Testdrive(no_reset=True, seed=1),
    Mz(app_password=""),
]


class Statistics:
    def __init__(self, times: list[float], durations: list[float]):
        assert len(times) == len(durations)
        self.queries: int = len(times)
        self.qps: float = len(times) / max(times)
        self.max: float = max(durations)
        self.min: float = min(durations)
        self.avg: float = float(numpy.mean(durations))
        self.p50: float = float(numpy.median(durations))
        self.p95: float = float(numpy.percentile(durations, 95))
        self.p99: float = float(numpy.percentile(durations, 99))
        self.p99_9: float = float(numpy.percentile(durations, 99.9))
        self.p99_99: float = float(numpy.percentile(durations, 99.99))
        self.p99_999: float = float(numpy.percentile(durations, 99.999))
        self.p99_9999: float = float(numpy.percentile(durations, 99.9999))
        self.p99_99999: float = float(numpy.percentile(durations, 99.99999))
        self.p99_999999: float = float(numpy.percentile(durations, 99.999999))
        self.std: float = float(numpy.std(durations, ddof=1))
        self.slope: float = float(numpy.polyfit(times, durations, 1)[0])

    def __str__(self) -> str:
        return f"""  queries: {self.queries:>5}
  qps: {self.qps:>7.2f}
  min: {self.min:>7.2f}ms
  avg: {self.avg:>7.2f}ms
  p50: {self.p50:>7.2f}ms
  p95: {self.p95:>7.2f}ms
  p99: {self.p99:>7.2f}ms
  max: {self.max:>7.2f}ms
  std: {self.std:>7.2f}ms
  slope: {self.slope:>5.4f}"""

    def __dir__(self) -> list[str]:
        return [
            "queries",
            "qps",
            "max",
            "min",
            "avg",
            "p50",
            "p95",
            "p99",
            "std",
            "slope",
        ]


def report(
    mz_string: str,
    scenario: Scenario,
    measurements: dict[str, list[Measurement]],
    start_time: float,
    guarantees: bool,
    suffix: str,
) -> tuple[dict[str, Statistics], list[TestFailureDetails]]:
    scenario_name = type(scenario).name()
    stats: dict[str, Statistics] = {}
    failures: list[TestFailureDetails] = []
    plt.figure(figsize=(10, 6))
    for key, m in measurements.items():
        times: list[float] = [x.timestamp - start_time for x in m]
        durations: list[float] = [x.duration * 1000 for x in m]
        stats[key] = Statistics(times, durations)
        plt.scatter(times, durations, label=key, marker=MarkerStyle("+"))
        print(f"Statistics for {key}:\n{stats[key]}")
        if key in scenario.guarantees and guarantees:
            for stat, guarantee in scenario.guarantees[key].items():
                duration = getattr(stats[key], stat)
                less_than = less_than_is_regression(stat)
                if duration < guarantee if less_than else duration > guarantee:
                    failure = f"Scenario {scenario_name} failed: {key}: {stat}: {duration:.2f} {'<' if less_than else '>'} {guarantee:.2f}"
                    print(failure)
                    failures.append(
                        TestFailureDetails(
                            message=failure,
                            details=str(stats[key]),
                            test_class_name_override=scenario_name,
                        )
                    )
                else:
                    print(
                        f"Scenario {scenario_name} succeeded: {key}: {stat}: {duration:.2f} {'>=' if less_than else '<='} {guarantee:.2f}"
                    )

    plt.xlabel("time [s]")
    plt.ylabel("latency [ms]")
    plt.title(f"{scenario_name} against {mz_string}")
    plt.legend(loc="best")
    plt.grid(True)
    plt.ylim(bottom=0)
    plot_path = f"plots/{scenario_name}_{suffix}.png"
    plt.savefig(MZ_ROOT / plot_path, dpi=300)
    if buildkite.is_in_buildkite():
        buildkite.upload_artifact(plot_path, cwd=MZ_ROOT)
        print(f"+++ Plot for {scenario_name}")
        print(
            buildkite.inline_image(
                f"artifact://{plot_path}", f"Plot for {scenario_name}"
            )
        )
    else:
        print(f"Saving plot to {plot_path}")

    return stats, failures


def run_once(
    c: Composition,
    scenarios: list[type[Scenario]],
    service_names: list[str],
    tag: str | None,
    params: str | None,
    args,
    suffix: str,
) -> tuple[dict[Scenario, dict[str, Statistics]], list[TestFailureDetails]]:
    stats: dict[Scenario, dict[str, Statistics]] = {}
    failures: list[TestFailureDetails] = []

    overrides = []

    if args.mz_url:
        overrides = [
            Testdrive(
                no_reset=True,
                materialize_url=args.mz_url,
                no_consistency_checks=True,
            )
        ]
    else:
        mz_image = f"materialize/materialized:{tag}" if tag else None
        overrides = [
            Materialized(
                image=mz_image,
                default_size=args.size,
                soft_assertions=False,
                external_cockroach=True,
                external_minio=True,
                sanity_restart=False,
                additional_system_parameter_defaults={
                    "enable_statement_lifecycle_logging": "false",
                    "statement_logging_default_sample_rate": "0",
                    "statement_logging_max_sample_rate": "0",
                },
            )
        ]

    c.silent = True

    with c.override(*overrides):
        for scenario_class in scenarios:
            scenario_name = scenario_class.name()
            print(f"--- Running scenario {scenario_name}")

            if args.mz_url:
                target = parse_pg_conn_string(args.mz_url)
                c.up("testdrive", persistent=True)
                conn_infos = {"materialized": target}
                conn = target.connect()
                with conn.cursor() as cur:
                    cur.execute("SELECT mz_version()")
                    mz_version = cur.fetchall()[0][0]
                conn.close()
                mz_string = f"{mz_version} ({target.host})"
            else:
                c.up(*service_names)
                c.up("testdrive", persistent=True)
                c.sql(
                    "ALTER SYSTEM SET max_connections = 1000000",
                    user="mz_system",
                    port=6877,
                )

                mz_version = c.query_mz_version()
                mz_string = f"{mz_version} (docker)"
                conn_infos = {
                    "materialized": PgConnInfo(
                        user="materialize",
                        database="materialize",
                        host="127.0.0.1",
                        port=c.default_port("materialized"),
                    ),
                    "mz_system": PgConnInfo(
                        user="mz_system",
                        database="materialize",
                        host="127.0.0.1",
                        port=c.port("materialized", 6877),
                    ),
                    "postgres": PgConnInfo(
                        user="postgres",
                        password="postgres",
                        database="postgres",
                        host="127.0.0.1",
                        port=c.default_port("postgres"),
                    ),
                }

            state = State(
                measurements=defaultdict(list),
                load_phase_duration=args.load_phase_duration,
                periodic_dists={pd[0]: int(pd[1]) for pd in args.periodic_dist or []},
            )
            scenario = scenario_class(c, conn_infos)
            scenario.setup(c, conn_infos)
            start_time = time.time()
            Path(MZ_ROOT / "plots").mkdir(parents=True, exist_ok=True)
            try:
                # Don't let the garbage collector interfere with our measurements
                gc.disable()
                scenario.run(c, state)
                scenario.teardown()
                gc.collect()
                gc.enable()
            finally:
                new_stats, new_failures = report(
                    mz_string,
                    scenario,
                    state.measurements,
                    start_time,
                    args.guarantees,
                    suffix,
                )
                failures.extend(new_failures)
                stats[scenario] = new_stats

            if not args.mz_url:
                c.kill("cockroach", "materialized", "testdrive")
                c.rm("cockroach", "materialized", "testdrive")
                c.rm_volumes("mzdata")

    return stats, failures


def less_than_is_regression(stat: str) -> bool:
    return stat == "qps"


def check_regressions(
    this_stats: dict[Scenario, dict[str, Statistics]],
    other_stats: dict[Scenario, dict[str, Statistics]],
    other_tag: str,
) -> list[TestFailureDetails]:
    failures: list[TestFailureDetails] = []

    assert len(this_stats) == len(other_stats)

    for scenario, other_scenario in zip(this_stats.keys(), other_stats.keys()):
        scenario_name = type(scenario).name()
        assert type(other_scenario).name() == scenario_name
        has_failed = False
        print(f"Comparing scenario {scenario_name}")
        output_lines = [
            f"{'QUERY':<40} | {'STAT':<7} | {'THIS':^12} | {'OTHER':^12} | {'CHANGE':^9} | {'THRESHOLD':^9} | {'REGRESSION?':^12}",
            "-" * 118,
        ]

        ignored_queries = set()
        for phase in scenario.phases:
            # We only care about LoadPhases, and only they have report_regressions
            if not isinstance(phase, LoadPhase):
                continue
            for phase_action in phase.phase_actions:
                if not phase_action.report_regressions:
                    ignored_queries.add(str(phase_action.action))

        for query in this_stats[scenario].keys():
            for stat in dir(this_stats[scenario][query]):
                this_value = getattr(this_stats[scenario][query], stat)
                other_value = getattr(other_stats[other_scenario][query], stat)
                less_than = less_than_is_regression(stat)
                percentage = f"{(this_value / other_value - 1) * 100:.2f}%"
                threshold = (
                    None
                    if query in ignored_queries
                    else (
                        scenario.regression_thresholds.get(query, {}).get(stat)
                        or REGRESSION_THRESHOLDS[stat]
                    )
                )
                if threshold is None:
                    regression = ""
                elif (
                    this_value < other_value / threshold
                    if less_than
                    else this_value > other_value * threshold
                ):
                    regression = "!!YES!!"
                    if not known_regression(scenario_name, other_tag):
                        has_failed = True
                else:
                    regression = "no"
                threshold_text = (
                    f"{((threshold - 1) * 100):.0f}%" if threshold is not None else ""
                )
                output_lines.append(
                    f"{query[:40]:<40} | {stat:<7} | {this_value:>12.2f} | {other_value:>12.2f} | {percentage:>9} | {threshold_text:>9} | {regression:^12}"
                )

        print("\n".join(output_lines))
        if has_failed:
            failures.append(
                TestFailureDetails(
                    message=f"Scenario {scenario_name} regressed",
                    details="\n".join(output_lines),
                    test_class_name_override=scenario_name,
                )
            )

    return failures


def resolve_tag(tag: str) -> str:
    if tag == "common-ancestor":
        # TODO: We probably will need overrides too
        return resolve_ancestor_image_tag({})
    return tag


def upload_results_to_test_analytics(
    c: Composition,
    load_phase_duration: int | None,
    stats: dict[Scenario, dict[str, Statistics]],
    was_successful: bool,
) -> None:
    if not buildkite.is_in_buildkite():
        return

    test_analytics = TestAnalyticsDb(create_test_analytics_config(c))
    test_analytics.builds.add_build_job(was_successful=was_successful)

    result_entries = []

    for scenario in stats.keys():
        scenario_name = type(scenario).name()
        scenario_version = scenario.version

        for query in stats[scenario].keys():
            result_entries.append(
                parallel_benchmark_result_storage.ParallelBenchmarkResultEntry(
                    scenario_name=scenario_name,
                    scenario_version=str(scenario_version),
                    query=query,
                    load_phase_duration=load_phase_duration,
                    queries=stats[scenario][query].queries,
                    qps=stats[scenario][query].qps,
                    min=stats[scenario][query].min,
                    max=stats[scenario][query].max,
                    avg=stats[scenario][query].avg,
                    p50=stats[scenario][query].p50,
                    p95=stats[scenario][query].p95,
                    p99=stats[scenario][query].p99,
                    p99_9=stats[scenario][query].p99_9,
                    p99_99=stats[scenario][query].p99_99,
                    p99_999=stats[scenario][query].p99_999,
                    p99_9999=stats[scenario][query].p99_9999,
                    p99_99999=stats[scenario][query].p99_99999,
                    p99_999999=stats[scenario][query].p99_999999,
                    std=stats[scenario][query].std,
                    slope=stats[scenario][query].slope,
                )
            )

    test_analytics.parallel_benchmark_results.add_result(
        framework_version=PARALLEL_BENCHMARK_FRAMEWORK_VERSION,
        results=result_entries,
    )

    try:
        test_analytics.submit_updates()
        print("Uploaded results.")
    except Exception as e:
        # An error during an upload must never cause the build to fail
        test_analytics.on_upload_failed(e)


def workflow_default(c: Composition, parser: WorkflowArgumentParser) -> None:
    c.silent = True

    parser.add_argument(
        "--redpanda",
        action="store_true",
        help="run against Redpanda instead of the Confluent Platform",
    )

    parser.add_argument(
        "--guarantees",
        action="store_true",
        default=True,
        help="Check guarantees defined by test scenarios",
    )

    parser.add_argument(
        "--size",
        metavar="N-N",
        type=str,
        default="1",
        help="default SIZE",
    )

    parser.add_argument(
        "--scenario",
        metavar="SCENARIO",
        action="append",
        type=str,
        help="Scenario to run",
    )

    parser.add_argument(
        "--load-phase-duration",
        type=int,
        help="Override durations of LoadPhases",
    )

    parser.add_argument(
        "--periodic-dist",
        nargs=2,
        metavar=("action", "per_second"),
        action="append",
        help="Override periodic distribution for an action with specified name",
    )

    parser.add_argument(
        "--this-params",
        metavar="PARAMS",
        type=str,
        default=os.getenv("THIS_PARAMS", None),
        help="Semicolon-separated list of parameter=value pairs to apply to the 'THIS' Mz instance",
    )

    parser.add_argument(
        "--other-tag",
        metavar="TAG",
        type=str,
        default=None,
        help="'Other' Materialize container tag to benchmark. If not provided, the last released Mz version will be used.",
    )

    parser.add_argument(
        "--other-params",
        metavar="PARAMS",
        type=str,
        default=os.getenv("OTHER_PARAMS", None),
        help="Semicolon-separated list of parameter=value pairs to apply to the 'OTHER' Mz instance",
    )

    parser.add_argument("--mz-url", type=str, help="Remote Mz instance to run against")

    args = parser.parse_args()

    if args.scenario:
        for scenario in args.scenario:
            assert scenario in globals(), f"scenario {scenario} does not exist"
        scenarios: list[type[Scenario]] = [
            globals()[scenario] for scenario in args.scenario
        ]
    else:
        scenarios = list(all_subclasses(Scenario))

    sharded_scenarios = buildkite.shard_list(scenarios, lambda s: s.name())

    service_names = ["materialized", "postgres", "mysql"] + (
        ["redpanda"] if args.redpanda else ["zookeeper", "kafka", "schema-registry"]
    )

    this_stats, failures = run_once(
        c,
        sharded_scenarios,
        service_names,
        tag=None,
        params=args.this_params,
        args=args,
        suffix="this",
    )
    if args.other_tag:
        assert not args.mz_url, "Can't set both --mz-url and --other-tag"
        tag = resolve_tag(args.other_tag)
        print(f"--- Running against other tag for comparison: {tag}")
        args.guarantees = False
        other_stats, other_failures = run_once(
            c,
            sharded_scenarios,
            service_names,
            tag=tag,
            params=args.other_params,
            args=args,
            suffix="other",
        )
        failures.extend(other_failures)
        failures.extend(check_regressions(this_stats, other_stats, tag))

    upload_results_to_test_analytics(
        c, args.load_phase_duration, this_stats, not failures
    )

    if failures:
        raise FailedTestExecutionError(errors=failures)


# TODO: 24 hour runs (also against real staging with sources, similar to QA canary)
#       Set up remote sources, share with QA canary pipeline, use concurrency group, start first 24 hour runs
#       Maybe also set up the real rr-bench there as a separate step
# TODO: Choose an existing cluster name (for remote mz)
# TODO: Measure Memory?
