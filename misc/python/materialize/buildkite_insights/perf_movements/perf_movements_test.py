# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Fixtures are copied verbatim from nightly build 18282, so a change to any printer
these parsers depend on shows up here rather than as a silently empty report."""

from __future__ import annotations

import json

from materialize.buildkite_insights.perf_movements import spec_sheet
from materialize.buildkite_insights.perf_movements.log_parsers import (
    normalize_log_text,
    parse_feature_benchmark_log,
    parse_parallel_benchmark_log,
    parse_scalability_log,
)
from materialize.buildkite_insights.perf_movements.movement import (
    BaselineKind,
    Direction,
    JobRef,
    Movement,
)
from materialize.buildkite_insights.perf_movements.perf_movements import select

JOB = JobRef(
    pipeline_slug="nightly",
    build_number=18282,
    job_id="01a08da8-58a4-44af-868e-2a3f335a3319",
    step_key="feature-benchmark",
)

FEATURE_BENCHMARK_LOG = """
--- Benchmark results
NAME                                | TYPE            |      THIS       |      OTHER      |  UNIT  | THRESHOLD  |  Regression?  | 'THIS' is
--------------------------------------------------------------------------------------------------------------------------------------------------------
CountDistinct                       | wallclock       |           1.297 |           1.317 |   s    |    10%     |      no       | better:  1.5% faster
CountDistinct                       | memory_mz       |         685.918 |         656.168 |   MB   |    20%     |      no       | worse:   4.5% more
InsertBatch                         | memory_clusterd |          44.984 |          34.184 |   MB   |    50%     |      no       | worse:  31.6% more
Uncomparable                        | wallclock       |             N/A |             N/A |   s    |    10%     |      no       | not comparable
+++ Done
"""

PARALLEL_BENCHMARK_LOG = """
Comparing scenario StaggeredReads
QUERY                                    | STAT    |     THIS     |    OTHER     |  CHANGE   | THRESHOLD | REGRESSION?
----------------------------------------------------------------------------------------------------------------------
SELECT 1 (reuse connection)              | avg     |      2450.15 |      2800.56 |   -12.51% |       20% |      no
SELECT 1 (reuse connection)              | max     |      6675.73 |      6945.49 |    -3.88% |           |
SELECT 1 (reuse connection)              | qps     |         3.69 |         3.23 |    14.10% |       20% |      no
"""

SCALABILITY_LOG = """
Regressions:
* Regression in workload 'ReadWorkload' at concurrency 8 with MaterializeContainer (None specified as HEAD): 800.0 tps vs. 1000.0 tps (-200.0 tps; -20.0%)
Improvements:
* Scalability improvement in workload 'InsertAndSelectCountInMvWorkload' at concurrency 4 with MaterializeContainer (None specified as HEAD): 1202.76 tps vs. 927.45 tps (275.32 tps; 29.69%)
"""

SCALABILITY_LOG_WITHOUT_CHANGES = """
Regressions:
* None
Improvements:
* None
"""

CLUSTER_CSV = """scenario,scenario_version,scale,mode,category,test_name,cluster_size,repetition,size_bytes,time_ms
join,1.0.0,4,strong,hydration,tpch_join,25cc,0,1000,100
join,1.0.0,4,strong,hydration,tpch_join,25cc,1,1000,140
"""

ENVD_CSV = """scenario,scenario_version,scale,mode,category,test_name,envd_cpus,repetition,qps
peek,1.0.0,4,strong,qps,peek_qps,8,0,500
peek,1.0.0,4,strong,qps,peek_qps,8,1,700
"""


def _by_key(movements: list[Movement]) -> dict[tuple[str, str], Movement]:
    return {(m.scenario, m.metric): m for m in movements}


def test_feature_benchmark_parses_every_comparable_row() -> None:
    movements = _by_key(parse_feature_benchmark_log(FEATURE_BENCHMARK_LOG, JOB))

    assert set(movements) == {
        ("CountDistinct", "wallclock"),
        ("CountDistinct", "memory_mz"),
        ("InsertBatch", "memory_clusterd"),
    }, "a row with unparseable values must be dropped, everything else kept"


def test_feature_benchmark_reads_direction_and_threshold() -> None:
    movements = _by_key(parse_feature_benchmark_log(FEATURE_BENCHMARK_LOG, JOB))

    faster = movements[("CountDistinct", "wallclock")]
    assert faster.direction == Direction.LOWER_IS_BETTER
    assert faster.baseline_kind == BaselineKind.JOB_COMPARISON
    assert faster.unit == "s"
    assert faster.threshold_percent == 10.0
    assert faster.reported_regression is False
    assert faster.is_improvement, "1.297s against 1.317s is faster"

    more_memory = movements[("InsertBatch", "memory_clusterd")]
    assert more_memory.is_deterioration
    assert round(more_memory.change_percent, 1) == 31.6


def test_feature_benchmark_later_table_wins() -> None:
    rerun = FEATURE_BENCHMARK_LOG + """
NAME                                | TYPE            |      THIS       |      OTHER      |  UNIT  | THRESHOLD  |  Regression?  | 'THIS' is
--------------------------------------------------------------------------------------------------------------------------------------------------------
CountDistinct                       | wallclock       |           2.000 |           1.317 |   s    |    10%     |    !!YES!!    | worse:  51.9% slower
"""

    movements = _by_key(parse_feature_benchmark_log(rerun, JOB))
    rerun_row = movements[("CountDistinct", "wallclock")]

    assert rerun_row.this == 2.000, "the rerun's verdict replaces the first cycle's"
    assert rerun_row.reported_regression is True


def test_parallel_benchmark_direction_follows_the_statistic() -> None:
    movements = _by_key(parse_parallel_benchmark_log(PARALLEL_BENCHMARK_LOG, JOB))

    latency = movements[("StaggeredReads/SELECT 1 (reuse connection)", "avg")]
    assert latency.direction == Direction.LOWER_IS_BETTER
    assert latency.is_improvement, "a lower average latency is better"

    throughput = movements[("StaggeredReads/SELECT 1 (reuse connection)", "qps")]
    assert throughput.direction == Direction.HIGHER_IS_BETTER
    assert throughput.is_improvement, "a higher qps is better despite the same sign"


def test_log_in_buildkite_json_form_is_parsed() -> None:
    """The MCP server saves job logs as `{"entries": [{"c": ...}]}` while announcing
    plain text. Unnormalized, the line-anchored regexes match nothing and the report is
    empty rather than failing, so this is the shape most likely to go unnoticed."""
    as_json = json.dumps(
        {
            "entries": [
                {"c": line, "rn": index, "ts": 0}
                for index, line in enumerate(FEATURE_BENCHMARK_LOG.splitlines())
            ]
        }
    )

    from_json = parse_feature_benchmark_log(as_json, JOB)
    from_text = parse_feature_benchmark_log(FEATURE_BENCHMARK_LOG, JOB)

    assert from_json and _by_key(from_json).keys() == _by_key(from_text).keys()


def test_log_in_raw_api_form_is_parsed() -> None:
    as_json = json.dumps({"content": FEATURE_BENCHMARK_LOG})

    assert len(parse_feature_benchmark_log(as_json, JOB)) == 3


def test_normalize_leaves_plain_text_alone() -> None:
    assert normalize_log_text(FEATURE_BENCHMARK_LOG) == FEATURE_BENCHMARK_LOG
    assert normalize_log_text("{not json after all") == "{not json after all"


def test_parallel_benchmark_units_and_skipped_statistics() -> None:
    log = """
Comparing scenario Mixed
QUERY                                    | STAT    |     THIS     |    OTHER     |  CHANGE   | THRESHOLD | REGRESSION?
----------------------------------------------------------------------------------------------------------------------
SELECT 1                                 | qps     |         3.69 |         3.23 |    14.10% |       20% |      no
SELECT 1                                 | p95     |      3424.58 |      6927.95 |   -50.57% |       30% |      no
SELECT 1                                 | queries |       1000.0 |        900.0 |    11.11% |           |
SELECT 1                                 | slope   |         0.01 |         0.02 |   -50.00% |           |
"""
    movements = _by_key(parse_parallel_benchmark_log(log, JOB))

    assert ("Mixed/SELECT 1", "slope") not in movements, (
        "a slope ratio against a near-zero baseline is not a meaningful movement"
    )
    assert movements[("Mixed/SELECT 1", "qps")].unit == "qps"
    assert movements[("Mixed/SELECT 1", "p95")].unit == "ms"

    executed = movements[("Mixed/SELECT 1", "queries")]
    assert executed.direction == Direction.HIGHER_IS_BETTER
    assert executed.is_improvement, "more queries completed in the load phase is better"


def test_parallel_benchmark_ungated_statistic_has_no_verdict() -> None:
    movements = _by_key(parse_parallel_benchmark_log(PARALLEL_BENCHMARK_LOG, JOB))
    ungated = movements[("StaggeredReads/SELECT 1 (reuse connection)", "max")]

    assert ungated.threshold_percent is None
    assert ungated.reported_regression is None


def test_scalability_reads_both_directions() -> None:
    movements = _by_key(parse_scalability_log(SCALABILITY_LOG, JOB))

    regression = movements[("ReadWorkload @ concurrency 8", "tps")]
    assert regression.reported_regression is True
    assert regression.is_deterioration, "fewer tps is worse"

    improvement = movements[
        ("InsertAndSelectCountInMvWorkload @ concurrency 4", "tps")
    ]
    assert improvement.reported_regression is False
    assert improvement.is_improvement
    assert round(improvement.change_percent, 2) == 29.68


def test_scalability_ignores_the_empty_bullet() -> None:
    assert parse_scalability_log(SCALABILITY_LOG_WITHOUT_CHANGES, JOB) == []


def test_spec_sheet_takes_the_median_of_repetitions() -> None:
    medians = spec_sheet.median_per_key(spec_sheet.parse_results_csv(CLUSTER_CSV))

    key, metric = next(k for k in medians if k[1] == "time_ms")
    assert medians[(key, metric)] == 120.0
    assert key.dimension_name == "cluster_size"
    assert key.dimension == "25cc"


def test_spec_sheet_reads_the_environmentd_schema() -> None:
    medians = spec_sheet.median_per_key(spec_sheet.parse_results_csv(ENVD_CSV))

    key, metric = next(iter(medians))
    assert metric == "qps"
    assert key.dimension_name == "envd_cpus"
    assert medians[(key, metric)] == 600.0


def test_spec_sheet_reads_crlf_rows() -> None:
    """Buildkite serves these artifacts with CRLF line endings, and the metric is the
    last column, so a stray carriage return would land on the value being parsed."""
    crlf = (
        "scenario,scenario_version,scale,mode,category,test_name,envd_cpus,repetition,qps\r\n"
        "qps_envd_strong_scaling,1.0.0,1,strong,peek_qps,dbbench_256_conns,1,0,453.521\r\n"
    )

    medians = spec_sheet.median_per_key(spec_sheet.parse_results_csv(crlf))

    key, metric = next(iter(medians))
    assert metric == "qps"
    assert medians[(key, metric)] == 453.521
    assert key.test_name == "dbbench_256_conns"


def test_spec_sheet_needs_enough_history() -> None:
    latest = spec_sheet.median_per_key(spec_sheet.parse_results_csv(CLUSTER_CSV))
    history = [latest, latest]

    assert (
        spec_sheet.compare_to_window(latest, history, JOB, min_history=3) == []
    ), "a key with two earlier observations is not reported at min_history=3"

    movements = spec_sheet.compare_to_window(latest, history, JOB, min_history=2)
    assert {m.metric for m in movements} == {"time_ms", "size_bytes"}
    assert all(m.baseline_kind == BaselineKind.WINDOW_MEDIAN for m in movements)


def test_spec_sheet_window_baseline_is_the_median_of_builds() -> None:
    key = next(
        k for k in spec_sheet.median_per_key(spec_sheet.parse_results_csv(CLUSTER_CSV))
    )
    latest = {key: 200.0}
    history = [{key: 100.0}, {key: 110.0}, {key: 300.0}]

    movement = spec_sheet.compare_to_window(latest, history, JOB, min_history=3)[0]

    assert movement.baseline == 110.0, "the median must resist the 300.0 outlier"
    assert movement.is_deterioration, "time_ms rising is worse"


def test_select_ranks_deteriorations_before_improvements() -> None:
    movements = parse_scalability_log(SCALABILITY_LOG, JOB)

    ranked = select(movements, threshold_percent=5.0, only="all")

    assert [m.is_improvement for m in ranked] == [False, True]


def test_select_keeps_a_published_regression_below_the_threshold() -> None:
    small_regression = Movement(
        job=JOB,
        scenario="Tiny",
        metric="wallclock",
        this=1.01,
        baseline=1.0,
        direction=Direction.LOWER_IS_BETTER,
        baseline_kind=BaselineKind.JOB_COMPARISON,
        threshold_percent=0.5,
        reported_regression=True,
    )

    assert select([small_regression], threshold_percent=50.0, only="all") == [
        small_regression
    ], "the step's own gate outranks the caller's threshold"


def test_select_reports_movements_the_step_gate_tolerates() -> None:
    movements = parse_feature_benchmark_log(FEATURE_BENCHMARK_LOG, JOB)

    ranked = select(movements, threshold_percent=1.0, only="all")

    assert len(ranked) == 3, (
        "every comparable row moved more than 1% and none reached its own 10-50% gate, "
        "so a step threshold must not suppress them"
    )
    assert [round(m.change_percent, 1) for m in ranked] == [31.6, 4.5, -1.5]


def test_select_filters_to_one_direction() -> None:
    movements = parse_scalability_log(SCALABILITY_LOG, JOB)

    only_regressions = select(movements, threshold_percent=5.0, only="regressions")

    assert [m.scenario for m in only_regressions] == ["ReadWorkload @ concurrency 8"]
