# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from statistics import mean, variance
from typing import Generic, TypeVar

from materialize.feature_benchmark.benchmark_result import BenchmarkScenarioResult
from materialize.feature_benchmark.benchmark_result_evaluator import (
    RelativeThresholdEvaluator,
)
from materialize.feature_benchmark.measurement import (
    MeasurementType,
)
from materialize.feature_benchmark.scenario import Scenario
from materialize.feature_benchmark.scenario_version import ScenarioVersion

T = TypeVar("T", bound=int | float)


class ReportMeasurement(Generic[T]):
    result: T | None
    min: T | None
    max: T | None
    mean: T | None
    variance: float | None

    def __init__(self, points: list[T | None]):
        self.result = points[0]
        set_points = [point for point in points if point is not None]
        if self.result is not None and set_points:
            self.min = min(set_points)
            self.max = max(set_points)
            self.mean = mean(set_points)
            self.variance = variance(set_points) if len(set_points) > 1 else None
        else:
            self.min = None
            self.max = None
            self.mean = None
            self.variance = None


class Report:
    def __init__(self, cycle_number: int) -> None:
        self.cycle_number = cycle_number
        """ 1-based cycle number. """
        self._result_by_scenario_name: dict[str, BenchmarkScenarioResult] = dict()

    def add_scenario_result(self, result: BenchmarkScenarioResult) -> None:
        assert (
            result.scenario_name not in self._result_by_scenario_name.keys()
        ), f"Result of scenario {result.scenario_name} already present"
        self._result_by_scenario_name[result.scenario_name] = result

    def get_scenario_names(self) -> list[str]:
        return list(self._result_by_scenario_name.keys())

    def as_string(self, use_colors: bool, limit_to_scenario: str | None = None) -> str:
        output_lines = []

        output_lines.append(
            f"{'NAME':<35} | {'TYPE':<15} | {'THIS':^15} | {'OTHER':^15} | {'UNIT':^6} | {'THRESHOLD':^10} | {'Regression?':^13} | 'THIS' is"
        )
        output_lines.append("-" * 152)

        for scenario_result in self._result_by_scenario_name.values():
            evaluator = RelativeThresholdEvaluator(scenario_result.scenario_class)
            for metric in scenario_result.metrics:
                if not metric.has_values():
                    continue

                if (
                    limit_to_scenario is not None
                    and scenario_result.scenario_name != limit_to_scenario
                ):
                    continue

                regression = "!!YES!!" if evaluator.is_regression(metric) else "no"
                threshold = f"{(evaluator.get_threshold(metric) * 100):.0f}%"
                output_lines.append(
                    f"{scenario_result.scenario_name:<35} | {metric.measurement_type:<15} | {metric.this_as_str():>15} | {metric.other_as_str():>15} | {metric.unit():^6} | {threshold:^10} | {regression:^13} | {evaluator.human_readable(metric, use_colors)}"
                )

        return "\n".join(output_lines)

    def __str__(self) -> str:
        return self.as_string(use_colors=False)

    def measurements_of_this(
        self, scenario_name: str
    ) -> dict[MeasurementType, ReportMeasurement]:
        scenario_result = self.get_scenario_result_by_name(scenario_name)

        this_results = dict()
        for metric in scenario_result.metrics:
            this_results[metric.measurement_type] = ReportMeasurement(
                metric.points_this()
            )

        return this_results

    def get_scenario_version(self, scenario_name: str) -> ScenarioVersion:
        scenario_result = self.get_scenario_result_by_name(scenario_name)
        return scenario_result.get_scenario_version()

    def get_scenario_result_by_name(
        self, scenario_name: str
    ) -> BenchmarkScenarioResult:
        return self._result_by_scenario_name[scenario_name]

    def has_scenario_regression(self, scenario_name: str) -> bool:
        scenario_result = self.get_scenario_result_by_name(scenario_name)
        evaluator = RelativeThresholdEvaluator(scenario_result.scenario_class)

        for metric in scenario_result.metrics:
            if evaluator.is_regression(metric):
                return True

        return False


def determine_scenario_classes_with_regressions(
    selected_report_by_scenario_name: dict[str, Report]
) -> list[type[Scenario]]:
    scenario_classes_with_regressions = set()

    for scenario_name, report in selected_report_by_scenario_name.items():
        if report.has_scenario_regression(scenario_name):
            scenario_result = report.get_scenario_result_by_name(scenario_name)
            scenario_classes_with_regressions.add(scenario_result.scenario_class)

    return list(scenario_classes_with_regressions)
