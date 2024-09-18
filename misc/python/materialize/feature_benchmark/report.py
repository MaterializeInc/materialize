# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from collections.abc import Iterable
from statistics import mean, variance
from typing import Generic, TypeVar

from materialize.feature_benchmark.comparator import Comparator
from materialize.feature_benchmark.measurement import (
    MeasurementType,
)
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
        self._comparisons: list[Comparator] = []

    def add_comparison(self, comparison: Comparator) -> None:
        self._comparisons.append(comparison)

    def add_comparisons(self, comparisons: Iterable[Comparator]) -> None:
        self._comparisons.extend(comparisons)

    def as_string(self, use_colors: bool, limit_to_scenario: str | None = None) -> str:
        output_lines = []

        output_lines.append(
            f"{'NAME':<35} | {'TYPE':<15} | {'THIS':^15} | {'OTHER':^15} | {'UNIT':^6} | {'THRESHOLD':^10} | {'Regression?':^13} | 'THIS' is"
        )
        output_lines.append("-" * 152)

        for comparison in self._comparisons:
            if not comparison.has_values():
                continue

            if (
                limit_to_scenario is not None
                and comparison.scenario_name != limit_to_scenario
            ):
                continue

            regression = "!!YES!!" if comparison.is_regression() else "no"
            threshold = f"{(comparison.threshold * 100):.0f}%"
            output_lines.append(
                f"{comparison.scenario_name:<35} | {comparison.measurement_type:<15} | {comparison.this_as_str():>15} | {comparison.other_as_str():>15} | {comparison.unit():^6} | {threshold:^10} | {regression:^13} | {comparison.human_readable(use_colors)}"
            )

        return "\n".join(output_lines)

    def __str__(self) -> str:
        return self.as_string(use_colors=False)

    def measurements_of_this(
        self, scenario_name: str
    ) -> dict[MeasurementType, ReportMeasurement]:
        result = dict()

        for comparison in self._comparisons:
            if comparison.scenario_name == scenario_name:
                result[comparison.measurement_type] = ReportMeasurement(
                    comparison.points_this()
                )

        return result

    def get_scenario_version(self, scenario_name: str) -> ScenarioVersion:
        for comparison in self._comparisons:
            if comparison.scenario_name == scenario_name:
                return comparison.get_scenario_version()

        raise RuntimeError(f"Scenario {scenario_name} not found!")
