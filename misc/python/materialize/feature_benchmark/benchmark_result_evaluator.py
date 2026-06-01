# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from __future__ import annotations

from typing import Generic, TypeVar

from materialize.feature_benchmark.benchmark_result import BenchmarkScenarioMetric
from materialize.feature_benchmark.measurement import MeasurementType
from materialize.feature_benchmark.scenario import Scenario
from materialize.terminal import (
    COLOR_BAD,
    COLOR_GOOD,
    with_conditional_formatting,
)

T = TypeVar("T")


class BenchmarkResultEvaluator(Generic[T]):

    def ratio(self, metric: BenchmarkScenarioMetric) -> float | None:
        raise RuntimeError

    def is_regression(
        self, metric: BenchmarkScenarioMetric, threshold: float | None = None
    ) -> bool:
        raise RuntimeError

    def is_strong_regression(self, metric: BenchmarkScenarioMetric) -> bool:
        raise RuntimeError

    def human_readable(self, metric: BenchmarkScenarioMetric, use_colors: bool) -> str:
        raise RuntimeError


class RelativeThresholdEvaluator(BenchmarkResultEvaluator[float | None]):

    def __init__(self, scenario_class: type[Scenario]) -> None:
        self.threshold_by_measurement_type: dict[MeasurementType, float] = (
            scenario_class.RELATIVE_THRESHOLD
        )

    def get_threshold(self, metric: BenchmarkScenarioMetric) -> float:
        return self.threshold_by_measurement_type[metric.measurement_type]

    def ratio(self, metric: BenchmarkScenarioMetric) -> float | None:
        if metric._points[0] is None or metric._points[1] is None:
            return None
        else:
            return metric._points[0] / metric._points[1]

    def is_regression(
        self, metric: BenchmarkScenarioMetric, threshold: float | None = None
    ) -> bool:
        if threshold is None:
            threshold = self.get_threshold(metric)

        ratio = self.ratio(metric)

        if ratio is None:
            return False
        if ratio > 1:
            return ratio - 1 > threshold
        else:
            return False

    def is_strong_regression(self, metric: BenchmarkScenarioMetric) -> bool:
        return self.is_regression(
            metric,
            threshold=self.get_threshold(metric) * 2,
        )

    def human_readable(self, metric: BenchmarkScenarioMetric, use_colors: bool) -> str:
        assert metric.measurement_type.is_lower_value_better(), "unexpected metric"

        if metric.measurement_type.is_amount():
            improvement = "less"
            deterioration = "more"
        else:
            improvement = "faster"
            deterioration = "slower"

        ratio = self.ratio(metric)
        if ratio is None:
            return "not comparable"
        if ratio >= 2:
            return with_conditional_formatting(
                f"worse:  {ratio:4.1f} TIMES {deterioration}",
                COLOR_BAD,
                condition=use_colors,
            )
        elif ratio > 1:
            return with_conditional_formatting(
                f"worse:  {-(1-ratio)*100:4.1f}% {deterioration}",
                COLOR_BAD,
                condition=use_colors,
            )
        elif ratio == 1:
            return "the same"
        elif ratio > 0.5:
            return with_conditional_formatting(
                f"better: {(1-ratio)*100:4.1f}% {improvement}",
                COLOR_GOOD,
                condition=use_colors,
            )
        else:
            return with_conditional_formatting(
                f"better: {(1/ratio):4.1f} times {improvement}",
                COLOR_GOOD,
                condition=use_colors,
            )
