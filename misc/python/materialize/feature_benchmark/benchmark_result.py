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

from materialize.feature_benchmark.measurement import MeasurementType, MeasurementUnit
from materialize.feature_benchmark.scenario import Scenario
from materialize.feature_benchmark.scenario_version import ScenarioVersion

T = TypeVar("T")


class BenchmarkScenarioResult:

    def __init__(
        self, scenario_class: type[Scenario], measurement_types: list[MeasurementType]
    ):
        self.scenario_class = scenario_class
        self.scenario_name = scenario_class.__name__
        self.version: ScenarioVersion | None = None
        self.metrics: list[BenchmarkScenarioMetric] = []

        for measurement_type in measurement_types:
            self.metrics.append(
                BenchmarkScenarioMetric(scenario_class, measurement_type)
            )

    def set_scenario_version(self, version: ScenarioVersion):
        self.version = version

    def get_scenario_version(self) -> ScenarioVersion:
        assert self.version is not None
        return self.version

    def empty(self) -> None:
        self.metrics = []

    def is_empty(self) -> bool:
        return len(self.metrics) == 0

    def get_metric_by_measurement_type(
        self, measurement_type: MeasurementType
    ) -> BenchmarkScenarioMetric | None:
        for metric in self.metrics:
            if metric.measurement_type == measurement_type:
                return metric

        return None


class BenchmarkScenarioMetric(Generic[T]):
    def __init__(
        self, scenario_class: type[Scenario], measurement_type: MeasurementType
    ) -> None:
        self.scenario_class = scenario_class
        self.measurement_type = measurement_type
        self._points: list[T] = []
        self._unit: MeasurementUnit = MeasurementUnit.UNKNOWN

    def append_point(
        self, point: T, unit: MeasurementUnit, aggregation_name: str
    ) -> None:
        if self._unit == MeasurementUnit.UNKNOWN:
            self._unit = unit
        elif point is None:
            # ignore unit check
            pass
        else:
            assert (
                self._unit == unit
            ), f"Mix of units in {self.scenario_class.__name__}: {self._unit} and {unit} in {aggregation_name}"

        self._points.append(point)

    def this(self) -> T:
        return self._points[0]

    def points_this(self) -> list[T]:
        return self._points

    def this_as_str(self) -> str:
        if self.this() is None:
            return "           None"
        else:
            return f"{self.this():>11.3f}"

    def other(self) -> T:
        return self._points[1]

    def other_as_str(self) -> str:
        if self.other() is None:
            return "           None"
        else:
            return f"{self.other():>11.3f}"

    def unit(self) -> MeasurementUnit:
        assert self._unit is not None
        return self._unit

    def has_values(self) -> bool:
        return self.this() is not None or self.other() is not None
