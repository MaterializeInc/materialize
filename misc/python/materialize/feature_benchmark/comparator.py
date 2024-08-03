# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Generic, TypeVar

from materialize.feature_benchmark.measurement import MeasurementType, MeasurementUnit
from materialize.feature_benchmark.scenario_version import ScenarioVersion
from materialize.terminal import (
    COLOR_BAD,
    COLOR_GOOD,
    with_conditional_formatting,
)

T = TypeVar("T")


class Comparator(Generic[T]):
    def __init__(self, type: MeasurementType, name: str, threshold: float) -> None:
        self.name = name
        self.type = type
        self.threshold = threshold
        self._points: list[T] = []
        self._unit: MeasurementUnit = MeasurementUnit.UNKNOWN
        self.version: ScenarioVersion | None = None

    def append(self, point: T, unit: MeasurementUnit) -> None:
        if self._unit == MeasurementUnit.UNKNOWN:
            self._unit = unit
        else:
            assert (
                self._unit == unit
            ), f"Mix of units in {self.name}: {self._unit} and {unit}"

        self._points.append(point)

    def this(self) -> T:
        return self._points[0]

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

    def set_scenario_version(self, version: ScenarioVersion):
        self.version = version

    def get_scenario_version(self) -> ScenarioVersion:
        assert self.version is not None
        return self.version

    def is_regression(self, threshold: float | None = None) -> bool:
        raise RuntimeError

    def is_strong_regression(self) -> bool:
        return self.is_regression(threshold=self.threshold * 2)

    def ratio(self) -> float | None:
        raise RuntimeError

    def human_readable(self, use_colors: bool) -> str:
        return str(self)


class RelativeThresholdComparator(Comparator[float | None]):
    def ratio(self) -> float | None:
        if self._points[0] is None or self._points[1] is None:
            return None
        else:
            return self._points[0] / self._points[1]

    def is_regression(self, threshold: float | None = None) -> bool:
        if threshold is None:
            threshold = self.threshold

        ratio = self.ratio()

        if ratio is None:
            return False
        if ratio > 1:
            return ratio - 1 > threshold
        else:
            return False

    def human_readable(self, use_colors: bool) -> str:
        assert self.type.is_lower_value_better(), "unexpected metric"

        if self.type.is_amount():
            improvement = "less"
            deterioration = "more"
        else:
            improvement = "faster"
            deterioration = "slower"

        ratio = self.ratio()
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
