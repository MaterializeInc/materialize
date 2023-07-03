# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Generic, List, Protocol, TypeVar

from materialize.feature_benchmark.measurement import MeasurementType

T = TypeVar("T")


class Comparator(Generic[T]):
    def __init__(self, type: MeasurementType, name: str, threshold: float) -> None:
        self.name = name
        self.type = type
        self.threshold = threshold
        self._points: List[T] = []

    def append(self, point: T) -> None:
        self._points.append(point)

    def this(self) -> T:
        return self._points[0]

    def other(self) -> T:
        return self._points[1]

    def is_regression(self) -> bool:
        assert False

    def ratio(self) -> float:
        assert False

    def human_readable(self) -> str:
        return str(self)


class SuccessComparator(Comparator[float]):
    def is_regression(self) -> bool:
        return False


class RelativeThresholdComparator(Comparator[float]):
    def ratio(self) -> float:
        return self._points[0] / self._points[1]

    def is_regression(self) -> bool:
        ratio = self.ratio()
        if ratio > 1:
            return ratio - 1 > self.threshold
        else:
            return False

    def human_readable(self) -> str:
        ratio = self.ratio()
        if ratio >= 2:
            return f"{ratio:3.1f} TIMES more/slower"
        elif ratio > 1:
            return f"{-(1-ratio)*100:3.1f} pct   more/slower"
        elif ratio == 1:
            return "          same"
        elif ratio > 0.5:
            return f"{(1-ratio)*100:3.1f} pct   less/faster"
        else:
            return f"{(1/ratio):3.1f} times less/faster"


class Overlappable(Protocol):
    def overlap(self, other: "Overlappable") -> float:
        ...


class OverlapComparator(Comparator[Overlappable]):
    def ratio(self) -> float:
        return self._points[0].overlap(other=self._points[1])

    def is_regression(self) -> bool:
        return self.ratio() < 1 - self.threshold
