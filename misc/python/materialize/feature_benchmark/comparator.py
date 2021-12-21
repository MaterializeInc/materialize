# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import Any, List


class Comparator:
    def __init__(self, name: str, threshold: float) -> None:
        self._name = name
        self._threshold = threshold
        self._points: List[Any] = []

    def append(self, point: Any) -> None:
        self._points.append(point)

    def name(self) -> str:
        return self._name

    def this(self) -> Any:
        return self._points[0]

    def other(self) -> Any:
        return self._points[1]

    def is_regression(self) -> bool:
        assert False

    def ratio(self) -> float:
        assert False

    def human_readable(self) -> str:
        return str(self)


class RelativeThresholdComparator(Comparator):
    def ratio(self) -> float:
        assert type(self._points[0]) is float
        assert type(self._points[1]) is float
        return self._points[0] / self._points[1]

    def is_regression(self) -> bool:
        ratio = self.ratio()
        if ratio > 1:
            return ratio - 1 > self._threshold
        else:
            return False

    def human_readable(self) -> str:
        ratio = self.ratio()
        if ratio >= 2:
            return f"{ratio:3.1f} TIMES slower"
        elif ratio > 1:
            return f"{-(1-ratio)*100:3.1f} pct   slower"
        elif ratio == 1:
            return "          same"
        elif ratio > 0.5:
            return f"{(1-ratio)*100:3.1f} pct   faster"
        else:
            return f"{(1/ratio):3.1f} times faster"


class OverlapComparator(Comparator):
    def ratio(self) -> float:
        return self._points[0].overlap(other=self._points[1])  # type: ignore

    def is_regression(self) -> bool:
        return self.ratio() < 1 - self._threshold
