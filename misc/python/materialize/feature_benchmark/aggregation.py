# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import statistics
from collections.abc import Callable
from typing import Any

import numpy as np

from materialize.feature_benchmark.measurement import (
    Measurement,
    MeasurementType,
    MeasurementUnit,
)


class Aggregation:
    def __init__(self) -> None:
        self.measurement_type: MeasurementType | None = None
        self._data: list[float] = []
        self._unit: MeasurementUnit = MeasurementUnit.UNKNOWN

    def append_measurement(self, measurement: Measurement) -> None:
        assert measurement.unit != MeasurementUnit.UNKNOWN, "Unknown unit"
        self.measurement_type = measurement.type
        self._unit = measurement.unit
        self._data.append(measurement.value)

    def aggregate(self) -> Any:
        if len(self._data) == 0:
            return None

        return self.func()([*self._data])

    def unit(self) -> MeasurementUnit:
        return self._unit

    def func(self) -> Callable:
        raise NotImplementedError

    def name(self) -> str:
        return self.__class__.__name__


class MinAggregation(Aggregation):
    def func(self) -> Callable:
        return min


class MeanAggregation(Aggregation):
    def func(self) -> Callable:
        return np.mean


class StdDevAggregation(Aggregation):
    def __init__(self, num_stdevs: float) -> None:
        super().__init__()
        self._num_stdevs = num_stdevs

    def aggregate(self) -> float | None:
        if len(self._data) == 0:
            return None

        stdev: float = np.std(self._data, dtype=float)
        mean: float = np.mean(self._data, dtype=float)
        val = mean - (stdev * self._num_stdevs)
        return val


class NormalDistributionAggregation(Aggregation):
    def aggregate(self) -> statistics.NormalDist | None:
        if len(self._data) == 0:
            return None

        return statistics.NormalDist(
            mu=np.mean(self._data, dtype=float), sigma=np.std(self._data, dtype=float)
        )


class NoAggregation(Aggregation):
    def aggregate(self) -> Any:
        if len(self._data) == 0:
            return None

        return self._data[0]
