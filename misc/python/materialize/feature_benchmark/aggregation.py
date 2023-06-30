# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import statistics
from typing import Any, Callable, List

import numpy as np

from materialize.feature_benchmark.measurement import Measurement


class Aggregation:
    def __init__(self) -> None:
        self._data: List[float] = []

    def append(self, measurement: Measurement) -> None:
        self._data.append(measurement.value)

    def aggregate(self) -> Any:
        return self.func()([*self._data])

    def func(self) -> Callable:
        assert False


class MinAggregation(Aggregation):
    def func(self) -> Callable:
        return min


class MeanAggregation(Aggregation):
    def func(self) -> Callable:
        return np.mean


class StdDevAggregation(Aggregation):
    def __init__(self, num_stdevs: float) -> None:
        self._data = []
        self._num_stdevs = num_stdevs

    def aggregate(self) -> float:
        stdev: float = np.std(self._data, dtype=float)
        mean: float = np.mean(self._data, dtype=float)
        val = mean - (stdev * self._num_stdevs)
        return val


class NormalDistributionAggregation(Aggregation):
    def aggregate(self) -> statistics.NormalDist:
        return statistics.NormalDist(
            mu=np.mean(self._data, dtype=float), sigma=np.std(self._data, dtype=float)
        )


class NoAggregation(Aggregation):
    def aggregate(self) -> Any:
        return self._data[0]
