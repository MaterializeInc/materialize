# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import List

import numpy as np


class Filter:
    def __init__(self) -> None:
        self._data: List[float] = []


class RemoveOutliers(Filter):
    def filter(self, measurement: float) -> bool:
        self._data.append(measurement)

        if len(self._data) > 3:
            mean = np.mean(self._data)
            stdev = np.std(self._data)
            if measurement > mean + (1 * stdev):
                return True
            else:
                return False
        else:
            return False


class NoFilter(Filter):
    def filter(self, measurement: float) -> bool:
        return False


class FilterFirst(Filter):
    def filter(self, measurement: float) -> bool:
        self._data.append(measurement)

        if len(self._data) == 1:
            print("Discarding first measurement.")
            return True
        else:
            return False
