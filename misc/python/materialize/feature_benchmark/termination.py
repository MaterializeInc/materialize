# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import statistics
from typing import List, Optional

import numpy as np
from scipy import stats  # type: ignore


class TerminationCondition:
    pass

    def terminate(self, measurement: float) -> bool:
        assert False


class NormalDistributionOverlap(TerminationCondition):
    """Signal termination if the overlap between the two distributions is above the threshold"""

    def __init__(self, threshold: float) -> None:
        self._threshold = threshold
        self._data: List[float] = []
        self._last_fit: Optional[statistics.NormalDist] = None

    def terminate(self, measurement: float) -> bool:
        self._data.append(measurement)

        if len(self._data) > 10:
            (mu, sigma) = stats.norm.fit(self._data)
            current_fit = statistics.NormalDist(mu=mu, sigma=sigma)

            if self._last_fit:
                current_overlap = current_fit.overlap(other=self._last_fit)
                if current_overlap >= self._threshold:
                    return True

            self._last_fit = current_fit

        return False


class ProbForMin(TerminationCondition):
    """Signal termination if the probability that a new value will arrive that is smaller than all the previous values
    has dropped below the threshold
    """

    def __init__(self, threshold: float) -> None:
        self._threshold = threshold
        self._data: List[float] = []

    def terminate(self, measurement: float) -> bool:
        self._data.append(measurement)

        if len(self._data) > 5:
            mean = np.mean(self._data)
            stdev = np.std(self._data)
            min_val = np.min(self._data)  # type: ignore
            dist = stats.norm(loc=mean, scale=stdev)
            prob = dist.cdf(min_val)
            if prob < (1 - self._threshold):
                return True
            else:
                return False
        else:
            return False
