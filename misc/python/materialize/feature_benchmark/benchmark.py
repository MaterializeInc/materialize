# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sys
from typing import List, Tuple

from materialize.feature_benchmark.aggregation import Aggregation
from materialize.feature_benchmark.comparator import Comparator
from materialize.feature_benchmark.executor import Executor
from materialize.feature_benchmark.filter import Filter
from materialize.feature_benchmark.scenario import Scenario
from materialize.feature_benchmark.termination import TerminationCondition


class Benchmark:
    def __init__(
        self,
        mz_id: int,
        scenario: Scenario,
        executor: Executor,
        filter: Filter,
        termination_conditions: List[TerminationCondition],
        aggregation: Aggregation,
    ) -> None:
        self._mz_id = mz_id
        self._scenario = scenario
        self._executor = executor
        self._filter = filter
        self._termination_conditions = termination_conditions
        self._aggregation = aggregation

    def run(self) -> Tuple[float, int]:
        name = self._scenario.__name__  # type: ignore

        # Run the SHARED section once for both Mzs under measurement
        shared = self._scenario.SHARED
        if self._mz_id == 0 and shared is not None:
            print(f"Running the SHARED section for {name} ...")
            shared.run(executor=self._executor, measure=False)

        # Run the SHARED section once for each Mz
        init = self._scenario.INIT
        if init is not None:
            print(f"Running the INIT section for {name} ...")
            init.run(executor=self._executor, measure=False)

        before = self._scenario.BEFORE(executor=self._executor)
        benchmark = self._scenario.BENCHMARK(executor=self._executor)

        # Use zip() to run both the BEFORE and the BENCHMARK sections
        for i, _before_result, measurement in zip(
            range(sys.maxsize), before, benchmark
        ):
            if self._filter and getattr(self._filter, "filter")(measurement):
                continue

            print(f"Measurement: {measurement}")

            self._aggregation.append(measurement)

            for termination_condition in self._termination_conditions:
                if termination_condition.terminate(measurement):
                    return self._aggregation.aggregate(), i + 1
        assert False, "Source exhausted before termination condition reached"


class Report:
    def __init__(self) -> None:
        self._comparisons: List[Comparator] = []

    def append(self, comparison: Comparator) -> None:
        self._comparisons.append(comparison)

    def dump(self) -> None:
        print(
            f"{'NAME':<25} | {'THIS':^11} | {'OTHER':^11} | {'Regression?':^13} | 'THIS' is:"
        )
        print("-" * 100)

        for comparison in self._comparisons:
            regression = "!!YES!!" if comparison.is_regression() else "no"
            print(
                f"{comparison.name():<25} | {comparison.this():>11.3f} | {comparison.other():>11.3f} | {regression:^13} | {comparison.human_readable()}"
            )
