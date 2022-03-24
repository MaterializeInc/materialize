# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sys
from typing import List, Optional, Tuple, Type

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
        scenario: Type[Scenario],
        executor: Executor,
        filter: Filter,
        termination_conditions: List[TerminationCondition],
        aggregation: Aggregation,
        scale: Optional[str] = None,
    ) -> None:
        self._scale = scale
        self._mz_id = mz_id
        self._scenario = scenario
        self._executor = executor
        self._filter = filter
        self._termination_conditions = termination_conditions
        self._aggregation = aggregation

    def run(self) -> Tuple[float, int]:
        scale = self._scenario.SCALE

        if self._scale:
            if self._scale.startswith("+"):
                scale = scale + float(self._scale.lstrip("+"))
            elif self._scale.startswith("-"):
                scale = scale - float(self._scale.lstrip("-"))
            elif float(self._scale) > 0:
                scale = float(self._scale)

        scenario_class = self._scenario
        scenario = scenario_class(scale=scale)
        name = scenario.name()

        print(
            f"Sizing in effect for scenario {name}: scale = {scenario.scale()} , N = {scenario.n()}"
        )

        # Run the shared() section once for both Mzs under measurement
        shared = scenario.shared()
        if self._mz_id == 0 and shared is not None:
            print(f"Running the shared() section for {name} ...")

            for shared_item in shared if isinstance(shared, list) else [shared]:
                shared_item.run(executor=self._executor)

            print("shared() done")

        # Run the init() section once for each Mz
        init = scenario.init()
        if init is not None:
            print(f"Running the init() section for {name} ...")

            for init_item in init if isinstance(init, list) else [init]:
                init_item.run(executor=self._executor)

            print("init() done")

        for i in range(sys.maxsize):
            # Run the before() section once for each measurement
            before = scenario.before()
            for before_item in before if isinstance(before, list) else [before]:
                before_item.run(executor=self._executor)

            # Collect timestamps from any part of the workload being benchmarked
            timestamps = []
            benchmark = scenario.benchmark()
            for benchmark_item in (
                benchmark if isinstance(benchmark, list) else [benchmark]
            ):
                item_timestamps = benchmark_item.run(executor=self._executor)
                if item_timestamps:
                    timestamps.extend(
                        item_timestamps
                        if isinstance(item_timestamps, list)
                        else [item_timestamps]
                    )

            assert (
                len(timestamps) == 2
            ), f"benchmark() did not return exactly 2 timestamps: scenario: {scenario}, timestamps: {timestamps}"
            assert (
                timestamps[1] >= timestamps[0]
            ), f"Second timestamp reported not greater than first: scenario: {scenario}, timestamps: {timestamps}"

            measurement = timestamps[1] - timestamps[0]
            if self._filter and getattr(self._filter, "filter")(measurement):
                continue

            print(f"Measurement {i}: {measurement}")

            self._aggregation.append(measurement)

            for termination_condition in self._termination_conditions:
                if termination_condition.terminate(measurement):
                    return self._aggregation.aggregate(), i + 1

        assert False, "unreachable"


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
