# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import sys
from collections.abc import Iterable

from materialize import ui
from materialize.feature_benchmark.aggregation import Aggregation
from materialize.feature_benchmark.comparator import Comparator
from materialize.feature_benchmark.executor import Executor
from materialize.feature_benchmark.filter import Filter
from materialize.feature_benchmark.measurement import Measurement, MeasurementType
from materialize.feature_benchmark.scenario import Scenario
from materialize.feature_benchmark.termination import TerminationCondition
from materialize.mz_version import MzVersion


class Benchmark:
    def __init__(
        self,
        mz_id: int,
        mz_version: MzVersion,
        scenario: type[Scenario],
        executor: Executor,
        filter: Filter,
        termination_conditions: list[TerminationCondition],
        aggregation_class: type[Aggregation],
        default_size: int,
        scale: str | None = None,
        measure_memory: bool = True,
    ) -> None:
        self._scale = scale
        self._mz_id = mz_id
        self._mz_version = mz_version
        self._scenario = scenario
        self._executor = executor
        self._filter = filter
        self._termination_conditions = termination_conditions
        self._performance_aggregation = aggregation_class()
        self._messages_aggregation = aggregation_class()
        self._default_size = default_size

        if measure_memory:
            self._memory_aggregation = aggregation_class()

    def run(self) -> list[Aggregation]:
        scale = self._scenario.SCALE

        if self._scale and not self._scenario.FIXED_SCALE:
            if self._scale.startswith("+"):
                scale = scale + float(self._scale.lstrip("+"))
            elif self._scale.startswith("-"):
                scale = scale - float(self._scale.lstrip("-"))
            elif float(self._scale) > 0:
                scale = float(self._scale)

        scenario_class = self._scenario
        scenario = scenario_class(
            scale=scale, mz_version=self._mz_version, default_size=self._default_size
        )
        name = scenario.name()

        ui.header(
            f"Running scenario {name}, scale = {scenario.scale()}, N = {scenario.n()}"
        )

        # Run the shared() section once for both Mzs under measurement
        shared = scenario.shared()
        if self._mz_id == 0 and shared is not None:
            print(
                f"Running the shared() section for scenario {name} with {self._mz_version} ..."
            )

            for shared_item in shared if isinstance(shared, list) else [shared]:
                shared_item.run(executor=self._executor)

            print("shared() done")

        # Run the init() section once for each Mz
        init = scenario.init()
        if init is not None:
            print(
                f"Running the init() section for scenario {name} with {self._mz_version} ..."
            )

            for init_item in init if isinstance(init, list) else [init]:
                init_item.run(executor=self._executor)

            print("init() done")

        for i in range(sys.maxsize):
            # Run the before() section once for each measurement
            print(
                f"Running the before() section for scenario {name} with {self._mz_version} ..."
            )
            before = scenario.before()
            if before is not None:
                for before_item in before if isinstance(before, list) else [before]:
                    before_item.run(executor=self._executor)

            print(
                f"Running the benchmark for scenario {name} with {self._mz_version} ..."
            )
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

            performance_measurement = Measurement(
                type=MeasurementType.WALLCLOCK,
                value=timestamps[1] - timestamps[0],
            )

            if not self._filter or not self._filter.filter(performance_measurement):
                print(f"{i} {performance_measurement}")
                self._performance_aggregation.append(performance_measurement)

            messages = self._executor.Messages()
            if messages is not None:
                messages_measurement = Measurement(
                    type=MeasurementType.MESSAGES, value=messages
                )
                print(f"{i}: {messages_measurement}")
                self._messages_aggregation.append(messages_measurement)

            if self._memory_aggregation:
                memory_measurement = Measurement(
                    type=MeasurementType.MEMORY,
                    value=self._executor.DockerMem() / 2**20,  # Convert to Mb
                )

                if memory_measurement.value > 0:
                    if not self._filter or not self._filter.filter(memory_measurement):
                        print(f"{i} {memory_measurement}")
                        self._memory_aggregation.append(memory_measurement)

            for termination_condition in self._termination_conditions:
                if termination_condition.terminate(performance_measurement):
                    return [
                        self._performance_aggregation,
                        self._messages_aggregation,
                        self._memory_aggregation,
                    ]

        assert False, "unreachable"


class Report:
    def __init__(self) -> None:
        self._comparisons: list[Comparator] = []

    def append(self, comparison: Comparator) -> None:
        self._comparisons.append(comparison)

    def extend(self, comparisons: Iterable[Comparator]) -> None:
        self._comparisons.extend(comparisons)

    def dump(self) -> None:
        print(
            f"{'NAME':<35} | {'TYPE':<9} | {'THIS':^15} | {'OTHER':^15} | {'Regression?':^13} | 'THIS' is:"
        )
        print("-" * 100)

        for comparison in self._comparisons:
            regression = "!!YES!!" if comparison.is_regression() else "no"
            print(
                f"{comparison.name:<35} | {comparison.type:<9} | {comparison.this_as_str():>15} | {comparison.other_as_str():>15} | {regression:^13} | {comparison.human_readable()}"
            )


class SingleReport(Report):
    def dump(self) -> None:
        print(f"{'NAME':<25} | {'THIS':^11}")
        print("-" * 50)

        for comparison in self._comparisons:
            print(f"{comparison.name:<25} | {comparison.this():>11.3f}")
