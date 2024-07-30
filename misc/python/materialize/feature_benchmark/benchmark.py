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
from typing import Any

from materialize import ui
from materialize.feature_benchmark.aggregation import Aggregation
from materialize.feature_benchmark.comparator import Comparator
from materialize.feature_benchmark.executor import Executor
from materialize.feature_benchmark.filter import Filter
from materialize.feature_benchmark.measurement import (
    Measurement,
    MeasurementType,
    WallclockDuration,
)
from materialize.feature_benchmark.measurement_source import MeasurementSource
from materialize.feature_benchmark.scenario import Scenario
from materialize.feature_benchmark.scenario_version import ScenarioVersion
from materialize.feature_benchmark.termination import TerminationCondition
from materialize.mz_version import MzVersion


class Benchmark:
    def __init__(
        self,
        mz_id: int,
        mz_version: MzVersion,
        scenario_cls: type[Scenario],
        executor: Executor,
        filter: Filter,
        termination_conditions: list[TerminationCondition],
        aggregation_class: type[Aggregation],
        default_size: int,
        seed: int,
        scale: str | None = None,
        measure_memory: bool = True,
    ) -> None:
        self._scale = scale
        self._mz_id = mz_id
        self._mz_version = mz_version
        self._scenario_cls = scenario_cls
        self._executor = executor
        self._filter = filter
        self._termination_conditions = termination_conditions
        self._performance_aggregation = aggregation_class()
        self._messages_aggregation = aggregation_class()
        self._default_size = default_size
        self._seed = seed

        if measure_memory:
            self._memory_mz_aggregation = aggregation_class()
            self._memory_clusterd_aggregation = aggregation_class()

    def create_scenario_instance(self) -> Scenario:
        scale = self._scenario_cls.SCALE

        if self._scale and not self._scenario_cls.FIXED_SCALE:
            if self._scale.startswith("+"):
                scale = scale + float(self._scale.lstrip("+"))
            elif self._scale.startswith("-"):
                scale = scale - float(self._scale.lstrip("-"))
            elif float(self._scale) > 0:
                scale = float(self._scale)

        scenario_class = self._scenario_cls
        return scenario_class(
            scale=scale,
            mz_version=self._mz_version,
            default_size=self._default_size,
            seed=self._seed,
        )

    def run(self) -> list[Aggregation]:
        scenario = self.create_scenario_instance()
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
            timestamps: list[WallclockDuration] = []
            benchmark = scenario.benchmark()
            for benchmark_item in (
                benchmark if isinstance(benchmark, list) else [benchmark]
            ):
                assert isinstance(
                    benchmark_item, MeasurementSource
                ), f"Benchmark item is of type {benchmark_item.__class__} but not a MeasurementSource"
                item_timestamps = benchmark_item.run(executor=self._executor)
                timestamps.extend(item_timestamps)

            assert (
                len(timestamps) == 2
            ), f"benchmark() did not return exactly 2 timestamps: scenario: {scenario}, timestamps: {timestamps}"
            assert (
                timestamps[0].unit == timestamps[1].unit
            ), f"benchmark() returned timestamps with different units: scenario: {scenario}, timestamps: {timestamps}"
            assert timestamps[1].is_equal_or_after(
                timestamps[0]
            ), f"Second timestamp reported not greater than first: scenario: {scenario}, timestamps: {timestamps}"

            performance_measurement = Measurement(
                type=MeasurementType.WALLCLOCK,
                value=timestamps[1].duration - timestamps[0].duration,
                notes=f"Unit: {timestamps[0].unit}",
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

            if self._memory_mz_aggregation:
                memory_mz_measurement = Measurement(
                    type=MeasurementType.MEMORY_MZ,
                    value=self._executor.DockerMemMz() / 2**20,  # Convert to Mb
                )

                if memory_mz_measurement.value > 0:
                    if not self._filter or not self._filter.filter(
                        memory_mz_measurement
                    ):
                        print(f"{i} {memory_mz_measurement}")
                        self._memory_mz_aggregation.append(memory_mz_measurement)

            if self._memory_clusterd_aggregation:
                memory_clusterd_measurement = Measurement(
                    type=MeasurementType.MEMORY_CLUSTERD,
                    value=self._executor.DockerMemClusterd() / 2**20,  # Convert to Mb
                )

                if memory_clusterd_measurement.value > 0:
                    if not self._filter or not self._filter.filter(
                        memory_clusterd_measurement
                    ):
                        print(f"{i} {memory_clusterd_measurement}")
                        self._memory_clusterd_aggregation.append(
                            memory_clusterd_measurement
                        )

            for termination_condition in self._termination_conditions:
                if termination_condition.terminate(performance_measurement):
                    return [
                        self._performance_aggregation,
                        self._messages_aggregation,
                        self._memory_mz_aggregation,
                        self._memory_clusterd_aggregation,
                    ]

        assert False, "unreachable"


class Report:
    def __init__(self, cycle_number: int) -> None:
        self.cycle_number = cycle_number
        """ 1-based cycle number. """
        self._comparisons: list[Comparator] = []

    def append(self, comparison: Comparator) -> None:
        self._comparisons.append(comparison)

    def extend(self, comparisons: Iterable[Comparator]) -> None:
        self._comparisons.extend(comparisons)

    def as_string(self, use_colors: bool, limit_to_scenario: str | None = None) -> str:
        output_lines = []

        output_lines.append(
            f"{'NAME':<35} | {'TYPE':<15} | {'THIS':^15} | {'OTHER':^15} | {'THRESHOLD':^10} | {'Regression?':^13} | 'THIS' is:"
        )
        output_lines.append("-" * 100)

        for comparison in self._comparisons:
            if limit_to_scenario is not None and comparison.name != limit_to_scenario:
                continue

            regression = "!!YES!!" if comparison.is_regression() else "no"
            output_lines.append(
                f"{comparison.name:<35} | {comparison.type:<15} | {comparison.this_as_str():>15} | {comparison.other_as_str():>15} | {comparison.threshold * 100:^10.0f}% | {regression:^13} | {comparison.human_readable(use_colors)}"
            )

        return "\n".join(output_lines)

    def __str__(self) -> str:
        return self.as_string(use_colors=False)

    def measurements_of_this(self, scenario_name: str) -> dict[MeasurementType, Any]:
        result = dict()

        for comparison in self._comparisons:
            if comparison.name == scenario_name:
                result[comparison.type] = comparison.this()

        return result

    def get_scenario_version(self, scenario_name: str) -> ScenarioVersion:
        for comparison in self._comparisons:
            if comparison.name == scenario_name:
                return comparison.get_scenario_version()

        assert False, f"Scenario {scenario_name} not found!"
