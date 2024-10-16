# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

import time

from materialize.feature_benchmark.aggregation import Aggregation
from materialize.feature_benchmark.executor import Executor
from materialize.feature_benchmark.filter import Filter
from materialize.feature_benchmark.measurement import (
    Measurement,
    MeasurementType,
    MeasurementUnit,
    WallclockDuration,
)
from materialize.feature_benchmark.measurement_source import MeasurementSource
from materialize.feature_benchmark.scenario import Scenario
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

        print(
            f"--- Running scenario {scenario.name()}, scale = {scenario.scale()}, N = {scenario.n()}"
        )
        start_time = time.time()

        # Run the shared() section once for both Mzs under measurement
        self.run_shared(scenario)

        # Run the init() section once for each Mz
        self.run_init(scenario)

        i = 0
        while True:
            # Run the before() section once for each measurement
            self.run_before(scenario)

            performance_measurement = self.run_measurement(scenario, i)

            if self.shall_terminate(performance_measurement):
                duration = time.time() - start_time
                print(
                    f"Scenario {scenario.name()}, scale = {scenario.scale()}, N = {scenario.n()} took {duration:.0f}s to run"
                )

                return [
                    self._performance_aggregation,
                    self._memory_mz_aggregation,
                    self._memory_clusterd_aggregation,
                ]

            i = i + 1

    def run_shared(self, scenario: Scenario) -> None:
        shared = scenario.shared()
        if self._mz_id == 0 and shared is not None:
            print(
                f"Running the shared() section for scenario {scenario.name()} with {self._mz_version} ..."
            )

            for shared_item in shared if isinstance(shared, list) else [shared]:
                shared_item.run(executor=self._executor)

            print("shared() done")

    def run_init(self, scenario: Scenario) -> None:
        init = scenario.init()
        if init is not None:
            print(
                f"Running the init() section for scenario {scenario.name()} with {self._mz_version} ..."
            )

            for init_item in init if isinstance(init, list) else [init]:
                init_item.run(executor=self._executor)

            print("init() done")

    def run_before(self, scenario: Scenario) -> None:
        print(
            f"Running the before() section for scenario {scenario.name()} with {self._mz_version} ..."
        )
        before = scenario.before()
        if before is not None:
            for before_item in before if isinstance(before, list) else [before]:
                before_item.run(executor=self._executor)

    def shall_terminate(self, performance_measurement: Measurement) -> bool:
        for termination_condition in self._termination_conditions:
            if termination_condition.terminate(performance_measurement):
                return True

        return False

    def run_measurement(self, scenario: Scenario, i: int) -> Measurement:
        print(
            f"Running the benchmark for scenario {scenario.name()} with {self._mz_version} ..."
        )
        # Collect timestamps from any part of the workload being benchmarked
        timestamps: list[WallclockDuration] = []
        benchmark = scenario.benchmark()
        for benchmark_item in benchmark if isinstance(benchmark, list) else [benchmark]:
            assert isinstance(
                benchmark_item, MeasurementSource
            ), f"Benchmark item is of type {benchmark_item.__class__} but not a MeasurementSource"
            item_timestamps = benchmark_item.run(executor=self._executor)
            timestamps.extend(item_timestamps)

        self._validate_measurement_timestamps(scenario.name(), timestamps)

        performance_measurement = Measurement(
            type=MeasurementType.WALLCLOCK,
            value=timestamps[1].duration - timestamps[0].duration,
            unit=timestamps[0].unit,
            notes=f"Unit: {timestamps[0].unit}",
        )

        self._collect_performance_measurement(i, performance_measurement)

        if self._memory_mz_aggregation:
            self._collect_memory_measurement(
                i, MeasurementType.MEMORY_MZ, self._memory_mz_aggregation
            )

        if self._memory_clusterd_aggregation:
            self._collect_memory_measurement(
                i, MeasurementType.MEMORY_CLUSTERD, self._memory_clusterd_aggregation
            )

        return performance_measurement

    def _validate_measurement_timestamps(
        self, scenario_name: str, timestamps: list[WallclockDuration]
    ) -> None:
        assert (
            len(timestamps) == 2
        ), f"benchmark() did not return exactly 2 timestamps: scenario: {scenario_name}, timestamps: {timestamps}"
        assert (
            timestamps[0].unit == timestamps[1].unit
        ), f"benchmark() returned timestamps with different units: scenario: {scenario_name}, timestamps: {timestamps}"
        assert timestamps[1].is_equal_or_after(
            timestamps[0]
        ), f"Second timestamp reported not greater than first: scenario: {scenario_name}, timestamps: {timestamps}"

    def _collect_performance_measurement(
        self, i: int, performance_measurement: Measurement
    ) -> None:
        if not self._filter or not self._filter.filter(performance_measurement):
            print(f"{i} {performance_measurement}")
            self._performance_aggregation.append_measurement(performance_measurement)

    def _collect_memory_measurement(
        self, i: int, memory_measurement_type: MeasurementType, aggregation: Aggregation
    ) -> None:
        if memory_measurement_type == MeasurementType.MEMORY_MZ:
            value = self._executor.DockerMemMz()
        elif memory_measurement_type == MeasurementType.MEMORY_CLUSTERD:
            value = self._executor.DockerMemClusterd()
        else:
            raise ValueError(f"Unknown measurement type {memory_measurement_type}")
        memory_measurement = Measurement(
            type=memory_measurement_type,
            value=value / 2**20,  # Convert to Mb
            unit=MeasurementUnit.MEGABYTE,
        )

        if memory_measurement.value > 0:
            if not self._filter or not self._filter.filter(memory_measurement):
                print(f"{i} {memory_measurement}")
                aggregation.append_measurement(memory_measurement)
