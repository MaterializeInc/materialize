# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from math import ceil

from materialize.feature_benchmark.action import Action, DummyAction, TdAction
from materialize.feature_benchmark.measurement import MeasurementType
from materialize.feature_benchmark.measurement_source import MeasurementSource
from materialize.feature_benchmark.scenario_version import ScenarioVersion
from materialize.mz_version import MzVersion

BenchmarkingSequence = MeasurementSource | list[Action | MeasurementSource]


class RootScenario:
    SCALE: float = 6
    FIXED_SCALE: bool = False  # Will --scale=N have effect on the scenario
    RELATIVE_THRESHOLD: dict[MeasurementType, float] = {
        MeasurementType.WALLCLOCK: 0.10,
        # Increased the other measurements since they are easy to regress now
        # that we take the run with the minimum wallclock time:
        MeasurementType.MEMORY_MZ: 0.20,
        MeasurementType.MEMORY_CLUSTERD: 0.50,
    }

    def __init__(
        self, scale: float, mz_version: MzVersion, default_size: int, seed: int
    ) -> None:
        self._name = self.__class__.__name__
        self._scale = scale
        self._mz_version = mz_version
        self._n: int = int(10**scale)
        self._default_size = default_size
        self._seed = seed

    @classmethod
    def can_run(cls, version: MzVersion) -> bool:
        return True

    def shared(self) -> Action | list[Action] | None:
        return None

    def init(self) -> Action | list[Action] | None:
        return None

    def before(self) -> Action | list[Action] | None:
        return DummyAction()

    def benchmark(self) -> BenchmarkingSequence:
        raise NotImplementedError

    def name(self) -> str:
        return self._name

    def version(self) -> ScenarioVersion:
        return ScenarioVersion.create(1, 0, 0)

    def scale(self) -> float:
        return self._scale

    def n(self) -> int:
        return self._n

    def seed(self) -> int:
        return self._seed

    @staticmethod
    def name_with_scale(class_: type["Scenario"], num: int, params_dict: dict) -> str:
        """Return the name of the Scenario including the scale.
        Used for running multiple instances of the same scenario via the
        parameterized python module.
        """
        return f"{class_.__name__}_scale_{params_dict['SCALE']}"

    def table_ten(self) -> TdAction:
        """Returns a Td() object that creates the 'ten' table"""
        return TdAction(
            """
> CREATE TABLE ten (f1 INTEGER);
> INSERT INTO ten VALUES (0)
> INSERT INTO ten VALUES (1)
> INSERT INTO ten VALUES (2)
> INSERT INTO ten VALUES (3)
> INSERT INTO ten VALUES (4)
> INSERT INTO ten VALUES (5)
> INSERT INTO ten VALUES (6)
> INSERT INTO ten VALUES (7)
> INSERT INTO ten VALUES (8)
> INSERT INTO ten VALUES (9)
"""
        )

    def view_ten(self) -> TdAction:
        return TdAction(
            """
> CREATE VIEW ten (f1) AS (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9));
"""
        )

    def unique_values(self) -> str:
        """Returns a string of the form 'a1.f1 + (a2.f1 * 10) + (a2.f1 * 100) ...'"""
        return " + ".join(
            f"(a{i+1}.f1 * {10**i})" for i in range(0, ceil(self.scale()))
        )

    def join(self) -> str:
        """Returns a string of the form 'ten AS a1 , ten AS a2 , ten AS a3 ...'"""
        return ", ".join(f"ten AS a{i+1}" for i in range(0, ceil(self.scale())))

    def keyschema(self) -> str:
        return (
            "\n"
            + '$ set keyschema={"type": "record", "name": "Key", "fields": [ { "name": "f1", "type": "long" } ] }'
            + "\n"
        )

    def schema(self) -> str:
        return (
            "\n"
            + '$ set schema={"type" : "record", "name" : "test", "fields": [ { "name": "f2", "type": "long" } ] }'
            + "\n"
        )


# Used for benchmarks that are expected to run by default, e.g. in CI
class Scenario(RootScenario):
    pass


# Used for scenarios that need to be explicitly run from the command line using --root-scenario ScenarioBig
class ScenarioBig(RootScenario):
    pass


# Used for disabled scenarios
class ScenarioDisabled(RootScenario):
    pass
