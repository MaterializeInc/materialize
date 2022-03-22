# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from math import ceil
from typing import Dict, List, Optional, Type, Union

from materialize.feature_benchmark.action import Action, DummyAction, TdAction
from materialize.feature_benchmark.measurement_source import MeasurementSource

BenchmarkingSequence = Union[MeasurementSource, List[Union[Action, MeasurementSource]]]


class RootScenario:
    SCALE: float = 6

    def __init__(self, scale: float) -> None:
        self._name = self.__class__.__name__
        self._scale = scale
        self._n: int = int(10**scale)

    def shared(self) -> Optional[Union[Action, List[Action]]]:
        return None

    def init(self) -> Optional[Union[Action, List[Action]]]:
        return None

    def before(self) -> Action:
        return DummyAction()

    def benchmark(self) -> BenchmarkingSequence:
        assert False

    def name(self) -> str:
        return self._name

    def scale(self) -> float:
        return self._scale

    def n(self) -> int:
        return self._n

    @staticmethod
    def name_with_scale(cls: Type["Scenario"], num: int, params_dict: Dict) -> str:
        """Return the name of the Senario including the scale.
        Used for running multiple instances of the same scenario via the
        parameterized python module.
        """
        return f"{cls.__name__}_scale_{params_dict['SCALE']}"

    def table_ten(self) -> TdAction:
        """Returns a Td() object that creates the 'ten' table"""
        return TdAction(
            """
> CREATE TABLE ten (f1 INTEGER);
> INSERT INTO ten VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);
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
