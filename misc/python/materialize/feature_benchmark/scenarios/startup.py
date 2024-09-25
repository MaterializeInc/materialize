# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


from pathlib import Path

import materialize.optbench
import materialize.optbench.sql
from materialize.feature_benchmark.action import Action
from materialize.feature_benchmark.executor import Executor
from materialize.feature_benchmark.measurement_source import (
    Lambda,
    MeasurementSource,
)
from materialize.feature_benchmark.scenario import Scenario
from materialize.feature_benchmark.scenarios.optbench import OptbenchInit


class StartupInit(Action):
    def __init__(self, n: int) -> None:
        self._executor: Executor | None = None
        self._n = n

    def run(self, executor: Executor | None = None) -> None:
        e = executor or self._executor
        queries = materialize.optbench.sql.parse_from_file(
            Path("misc/python/materialize/optbench/workload/tpch.sql")
        )
        statements = []
        for i in range(self._n):
            for q, query in enumerate(queries):
                statements.extend(
                    [
                        f"CREATE VIEW vq_{q}_{i} AS {query}",
                        f"CREATE DEFAULT INDEX ON vq_{q}_{i}",
                        f"CREATE MATERIALIZED VIEW mvq_{q}_{i} AS {query}",
                    ]
                )
        e._composition.sql("\n".join(statements))  # type: ignore


class Startup(Scenario):
    """Startup benchmark"""

    def before(self) -> list[Action]:
        return [OptbenchInit("tpch", no_indexes=True), StartupInit(self.n())]

    def benchmark(self) -> MeasurementSource:
        return Lambda(lambda e: e.RestartMzMaterialized())
