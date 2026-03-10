# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.feature_benchmark.action import Action, TdAction
from materialize.feature_benchmark.measurement_source import MeasurementSource, Td
from materialize.feature_benchmark.scenario import Scenario
from materialize.mz_version import MzVersion


class StandingQuery(Scenario):
    """Feature benchmarks related to standing queries."""

    @classmethod
    def can_run(cls, version: MzVersion) -> bool:
        return version > MzVersion.create(26, 15, 0)


class StandingQueryExecute(StandingQuery):
    """Measure the latency of EXECUTE STANDING QUERY with a single parameter
    against a table with many rows where only a few match."""

    FIXED_SCALE = True
    SCALE = 5

    def init(self) -> list[Action]:
        return [
            TdAction(
                f"""\
> DROP TABLE IF EXISTS t1 CASCADE
> CREATE TABLE t1 (id INT, category INT, val INT)
> INSERT INTO t1 SELECT g, g % 100, g * 10 FROM generate_series(1, {self.n()}) AS g
> CREATE STANDING QUERY sq1 (cat INT) AS SELECT id, category, val FROM t1 WHERE category = cat
"""
            ),
        ]

    def benchmark(self) -> MeasurementSource:
        # Execute the standing query repeatedly, measuring time for each batch.
        repeated_executes = "\n".join(
            f"> EXECUTE STANDING QUERY sq1 ({i % 100})" for i in range(10)
        )

        return Td(
            f"""\
> SELECT 1
  /* A */
1

{repeated_executes}

> SELECT 1
  /* B */
1
"""
        )


class StandingQueryExecuteVsSelect(StandingQuery):
    """Measure the latency of EXECUTE STANDING QUERY compared to an equivalent
    SELECT with a literal filter, to verify standing queries are faster."""

    FIXED_SCALE = True
    SCALE = 5

    def init(self) -> list[Action]:
        return [
            TdAction(
                f"""\
> DROP TABLE IF EXISTS t1 CASCADE
> CREATE TABLE t1 (id INT, category INT, val INT)
> INSERT INTO t1 SELECT g, g % 100, g * 10 FROM generate_series(1, {self.n()}) AS g
> CREATE DEFAULT INDEX ON t1
> CREATE STANDING QUERY sq1 (cat INT) AS SELECT id, category, val FROM t1 WHERE category = cat
"""
            ),
        ]

    def benchmark(self) -> MeasurementSource:
        # Execute the standing query repeatedly.
        repeated_executes = "\n".join(
            f"> EXECUTE STANDING QUERY sq1 ({i % 100})" for i in range(10)
        )

        return Td(
            f"""\
> SELECT 1
  /* A */
1

{repeated_executes}

> SELECT 1
  /* B */
1
"""
        )
