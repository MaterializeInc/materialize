# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.feature_benchmark.action import Action, TdAction
from materialize.feature_benchmark.measurement_source import MeasurementSource, Td
from materialize.feature_benchmark.scenario import Scenario


class CustomerWorkload1(Scenario):
    """Aggregation over a large non-monotonic source."""

    def init(self) -> Action:
        return TdAction(
            dedent(
                f"""
                > DROP TABLE IF EXISTS t1 CASCADE;

                > CREATE TABLE t1 (f1 INTEGER);

                > INSERT INTO t1 SELECT * FROM generate_series(1, {self.n()});
                """
            )
        )

    def benchmark(self) -> MeasurementSource:
        return Td(
            dedent(
                f"""
                > DROP MATERIALIZED VIEW IF EXISTS v1;

                > CREATE MATERIALIZED VIEW v1 AS SELECT MAX(f1) FROM t1
                  /* A */;

                > SELECT * FROM v1
                  /* B */;
                {self.n()}
                """
            )
        )
