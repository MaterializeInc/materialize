# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from math import floor
from textwrap import dedent
from typing import List

from materialize.feature_benchmark.action import Action, TdAction
from materialize.feature_benchmark.measurement_source import MeasurementSource, Td
from materialize.feature_benchmark.scenario import Scenario


def create_skewed_table(scale: int) -> str:
    count = 10**scale
    return (
        dedent(
            f"""
       > CREATE TABLE skewed_table(f1 INTEGER);
       # Make sure 0 is overepresented
       > INSERT INTO skewed_table (f1) SELECT 0 FROM generate_series(1, {count});
       """
        )
        + "\n".join(
            [
                f"> INSERT INTO skewed_table (f1) SELECT MOD(generate_series, POW(10, {i})) FROM generate_series(1, {count} / {scale});"
                for i in range(scale)
            ]
        )
    )


def create_uniform_table(scale: int) -> str:
    count = 10**scale
    return dedent(
        f"""
        > CREATE TABLE uniform_table (f1 INTEGER);
        > INSERT INTO uniform_table (f1) SELECT generate_series FROM generate_series(0, {count-1});
        """
    )


class SkewedJoin(Scenario):
    def init(self) -> List[Action]:
        return [
            TdAction(
                create_skewed_table(floor(self.scale()))
                + create_uniform_table(floor(self.scale()))
            )
        ]

    def benchmark(self) -> MeasurementSource:
        return Td(
            dedent(
                """
                > DROP MATERIALIZED VIEW IF EXISTS v1;

                > CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) > 0 FROM skewed_table JOIN uniform_table USING (f1)
                  /* A */

                > SELECT * FROM v1
                  /* B */
                true
                """
            )
        )
