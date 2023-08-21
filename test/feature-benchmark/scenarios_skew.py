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

from materialize.feature_benchmark.measurement_source import MeasurementSource, Td
from materialize.feature_benchmark.scenario import Scenario


class SkewedJoin(Scenario):
    def benchmark(self) -> MeasurementSource:
        scale = self.scale()
        count = 10**scale

        return Td(
            dedent(
                f"""
                > DROP TABLE IF EXISTS skewed_table CASCADE;
                > DROP TABLE IF EXISTS uniform_table CASCADE;

                > CREATE TABLE skewed_table(f1 INTEGER);
                > CREATE TABLE uniform_table (f1 INTEGER);

                > CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(*) > 0 FROM skewed_table JOIN uniform_table USING (f1)
                  /* A */

                > INSERT INTO uniform_table (f1) SELECT generate_series FROM generate_series(0, {count-1}::integer);

                # Make sure 0 is overrepresented
                > INSERT INTO skewed_table (f1) SELECT 0 FROM generate_series(1, {count}::integer);
                """
            )
            + "\n".join(
                [
                    f"> INSERT INTO skewed_table (f1) SELECT MOD(generate_series, POW(10, {i})) FROM generate_series(1, ({count} / {scale})::integer);"
                    for i in range(floor(scale))
                ]
            )
            + dedent(
                """
                > SELECT * FROM v1
                  /* B */
                true
                """
            )
        )
