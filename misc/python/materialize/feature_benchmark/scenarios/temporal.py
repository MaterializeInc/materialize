# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from datetime import date, timedelta
from textwrap import dedent

from materialize.feature_benchmark.action import Action, TdAction
from materialize.feature_benchmark.measurement_source import MeasurementSource, Td
from materialize.feature_benchmark.scenario import Scenario


class TemporalFilter(Scenario):
    """Feature benchmarks related to temporal filters using mz_now()."""


class TemporalFilterIndexed(TemporalFilter):
    """Measure the time to create an index on a view with a temporal filter over 1M rows.

    The view applies a temporal filter (mz_now() range) on top of a materialized
    view joined with a table, ensuring the collection has future updates that
    require temporal bucketing before arrangement.
    """

    FIXED_SCALE = True

    def init(self) -> list[Action]:
        return [
            TdAction(dedent("""
                > DROP TABLE IF EXISTS anchor CASCADE;
                > CREATE TABLE anchor (a int);
                > INSERT INTO anchor VALUES (1);
            """)),
        ]

    def benchmark(self) -> MeasurementSource:
        today = date.today()
        next_week = today + timedelta(days=7)
        return Td(dedent(f"""
            > DROP VIEW IF EXISTS v_temporal CASCADE;
            > DROP MATERIALIZED VIEW IF EXISTS mv_temporal CASCADE;

            > CREATE MATERIALIZED VIEW mv_temporal AS
              SELECT x, a
              FROM generate_series(1, 1000000) AS x
              CROSS JOIN anchor;

            > SELECT COUNT(*) FROM mv_temporal;
            1000000

            > SELECT 1
              /* A */
            1

            > CREATE VIEW v_temporal AS
              SELECT x, a FROM mv_temporal
              WHERE mz_now() >= extract(epoch from timestamp '{today}')::uint8 * 1000
                AND mz_now() < extract(epoch from timestamp '{next_week}')::uint8 * 1000;

            > CREATE INDEX idx_temporal ON v_temporal (x);

            > SELECT COUNT(*) FROM v_temporal
              /* B */
            1000000
        """))
