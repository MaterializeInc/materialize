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
    """Measure the time to create an index on a view with a temporal filter over 10M rows.

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
              FROM generate_series(1, 10000000) AS x
              CROSS JOIN anchor;

            > SELECT COUNT(*) FROM mv_temporal;
            10000000

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
            10000000
        """))


class TemporalFilterSustainedInsert(TemporalFilter):
    """Measure ingestion latency through a temporal-filtered indexed view.

    Pre-loads 10M rows into a table that's filtered by a temporal predicate,
    with the filtered view indexed. Then drives several small inserts and
    times how long it takes for the index to reflect them.

    Each input row produces an insert at today + a retract at next-week
    through the temporal MFP. Without bucketing both updates land in the
    index's merge batcher, growing the arrangement spine; with bucketing,
    only today's update lands in the spine while future retractions
    accumulate in the BucketChain. The smaller spine keeps per-insert
    incremental merge work cheaper and that work is on the critical path
    of the trailing SELECT.
    """

    FIXED_SCALE = True

    def benchmark(self) -> MeasurementSource:
        today = date.today()
        next_week = today + timedelta(days=7)
        return Td(dedent(f"""
            > DROP TABLE IF EXISTS t_temporal CASCADE;
            > CREATE TABLE t_temporal (x int);
            > INSERT INTO t_temporal SELECT generate_series(1, 10000000);

            > CREATE VIEW v_temporal AS
              SELECT x FROM t_temporal
              WHERE mz_now() >= extract(epoch from timestamp '{today}')::uint8 * 1000
                AND mz_now() < extract(epoch from timestamp '{next_week}')::uint8 * 1000;

            > CREATE INDEX idx_temporal ON v_temporal (x);

            > SELECT COUNT(*) FROM v_temporal;
            10000000

            > SELECT 1
              /* A */
            1

            > INSERT INTO t_temporal SELECT generate_series(10000001, 10100000);
            > INSERT INTO t_temporal SELECT generate_series(10100001, 10200000);
            > INSERT INTO t_temporal SELECT generate_series(10200001, 10300000);
            > INSERT INTO t_temporal SELECT generate_series(10300001, 10400000);
            > INSERT INTO t_temporal SELECT generate_series(10400001, 10500000);

            > SELECT COUNT(*) FROM v_temporal
              /* B */
            10500000
        """))
