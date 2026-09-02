# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check


class AggregateFunctions(Check):
    """Aggregate functions beyond COUNT/MIN/MAX/SUM (see aggregation.py):
    the *_agg family, statistical aggregates, bool_and/bool_or, FILTER, and
    ORDER BY / DISTINCT within aggregates."""

    def initialize(self) -> Testdrive:
        return Testdrive(dedent("""
            > CREATE TABLE agg_fns_table (grp INT, val INT, txt STRING)
            > INSERT INTO agg_fns_table VALUES (1, 10, 'a'), (1, 20, 'b'), (2, 30, 'c')
            """))

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO agg_fns_table VALUES (2, 40, 'd')

                > CREATE MATERIALIZED VIEW agg_fns_stats1 AS
                  SELECT
                    grp,
                    avg(val) AS avg_val,
                    round(stddev_samp(val), 4) AS stddev_samp_val,
                    round(stddev_pop(val), 4) AS stddev_pop_val,
                    var_samp(val) AS var_samp_val,
                    var_pop(val) AS var_pop_val,
                    bool_and(val >= 20) AS ba,
                    bool_or(val >= 20) AS bo
                  FROM agg_fns_table
                  GROUP BY grp

                > CREATE MATERIALIZED VIEW agg_fns_lists1 AS
                  SELECT
                    grp,
                    array_agg(txt ORDER BY txt) AS arr,
                    string_agg(txt, ',' ORDER BY txt DESC) AS str,
                    list_agg(txt ORDER BY txt) AS lst,
                    jsonb_agg(val ORDER BY val) AS jarr,
                    jsonb_object_agg(txt, val) AS jobj,
                    map_agg(txt, val) AS mp
                  FROM agg_fns_table
                  GROUP BY grp
                """,
                """
                > INSERT INTO agg_fns_table VALUES (3, 50, 'e'), (3, 50, 'f')

                > CREATE MATERIALIZED VIEW agg_fns_filter1 AS
                  SELECT
                    grp,
                    count(*) FILTER (WHERE val >= 20) AS cnt_filter,
                    sum(val) FILTER (WHERE txt < 'e') AS sum_filter,
                    sum(DISTINCT val) AS sum_distinct,
                    count(DISTINCT val) AS cnt_distinct
                  FROM agg_fns_table
                  GROUP BY grp

                # Query hints from the SELECT documentation.
                > CREATE MATERIALIZED VIEW agg_fns_hint1 AS
                  SELECT grp, count(DISTINCT val) AS cnt
                  FROM agg_fns_table
                  GROUP BY grp
                  OPTIONS (AGGREGATE INPUT GROUP SIZE = 100)

                > CREATE MATERIALIZED VIEW agg_fns_hint2 AS
                  SELECT DISTINCT ON (grp) grp, val
                  FROM agg_fns_table
                  OPTIONS (DISTINCT ON INPUT GROUP SIZE = 100)
                  ORDER BY grp, val DESC
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(dedent("""
            > SELECT * FROM agg_fns_stats1
            1 15 7.0711 5 50 25 false true
            2 35 7.0711 5 50 25 true true
            3 50 0 0 0 0 true true

            > SELECT grp, arr::text, str, lst::text, jarr::text, jobj::text, mp::text FROM agg_fns_lists1
            1 {a,b} b,a {a,b} [10,20] "{\\"a\\":10,\\"b\\":20}" {a=>10,b=>20}
            2 {c,d} d,c {c,d} [30,40] "{\\"c\\":30,\\"d\\":40}" {c=>30,d=>40}
            3 {e,f} f,e {e,f} [50,50] "{\\"e\\":50,\\"f\\":50}" {e=>50,f=>50}

            > SELECT * FROM agg_fns_filter1
            1 1 30 30 2
            2 2 70 70 2
            3 2 <null> 50 1

            > SELECT * FROM agg_fns_hint1
            1 2
            2 2
            3 1

            > SELECT * FROM agg_fns_hint2
            1 20
            2 40
            3 50
            """))
