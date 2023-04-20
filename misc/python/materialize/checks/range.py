# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
from textwrap import dedent
from typing import List

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check
from materialize.checks.common import SAMPLE_KAFKA_SCHEMA


def schema() -> str:
    return dedent(SAMPLE_KAFKA_SCHEMA)


class Range(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            schema()
            + dedent(
                """
            > CREATE TABLE range_table (
                index INT,
                i4_range INT4RANGE,
                i8_range INT8RANGE,
                num_range NUMRANGE,
                ts_range TSRANGE,
                tstz_range TSTZRANGE,
                d_range DATERANGE
              );

            > INSERT INTO range_table VALUES (
                1,
                '[2,8]'::INT4RANGE,
                '[2,100]'::INT8RANGE,
                '[400,600]'::NUMRANGE,
                '[2023-01-01,2023-03-01)'::TSRANGE,
                '[2023-01-01,2023-03-01)'::TSTZRANGE,
                '[2023-01-01,2023-03-01)'::DATERANGE
              );
            > INSERT INTO range_table VALUES (
                2,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL
              );

            $ kafka-create-topic topic=ranges

            $ kafka-ingest format=avro topic=ranges schema=${schema} repeat=10
            {"f1": "A${kafka-ingest.iteration}"}

            > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

            > CREATE CONNECTION IF NOT EXISTS csr_conn FOR CONFLUENT SCHEMA REGISTRY URL '${testdrive.schema-registry-url}';

            > CREATE SOURCE range_source
              FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-ranges-${testdrive.seed}')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE NONE
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        manipulation = """
                > CREATE MATERIALIZED VIEW $view_name$ AS
                  SELECT
                    index,
                    i4_range,
                    i8_range,
                    num_range,
                    ts_range,
                    tstz_range,
                    d_range,
                    '[2,5]'::INT8RANGE AS i8_range2,
                    INT8RANGE(2,4) AS i8_range3,
                    GREATEST(i8_range, '[2,5]'::INT8RANGE) AS i8_greatest,
                    LEAST(i8_range, '[2,5]'::INT8RANGE) AS i8_least,
                    NULLIF(i8_range, '[2,17)'::INT8RANGE) AS i8_nif,
                    LOWER(i8_range) AS i8_low,
                    UPPER(i8_range) AS i8_up,
                    ISEMPTY(i8_range) AS i8_empty,
                    LOWER_INC(i8_range) AS i8_lowinc,
                    UPPER_INC(i8_range) AS i8_upinc,
                    LOWER_INF(i8_range) AS i8_lowinf,
                    UPPER_INF(i8_range) AS i8_upinf,
                    i8_range < '[2,16]'::INT8RANGE AS i8_lt,
                    i8_range <= '[2,16]'::INT8RANGE AS i8_le,
                    i8_range = '[2,16]'::INT8RANGE AS i8_eq,
                    i8_range >= '[2,16]'::INT8RANGE AS i8_ge,
                    i8_range > '[2,16]'::INT8RANGE AS i8_gt,
                    i8_range @> INT8RANGE(2,3) AS i8_containsrange,
                    i8_range @> 16::INT8 as i8_containselem,
                    i8_range <@ INT8RANGE(2,3) AS i8_inrange,
                    16::INT8 <@ i8_range as i8_inelem,
                    i8_range && INT8RANGE(4,12) AS i8_overlap,
                    i8_range << INT8RANGE(400, 500) AS i8_leftof,
                    i8_range >> INT8RANGE(0, 1) AS i8_rightof,
                    i8_range &< INT8RANGE(0, 1) AS i8_notextright,
                    i8_range &> INT8RANGE(0, 1) AS i8_notextleft,
                    i8_range -|- INT8RANGE(100, 150) AS i8_adjacent,
                    i8_range + '[3,20]'::INT8RANGE AS i8_merge,
                    i8_range * '[8,20]'::INT8RANGE AS i8_intersec,
                    i8_range - '[8,120]'::INT8RANGE AS i8_diff,
                    (SELECT partition FROM range_source_progress LIMIT 1) AS progress_range
                  FROM range_table;

                > INSERT INTO range_table SELECT * FROM range_table WHERE index = 1;
            """

        return [
            Testdrive(dedent(s))
            for s in [
                manipulation.replace("$view_name$", "range_view1"),
                manipulation.replace("$view_name$", "range_view2"),
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > SELECT * FROM range_table ORDER BY index ASC;
            1 [2,9) [2,101) [400,600] "[2023-01-01 00:00:00,2023-03-01 00:00:00)" "[2023-01-01 00:00:00 UTC,2023-03-01 00:00:00 UTC)" [2023-01-01,2023-03-01)
            1 [2,9) [2,101) [400,600] "[2023-01-01 00:00:00,2023-03-01 00:00:00)" "[2023-01-01 00:00:00 UTC,2023-03-01 00:00:00 UTC)" [2023-01-01,2023-03-01)
            1 [2,9) [2,101) [400,600] "[2023-01-01 00:00:00,2023-03-01 00:00:00)" "[2023-01-01 00:00:00 UTC,2023-03-01 00:00:00 UTC)" [2023-01-01,2023-03-01)
            1 [2,9) [2,101) [400,600] "[2023-01-01 00:00:00,2023-03-01 00:00:00)" "[2023-01-01 00:00:00 UTC,2023-03-01 00:00:00 UTC)" [2023-01-01,2023-03-01)
            2 <null> <null> <null> <null> <null> <null>

            > SELECT * FROM range_view1 ORDER BY index ASC;
            1 [2,9) [2,101) [400,600] "[2023-01-01 00:00:00,2023-03-01 00:00:00)" "[2023-01-01 00:00:00 UTC,2023-03-01 00:00:00 UTC)" [2023-01-01,2023-03-01) [2,6) [2,4) [2,101) [2,6) [2,101) 2 101 false true false false false false false false true true true true false true true true true false true false [2,101) [8,21) [2,8) [0,0]
            1 [2,9) [2,101) [400,600] "[2023-01-01 00:00:00,2023-03-01 00:00:00)" "[2023-01-01 00:00:00 UTC,2023-03-01 00:00:00 UTC)" [2023-01-01,2023-03-01) [2,6) [2,4) [2,101) [2,6) [2,101) 2 101 false true false false false false false false true true true true false true true true true false true false [2,101) [8,21) [2,8) [0,0]
            1 [2,9) [2,101) [400,600] "[2023-01-01 00:00:00,2023-03-01 00:00:00)" "[2023-01-01 00:00:00 UTC,2023-03-01 00:00:00 UTC)" [2023-01-01,2023-03-01) [2,6) [2,4) [2,101) [2,6) [2,101) 2 101 false true false false false false false false true true true true false true true true true false true false [2,101) [8,21) [2,8) [0,0]
            1 [2,9) [2,101) [400,600] "[2023-01-01 00:00:00,2023-03-01 00:00:00)" "[2023-01-01 00:00:00 UTC,2023-03-01 00:00:00 UTC)" [2023-01-01,2023-03-01) [2,6) [2,4) [2,101) [2,6) [2,101) 2 101 false true false false false false false false true true true true false true true true true false true false [2,101) [8,21) [2,8) [0,0]
            2 <null> <null> <null> <null> <null> <null> [2,6) [2,4) [2,6) [2,6) <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> [0,0]

            > SELECT * FROM range_view2 ORDER BY index ASC;
            1 [2,9) [2,101) [400,600] "[2023-01-01 00:00:00,2023-03-01 00:00:00)" "[2023-01-01 00:00:00 UTC,2023-03-01 00:00:00 UTC)" [2023-01-01,2023-03-01) [2,6) [2,4) [2,101) [2,6) [2,101) 2 101 false true false false false false false false true true true true false true true true true false true false [2,101) [8,21) [2,8) [0,0]
            1 [2,9) [2,101) [400,600] "[2023-01-01 00:00:00,2023-03-01 00:00:00)" "[2023-01-01 00:00:00 UTC,2023-03-01 00:00:00 UTC)" [2023-01-01,2023-03-01) [2,6) [2,4) [2,101) [2,6) [2,101) 2 101 false true false false false false false false true true true true false true true true true false true false [2,101) [8,21) [2,8) [0,0]
            1 [2,9) [2,101) [400,600] "[2023-01-01 00:00:00,2023-03-01 00:00:00)" "[2023-01-01 00:00:00 UTC,2023-03-01 00:00:00 UTC)" [2023-01-01,2023-03-01) [2,6) [2,4) [2,101) [2,6) [2,101) 2 101 false true false false false false false false true true true true false true true true true false true false [2,101) [8,21) [2,8) [0,0]
            1 [2,9) [2,101) [400,600] "[2023-01-01 00:00:00,2023-03-01 00:00:00)" "[2023-01-01 00:00:00 UTC,2023-03-01 00:00:00 UTC)" [2023-01-01,2023-03-01) [2,6) [2,4) [2,101) [2,6) [2,101) 2 101 false true false false false false false false true true true true false true true true true false true false [2,101) [8,21) [2,8) [0,0]
            2 <null> <null> <null> <null> <null> <null> [2,6) [2,4) [2,6) [2,6) <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> <null> [0,0]
            """
            )
        )
