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


def schema() -> str:
    return dedent(
        """
       $ set schema={
           "type" : "record",
           "name" : "test",
           "fields" : [
               {"name":"f1", "type":"string"}
           ]
         }
       """
    )


class BasicTopK(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TABLE basic_topk_table (f1 INTEGER);
            > INSERT INTO basic_topk_table VALUES (1), (2), (2), (3), (3), (3), (NULL), (NULL), (NULL), (NULL);
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > INSERT INTO basic_topk_table SELECT * FROM basic_topk_table
                > CREATE MATERIALIZED VIEW basic_topk_view1 AS SELECT f1, COUNT(f1) FROM basic_topk_table GROUP BY f1 ORDER BY f1 DESC NULLS LAST LIMIT 2;
                > INSERT INTO basic_topk_table SELECT * FROM basic_topk_table;
                """,
                """
                > INSERT INTO basic_topk_table SELECT * FROM basic_topk_table;
                > CREATE MATERIALIZED VIEW basic_topk_view2 AS SELECT f1, COUNT(f1) FROM basic_topk_table GROUP BY f1 ORDER BY f1 ASC NULLS FIRST LIMIT 2;
                > INSERT INTO basic_topk_table SELECT * FROM basic_topk_table;
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SHOW CREATE MATERIALIZED VIEW basic_topk_view1;
                materialize.public.basic_topk_view1 "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"basic_topk_view1\\" IN CLUSTER \\"default\\" AS SELECT \\"f1\\", \\"pg_catalog\\".\\"count\\"(\\"f1\\") FROM \\"materialize\\".\\"public\\".\\"basic_topk_table\\" GROUP BY \\"f1\\" ORDER BY \\"f1\\" DESC NULLS LAST LIMIT 2"

                > SELECT * FROM basic_topk_view1;
                2 32
                3 48

                > SHOW CREATE MATERIALIZED VIEW basic_topk_view2;
                materialize.public.basic_topk_view2 "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"basic_topk_view2\\" IN CLUSTER \\"default\\" AS SELECT \\"f1\\", \\"pg_catalog\\".\\"count\\"(\\"f1\\") FROM \\"materialize\\".\\"public\\".\\"basic_topk_table\\" GROUP BY \\"f1\\" ORDER BY \\"f1\\" ASC NULLS FIRST LIMIT 2"

                > SELECT * FROM basic_topk_view2;
                1 16
                <null> 0
                """
            )
        )


class MonotonicTopK(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            schema()
            + dedent(
                """
                $ kafka-create-topic topic=monotonic-topk

                $ kafka-ingest format=avro topic=monotonic-topk schema=${schema} publish=true repeat=1
                {"f1": "A"}

                > CREATE SOURCE monotonic_topk_source
                  FROM KAFKA BROKER '${testdrive.kafka-addr}'
                  TOPIC 'testdrive-monotonic-topk-${testdrive.seed}'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
                  ENVELOPE NONE
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(schema() + dedent(s))
            for s in [
                """
                $ kafka-ingest format=avro topic=monotonic-topk schema=${schema} publish=true repeat=2
                {"f1": "B"}
                > CREATE MATERIALIZED VIEW monotonic_topk_view1 AS SELECT f1, COUNT(f1) FROM monotonic_topk_source GROUP BY f1 ORDER BY f1 DESC NULLS LAST LIMIT 2;
                $ kafka-ingest format=avro topic=monotonic-topk schema=${schema} publish=true repeat=3
                {"f1": "C"}
                """,
                """
                $ kafka-ingest format=avro topic=monotonic-topk schema=${schema} publish=true repeat=4
                {"f1": "D"}
                > CREATE MATERIALIZED VIEW monotonic_topk_view2 AS SELECT f1, COUNT(f1) FROM monotonic_topk_source GROUP BY f1 ORDER BY f1 ASC NULLS FIRST LIMIT 2;
                $ kafka-ingest format=avro topic=monotonic-topk schema=${schema} publish=true repeat=5
                {"f1": "E"}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SHOW CREATE MATERIALIZED VIEW monotonic_topk_view1;
                materialize.public.monotonic_topk_view1 "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"monotonic_topk_view1\\" IN CLUSTER \\"default\\" AS SELECT \\"f1\\", \\"pg_catalog\\".\\"count\\"(\\"f1\\") FROM \\"materialize\\".\\"public\\".\\"monotonic_topk_source\\" GROUP BY \\"f1\\" ORDER BY \\"f1\\" DESC NULLS LAST LIMIT 2"

                > SELECT * FROM monotonic_topk_view1;
                E 5
                D 4

                > SHOW CREATE MATERIALIZED VIEW monotonic_topk_view2;
                materialize.public.monotonic_topk_view2 "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"monotonic_topk_view2\\" IN CLUSTER \\"default\\" AS SELECT \\"f1\\", \\"pg_catalog\\".\\"count\\"(\\"f1\\") FROM \\"materialize\\".\\"public\\".\\"monotonic_topk_source\\" GROUP BY \\"f1\\" ORDER BY \\"f1\\" ASC NULLS FIRST LIMIT 2"

                > SELECT * FROM monotonic_topk_view2;
                A 1
                B 2
                """
            )
        )


class MonotonicTop1(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            schema()
            + dedent(
                """
                $ kafka-create-topic topic=monotonic-top1

                $ kafka-ingest format=avro topic=monotonic-top1 schema=${schema} publish=true repeat=1
                {"f1": "A"}

                > CREATE SOURCE monotonic_top1_source
                  FROM KAFKA BROKER '${testdrive.kafka-addr}'
                  TOPIC 'testdrive-monotonic-top1-${testdrive.seed}'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
                  ENVELOPE NONE
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(schema() + dedent(s))
            for s in [
                """
                $ kafka-ingest format=avro topic=monotonic-top1 schema=${schema} publish=true repeat=2
                {"f1": "B"}
                > CREATE MATERIALIZED VIEW monotonic_top1_view1 AS SELECT f1, COUNT(f1) FROM monotonic_top1_source GROUP BY f1 ORDER BY f1 DESC NULLS LAST LIMIT 1;
                $ kafka-ingest format=avro topic=monotonic-top1 schema=${schema} publish=true repeat=3
                {"f1": "C"}
                """,
                """
                $ kafka-ingest format=avro topic=monotonic-top1 schema=${schema} publish=true repeat=4
                {"f1": "C"}
                > CREATE MATERIALIZED VIEW monotonic_top1_view2 AS SELECT f1, COUNT(f1) FROM monotonic_top1_source GROUP BY f1 ORDER BY f1 ASC NULLS FIRST LIMIT 1;
                $ kafka-ingest format=avro topic=monotonic-top1 schema=${schema} publish=true repeat=5
                {"f1": "D"}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SHOW CREATE MATERIALIZED VIEW monotonic_top1_view1;
                materialize.public.monotonic_top1_view1 "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"monotonic_top1_view1\\" IN CLUSTER \\"default\\" AS SELECT \\"f1\\", \\"pg_catalog\\".\\"count\\"(\\"f1\\") FROM \\"materialize\\".\\"public\\".\\"monotonic_top1_source\\" GROUP BY \\"f1\\" ORDER BY \\"f1\\" DESC NULLS LAST LIMIT 1"

                > SELECT * FROM monotonic_top1_view1;
                D 5

                > SHOW CREATE MATERIALIZED VIEW monotonic_top1_view2;
                materialize.public.monotonic_top1_view2 "CREATE MATERIALIZED VIEW \\"materialize\\".\\"public\\".\\"monotonic_top1_view2\\" IN CLUSTER \\"default\\" AS SELECT \\"f1\\", \\"pg_catalog\\".\\"count\\"(\\"f1\\") FROM \\"materialize\\".\\"public\\".\\"monotonic_top1_source\\" GROUP BY \\"f1\\" ORDER BY \\"f1\\" ASC NULLS FIRST LIMIT 1"

                > SELECT * FROM monotonic_top1_view2;
                A 1
                """
            )
        )
