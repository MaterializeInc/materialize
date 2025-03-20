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


class Comment(Check):
    """Test comments on types and tables, as well as the comment export as avro sink schema docs"""

    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
            > CREATE TYPE comment_type AS (x text, y int, z int)
            > CREATE TYPE comment_int4_list AS LIST (ELEMENT TYPE = int4)
            > CREATE TABLE comment_table (f1 comment_type, f2 comment_int4_list, f3 int)

            > CREATE SINK comment_sink1 FROM comment_table
              INTO KAFKA CONNECTION kafka_conn (TOPIC 'comment-sink1')
              FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
              ENVELOPE DEBEZIUM
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(dedent(s))
            for s in [
                """
                > COMMENT ON TYPE comment_type IS 'comment on comment_type';
                > COMMENT ON COLUMN comment_type.x IS 'comment on comment_type.x';
                > COMMENT ON TYPE comment_int4_list IS 'comment on comment_type';
                > COMMENT ON TABLE comment_table IS 'comment on comment_table';
                > COMMENT ON COLUMN comment_table.f1 IS 'comment on comment_table.f1';

                > CREATE SINK comment_sink2 FROM comment_table
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'comment-sink2')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
            """,
                """
                > COMMENT ON COLUMN comment_type.y IS 'comment on comment_type.y';
                > COMMENT ON COLUMN comment_table.f2 IS 'comment on comment_table.f2';

                > CREATE SINK comment_sink3 FROM comment_table
                  INTO KAFKA CONNECTION kafka_conn (TOPIC 'comment-sink3')
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE DEBEZIUM
            """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > COMMENT ON COLUMN comment_type.z IS 'comment on comment_type.z';
                > COMMENT ON COLUMN comment_table.f3 IS 'comment on comment_table.f3';

                > SELECT name, object_type, object_sub_id, comment FROM mz_internal.mz_comments JOIN mz_objects ON mz_comments.id = mz_objects.id WHERE name LIKE 'comment_%';
                comment_table table 1 "comment on comment_table.f1"
                comment_table table 2 "comment on comment_table.f2"
                comment_table table 3 "comment on comment_table.f3"
                comment_table table <null> "comment on comment_table"
                comment_type type 1 "comment on comment_type.x"
                comment_type type 2 "comment on comment_type.y"
                comment_type type 3 "comment on comment_type.z"
                comment_type type <null> "comment on comment_type"
                comment_int4_list type <null> "comment on comment_type"

                $ schema-registry-verify schema-type=avro subject=comment-sink1-value
                {"type":"record","name":"envelope","fields":[{"name":"before","type":["null",{"type":"record","name":"row","fields":[{"name":"f1","type":["null",{"type":"record","name":"record0","namespace":"com.materialize.sink","fields":[{"name":"x","type":["null","string"]},{"name":"y","type":["null","int"]},{"name":"z","type":["null","int"]}]}]},{"name":"f2","type":["null",{"type":"array","items":["null","int"]}]},{"name":"f3","type":["null","int"]}]}]},{"name":"after","type":["null","row"]}]}

                $ schema-registry-verify schema-type=avro subject=comment-sink2-value
                {"type":"record","name":"envelope","fields":[{"name":"before","type":["null",{"type":"record","name":"row","doc":"comment on comment_table","fields":[{"name":"f1","type":["null",{"type":"record","name":"record0","namespace":"com.materialize.sink","doc":"comment on comment_type","fields":[{"name":"x","type":["null","string"],"doc":"comment on comment_type.x"},{"name":"y","type":["null","int"]},{"name":"z","type":["null","int"]}]}],"doc":"comment on comment_table.f1"},{"name":"f2","type":["null",{"type":"array","items":["null","int"]}]},{"name":"f3","type":["null","int"]}]}]},{"name":"after","type":["null","row"]}]}

                $ schema-registry-verify schema-type=avro subject=comment-sink3-value
                {"type":"record","name":"envelope","fields":[{"name":"before","type":["null",{"type":"record","name":"row","doc":"comment on comment_table","fields":[{"name":"f1","type":["null",{"type":"record","name":"record0","namespace":"com.materialize.sink","doc":"comment on comment_type","fields":[{"name":"x","type":["null","string"],"doc":"comment on comment_type.x"},{"name":"y","type":["null","int"],"doc":"comment on comment_type.y"},{"name":"z","type":["null","int"]}]}],"doc":"comment on comment_table.f1"},{"name":"f2","type":["null",{"type":"array","items":["null","int"]}],"doc":"comment on comment_table.f2"},{"name":"f3","type":["null","int"]}]}]},{"name":"after","type":["null","row"]}]}
            """
            )
        )
