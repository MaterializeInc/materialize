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


class ParseError(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE parse_error_table (f1 STRING);
                > CREATE MATERIALIZED VIEW parse_error_view AS SELECT f1::INTEGER FROM parse_error_table;
                > INSERT INTO parse_error_table VALUES ('123');
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(s)
            for s in [
                "> INSERT INTO parse_error_table VALUES ('abc'), ('234');",
                "> INSERT INTO parse_error_table VALUES ('345'), ('klm');",
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                ! SELECT * FROM parse_error_view;
                contains: invalid input syntax for type integer
                """
            )
        )


class ParseHexError(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE parse_hex_error_table (f1 STRING);
                > CREATE MATERIALIZED VIEW parse_hex_error_view AS SELECT decode(f1, 'hex') FROM parse_hex_error_table;
                > INSERT INTO parse_hex_error_table VALUES ('aa');
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(s)
            for s in [
                "> INSERT INTO parse_hex_error_table VALUES ('bb'), ('xx');",
                "> INSERT INTO parse_hex_error_table VALUES ('yy'), ('cc');",
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                ! SELECT * FROM parse_hex_error_view;
                contains: invalid hexadecimal digit
                """
            )
        )


class DataflowErrorRetraction(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > CREATE TABLE dataflow_error_retraction_table (f1 STRING);
                > CREATE MATERIALIZED VIEW dataflow_error_retraction_view AS SELECT f1::INTEGER FROM dataflow_error_retraction_table;
                > INSERT INTO dataflow_error_retraction_table VALUES ('123');
                > INSERT INTO dataflow_error_retraction_table VALUES ('abc');
                > INSERT INTO dataflow_error_retraction_table VALUES ('klm');
                > INSERT INTO dataflow_error_retraction_table VALUES ('234');
                ! SELECT * FROM dataflow_error_retraction_view;
                contains: invalid input syntax for type integer
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(s)
            for s in [
                "> DELETE FROM dataflow_error_retraction_table WHERE f1 = 'abc'",
                "> DELETE FROM dataflow_error_retraction_table WHERE f1 = 'klm'",
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT * FROM dataflow_error_retraction_view;
                123
                234
                """
            )
        )


def schemas() -> str:
    return dedent(
        """
       $ set schema-f1={
           "type" : "record",
           "name" : "test",
           "fields" : [
               {"name":"f1", "type":"string"}
           ]
         }

       $ set schema-f2={
           "type" : "record",
           "name" : "test",
           "fields" : [
               {"name":"f2", "type":"int"}
           ]
         }
       """
    )


class DecodeError(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                $ kafka-create-topic topic=decode-error

                $ kafka-ingest format=avro topic=decode-error schema=${schema-f1} publish=true repeat=1
                {"f1": "A"}

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE CONNECTION IF NOT EXISTS csr_conn
                  FOR CONFLUENT SCHEMA REGISTRY
                  URL '${testdrive.schema-registry-url}';

                > CREATE SOURCE decode_error
                  FROM KAFKA CONNECTION kafka_conn
                  TOPIC 'testdrive-decode-error-${testdrive.seed}'
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                $ kafka-ingest format=avro topic=decode-error schema=${schema-f2} repeat=1
                {"f2": 123456789}
                """,
                """
                $ kafka-ingest format=bytes topic=decode-error repeat=1
                ABCD
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                ! SELECT * FROM decode_error
                contains: Decode error
                """
            )
        )
