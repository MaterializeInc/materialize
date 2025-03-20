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
from materialize.checks.checks import Check, externally_idempotent


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

    def manipulate(self) -> list[Testdrive]:
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

    def manipulate(self) -> list[Testdrive]:
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

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(s)
            for s in [
                dedent(
                    """
                > DELETE FROM dataflow_error_retraction_table WHERE f1 = 'abc'
                """
                ),
                dedent(
                    """
                > DELETE FROM dataflow_error_retraction_table WHERE f1 = 'klm'
                """
                ),
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


@externally_idempotent(False)
class DecodeError(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            schemas()
            + dedent(
                """
                $ kafka-create-topic topic=decode-error

                $ kafka-ingest format=avro topic=decode-error schema=${schema-f1} repeat=1
                {"f1": "A"}

                > CREATE SOURCE decode_error_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-decode-error-${testdrive.seed}')
                > CREATE TABLE decode_error FROM SOURCE decode_error_src (REFERENCE "testdrive-decode-error-${testdrive.seed}")
                  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
                  ENVELOPE NONE
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(schemas() + dedent(s))
            for s in [
                """
                # {"f2": 123456789}, no publish
                $ kafka-ingest format=bytes topic=decode-error repeat=1
                \\x00\x00\x00\x00\x01\xaa\xb4\xde\x75
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


class DecodeErrorUpsertValue(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ kafka-create-topic topic=decode-error-upsert-value

                $ set schema={
                  "type" : "record",
                  "name" : "test",
                  "fields" : [
                    {"name":"f1", "type":"int"}
                    ]
                  }

                $ kafka-ingest format=avro topic=decode-error-upsert-value key-format=bytes key-terminator=: schema=${schema}
                key0: {"f1": 1}
                key1: {"f1": 2}
                key2: {"f1": 3}

                > CREATE CLUSTER decode_error_upsert_value_cluster SIZE '1';

                > CREATE SOURCE decode_error_upsert_value_src
                  IN CLUSTER decode_error_upsert_value_cluster
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-decode-error-upsert-value-${testdrive.seed}')
                > CREATE TABLE decode_error_upsert_value FROM SOURCE decode_error_upsert_value_src (REFERENCE "testdrive-decode-error-upsert-value-${testdrive.seed}")
                  KEY FORMAT TEXT
                  VALUE FORMAT AVRO USING SCHEMA '${schema}'
                  ENVELOPE UPSERT

                $ kafka-ingest topic=decode-error-upsert-value key-format=bytes key-terminator=: format=bytes
                key1: garbage

                ! SELECT * FROM decode_error_upsert_value
                contains: avro deserialization error
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(
                dedent(
                    """
                    $ kafka-ingest topic=decode-error-upsert-value key-format=bytes key-terminator=: format=bytes
                    key2: garbage2

                    ! SELECT * FROM decode_error_upsert_value
                    contains: avro deserialization error
                    """
                )
            ),
            Testdrive(
                dedent(
                    """
                    # Ingest valid avro, but with an incompatible schema
                    $ set schema-string={
                       "type" : "record",
                       "name" : "test",
                       "fields" : [
                        {"name":"f1", "type":"string"}
                       ]
                      }

                    $ kafka-ingest topic=decode-error-upsert-value key-format=bytes key-terminator=: format=avro schema=${schema-string} confluent-wire-format=false
                    key3: {"f1": "garbage3"}

                    ! SELECT * FROM decode_error_upsert_value
                    contains: avro deserialization error
                    """,
                )
            ),
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                # Retract all the garbage and confirm the source is now operational

                $ kafka-ingest topic=decode-error-upsert-value key-format=bytes key-terminator=: format=bytes
                key1:
                key2:
                key3:

                > SELECT f1 FROM decode_error_upsert_value
                1
                """
            )
        )


class DecodeErrorUpsertKey(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                $ kafka-create-topic topic=decode-error-upsert-key

                $ set key-schema={
                  "type" : "record",
                  "name" : "test",
                  "fields" : [
                    {"name":"f1", "type":"int"}
                    ]
                  }

                $ kafka-ingest topic=decode-error-upsert-key key-format=avro format=bytes key-schema=${key-schema}
                {"f1": 1} value1
                {"f1": 2} value2

                > CREATE SOURCE decode_error_upsert_key_src
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-decode-error-upsert-key-${testdrive.seed}')
                > CREATE TABLE decode_error_upsert_key FROM SOURCE decode_error_upsert_key_src (REFERENCE "testdrive-decode-error-upsert-key-${testdrive.seed}")
                  KEY FORMAT AVRO USING SCHEMA '${key-schema}'
                  VALUE FORMAT BYTES
                  ENVELOPE UPSERT

                $ kafka-ingest topic=decode-error-upsert-key key-format=bytes key-terminator=: format=bytes
                garbage1: value3

                ! SELECT * FROM decode_error_upsert_key
                contains: avro deserialization error
            """
            )
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive(
                dedent(
                    """
                    # Retract existing garbage
                    $ kafka-ingest topic=decode-error-upsert-key key-format=bytes format=bytes key-terminator=:
                    garbage1:

                    # And introduce a new one -- valid avro, but with an incompatible schema
                    $ set key-schema-string={
                       "type" : "record",
                       "name" : "test",
                       "fields" : [
                        {"name":"f1", "type":"string"}
                       ]
                      }

                    $ kafka-ingest topic=decode-error-upsert-key key-format=avro format=bytes key-schema=${key-schema-string} confluent-wire-format=false
                    {"f1": "garbage2"} value4

                    ! SELECT * FROM decode_error_upsert_key
                    contains: avro deserialization error
                    """
                )
            ),
            Testdrive(
                dedent(
                    """
                    # Retract existing garbage and introduce a new one
                    $ kafka-ingest topic=decode-error-upsert-key key-format=bytes format=bytes key-terminator=:
                    garbage3: value4

                    $ set key-schema-string={
                       "type" : "record",
                       "name" : "test",
                       "fields" : [
                        {"name":"f1", "type":"string"}
                       ]
                      }

                    $ kafka-ingest topic=decode-error-upsert-key key-format=avro format=bytes key-schema=${key-schema-string} confluent-wire-format=false
                    {"f1": "garbage2"}

                    ! SELECT * FROM decode_error_upsert_key
                    contains: avro deserialization error
                    """,
                )
            ),
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                # Retract any remaining garbage
                $ kafka-ingest topic=decode-error-upsert-key key-format=bytes format=bytes key-terminator=:
                garbage3:

                # Source should return to operational status
                > SELECT f1 FROM decode_error_upsert_key
                1
                2
                """
            )
        )
