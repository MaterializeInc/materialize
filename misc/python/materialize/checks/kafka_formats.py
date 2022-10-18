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

PROTOBUF = dedent(
    """
    $ file-append path=test.proto
    syntax = "proto3";

    message Key {
        string key1 = 1;
        string key2 = 2;
    }

    message Value {
        string value1 = 1;
        string value2 = 2;
    }

    $ protobuf-compile-descriptors inputs=test.proto output=test.proto set-var=test-schema
    """
)


class KafkaFormats(Check):
    def initialize(self) -> Testdrive:
        return Testdrive(
            PROTOBUF
            + dedent(
                """
                $ kafka-create-topic topic=format-bytes

                $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=format-bytes
                key1A,key1B:value1A,value1B

                $ kafka-create-topic topic=format-protobuf partitions=1

                $ kafka-ingest topic=format-protobuf
                  key-format=protobuf key-descriptor-file=test.proto key-message=Key
                  format=protobuf descriptor-file=test.proto message=Value
                {"key1": "key1A", "key2": "key1B"} {"value1": "value1A", "value2": "value1B"}

                > CREATE CONNECTION IF NOT EXISTS kafka_conn FOR KAFKA BROKER '${testdrive.kafka-addr}';

                > CREATE SOURCE format_bytes1
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-format-bytes-${testdrive.seed}')
                  KEY FORMAT BYTES
                  VALUE FORMAT BYTES
                  ENVELOPE UPSERT

                > CREATE SOURCE format_text1
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-format-bytes-${testdrive.seed}')
                  KEY FORMAT TEXT
                  VALUE FORMAT TEXT
                  ENVELOPE UPSERT

                > CREATE SOURCE format_csv1 (key1, key2, value1, value2)
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-format-bytes-${testdrive.seed}')
                  KEY FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
                  VALUE FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
                  ENVELOPE UPSERT

                > CREATE SOURCE format_regex1 (key1, key2, value1, value2)
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-format-bytes-${testdrive.seed}')
                  KEY FORMAT REGEX '(?P<key1>[^,]+),(?P<key2>\\w+)'
                  VALUE FORMAT REGEX '(?P<value1>[^,]+),(?P<value2>\\w+)'
                  ENVELOPE UPSERT

                > CREATE SOURCE format_protobuf1
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-format-protobuf-${testdrive.seed}')
                  KEY FORMAT PROTOBUF MESSAGE '.Key' USING SCHEMA '${test-schema}'
                  VALUE FORMAT PROTOBUF MESSAGE '.Value' USING SCHEMA '${test-schema}'
                  INCLUDE KEY
            """
            )
        )

    def manipulate(self) -> List[Testdrive]:
        return [
            Testdrive(PROTOBUF + dedent(s))
            for s in [
                """
                > CREATE SOURCE format_bytes2
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-format-bytes-${testdrive.seed}')
                  KEY FORMAT BYTES
                  VALUE FORMAT BYTES
                  ENVELOPE UPSERT

                > CREATE SOURCE format_text2
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-format-bytes-${testdrive.seed}')
                  KEY FORMAT TEXT
                  VALUE FORMAT TEXT
                  ENVELOPE UPSERT

                > CREATE SOURCE format_csv2 (key1, key2, value1, value2)
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-format-bytes-${testdrive.seed}')
                  KEY FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
                  VALUE FORMAT CSV WITH 2 COLUMNS DELIMITED BY ','
                  ENVELOPE UPSERT

                > CREATE SOURCE format_regex2 (key1, key2, value1, value2)
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-format-bytes-${testdrive.seed}')
                  KEY FORMAT REGEX '(?P<key1>[^,]+),(?P<key2>\\w+)'
                  VALUE FORMAT REGEX '(?P<value1>[^,]+),(?P<value2>\\w+)'
                  ENVELOPE UPSERT

                > CREATE SOURCE format_protobuf2
                  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-format-protobuf-${testdrive.seed}')
                  KEY FORMAT PROTOBUF MESSAGE '.Key' USING SCHEMA '${test-schema}'
                  VALUE FORMAT PROTOBUF MESSAGE '.Value' USING SCHEMA '${test-schema}'
                  INCLUDE KEY

                $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=format-bytes
                key2A,key2B:value2A,value2B

                $ kafka-ingest topic=format-protobuf
                  key-format=protobuf key-descriptor-file=test.proto key-message=Key
                  format=protobuf descriptor-file=test.proto message=Value
                {"key1": "key2A", "key2": "key2B"} {"value1": "value2A", "value2": "value2B"}
                """,
                """
                $ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=format-bytes
                key3A,key3B:value3A,value3B

                $ kafka-ingest topic=format-protobuf
                  key-format=protobuf key-descriptor-file=test.proto key-message=Key
                  format=protobuf descriptor-file=test.proto message=Value
                {"key1": "key3A", "key2": "key3B"} {"value1": "value3A", "value2": "value3B"}
                """,
            ]
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            dedent(
                """
                > SELECT COUNT(*) FROM format_bytes1
                3

                > SELECT * FROM format_text1
                key1A,key1B value1A,value1B
                key2A,key2B value2A,value2B
                key3A,key3B value3A,value3B

                > SELECT * FROM format_csv1
                key1A key1B value1A value1B
                key2A key2B value2A value2B
                key3A key3B value3A value3B

                > SELECT * FROM format_regex1
                key1A key1B value1A value1B
                key2A key2B value2A value2B
                key3A key3B value3A value3B

                > SELECT * FROM format_protobuf1
                key1A key1B value1A value1B
                key2A key2B value2A value2B
                key3A key3B value3A value3B

                > SELECT * FROM format_text2
                key1A,key1B value1A,value1B
                key2A,key2B value2A,value2B
                key3A,key3B value3A,value3B

                > SELECT * FROM format_csv2
                key1A key1B value1A value1B
                key2A key2B value2A value2B
                key3A key3B value3A value3B

                > SELECT * FROM format_regex2
                key1A key1B value1A value1B
                key2A key2B value2A value2B
                key3A key3B value3A value3B

                > SELECT * FROM format_protobuf2
                key1A key1B value1A value1B
                key2A key2B value2A value2B
                key3A key3B value3A value3B
                """
            )
        )
