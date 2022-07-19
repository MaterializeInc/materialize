# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that reading an Avro buffer with a schema different from the one that was used
# when it was written does not cause panics or anything like that.
#

$ set 1column={"type": "record", "name": "schema_1column", "fields": [ {"name": "f1", "type": "int"} ] }
$ set 2columns-nodefault={"type": "record", "name": "schema_2columns_nodefault", "fields": [ {"name": "f1", "type": "int"} , {"name": "f2", "type": "int"} ] }
$ set 2columns-default={"type": "record", "name": "schema_2columns_default", "fields": [ {"name": "f1", "type": "int"} , {"name": "f2", "type": "int", "default": "345"} ] }

#
# 1 column -> 2 columns with no default
#

$ kafka-create-topic topic=decode-1to2-nodefault

$ kafka-ingest format=avro topic=decode-1to2-nodefault schema=${1column} timestamp=1
{"f1": 123}

> CREATE SOURCE decode_1to2_nodefault
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-decode-1to2-nodefault-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${2columns-nodefault}'
  ENVELOPE NONE

! SELECT * FROM decode_1to2_nodefault
contains:avro deserialization error: unable to decode row : IO error: UnexpectedEof

#
# 1 column -> 2 columns with default
#

$ kafka-create-topic topic=decode-1to2-default

$ kafka-ingest format=avro topic=decode-1to2-default schema=${1column} timestamp=1
{"f1": 123}

> CREATE SOURCE decode_1to2_default
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-decode-1to2-default-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${2columns-default}'
  ENVELOPE NONE

! SELECT * FROM decode_1to2_default
contains:Text: avro deserialization error: unable to decode row : IO error: UnexpectedEof

#
# 2 columns -> 1 column
#

$ kafka-create-topic topic=decode-2to1

$ kafka-ingest format=avro topic=decode-2to1 schema=${2columns-nodefault} timestamp=1
{"f1": 123, "f2": 234}
{"f2": 345, "f1": 456}

> CREATE SOURCE decode_2to1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-decode-2to1-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${1column}'
  ENVELOPE NONE

! SELECT * FROM decode_2to1
contains:Unexpected bytes remaining
