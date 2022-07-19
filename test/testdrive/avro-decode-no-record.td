# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Decoding a schema that is not a record results in a relation with a single column
#

$ set no-record
"int"

$ kafka-create-topic topic=avro-decode-no-record

$ kafka-ingest format=avro topic=avro-decode-no-record schema=${no-record} timestamp=1
123

> CREATE SOURCE avro_decode_no_record
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avro-decode-no-record-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${no-record}'
  ENVELOPE NONE

> SHOW COLUMNS FROM avro_decode_no_record
name       nullable  type
----------------------------
\?column?  false     integer
