# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Check that we get a meaningful error when JSON fails to decode.
#

$ set writer={"type": "record", "name": "value", "fields": [ { "name": "f1", "type": "string" } ] }
$ set reader={"type": "record", "name": "value", "fields": [ { "name": "f1", "type": {"type": "string", "connect.name": "io.debezium.data.Json" } } ] }

$ kafka-create-topic topic=avro-bad-json

$ kafka-ingest format=avro topic=avro-bad-json schema=${writer} timestamp=1
{"f1": "__debezium_unavailable_value"}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SOURCE avro_bad_json
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-avro-bad-json-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${reader}'
  ENVELOPE NONE

! SELECT * FROM avro_bad_json
contains: (got __debezium_unavailable_value)
