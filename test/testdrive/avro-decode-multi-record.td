# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Multi-record schemas are rejected
#

$ set multi-record=[{"type": "record", "name": "f1", "fields": [ {"name": "f1", "type": "int"} ] }, {"type": "record", "name": "f2", "fields": [ {"name": "f2", "type": "int"} ] } ]

$ kafka-create-topic topic=avro-decode-type-multi-record

$ kafka-ingest format=avro topic=avro-decode-type-multi-record schema=${multi-record} timestamp=1
{"f1": {"f1":123}}

! CREATE MATERIALIZED SOURCE avro_decode_type_multi_record
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avro-decode-type-multi-record-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${multi-record}'
  ENVELOPE NONE
row schemas must be records, got: Union
