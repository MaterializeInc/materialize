# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that attempting to decode something that is not a record results in a proper error
#

$ set no-record={"type": "map", "values" : "int" }

$ kafka-create-topic topic=avro-decode-no-record

$ kafka-ingest format=avro topic=avro-decode-no-record schema=${no-record} timestamp=1
{"f1":123}

! CREATE MATERIALIZED SOURCE avro_decode_no_record
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-avro-decode-no-record-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${no-record}'
  ENVELOPE NONE
validating avro value schema: row schemas must be records, got: Map(Piece(Int))
