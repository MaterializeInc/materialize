# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for https://github.com/MaterializeInc/materialize/issues/6305

$ set int={"type": "record", "name": "field_int", "fields": [ {"name": "f1", "type": "int"} ] }

$ kafka-create-topic topic=tail-fetch-count-zero

> CREATE MATERIALIZED SOURCE fetch_count_zero
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-tail-fetch-count-zero-${testdrive.seed}'
  WITH( timestamp_frequency_ms = 100 )
  FORMAT AVRO USING SCHEMA '${int}'
  ENVELOPE NONE

> BEGIN

> DECLARE c CURSOR FOR TAIL fetch_count_zero AS OF NOW();

> FETCH 0 c WITH (timeout='1h');
