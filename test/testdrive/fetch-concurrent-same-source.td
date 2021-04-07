# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that FETCH-ing using multiple cursors from the same source works as expected
#

$ set-regex match=\d{13} replacement=<TIMESTAMP>

$ set int={"type": "record", "name": "field_int", "fields": [ {"name": "f1", "type": "int"} ] }

$ kafka-create-topic topic=fetch-concurrent-same-source

> CREATE MATERIALIZED SOURCE fetch_concurrent_same_source
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-fetch-concurrent-same-source-${testdrive.seed}'
  WITH( timestamp_frequency_ms = 10 )
  FORMAT AVRO USING SCHEMA '${int}'
  ENVELOPE NONE

$ kafka-ingest format=avro topic=fetch-concurrent-same-source schema=${int} timestamp=1
{"f1": 123}
{"f1": 234}
{"f1": 345}

> SELECT COUNT(*) = 3 FROM fetch_concurrent_same_source;
true

> BEGIN

> DECLARE c1 CURSOR FOR SELECT * FROM fetch_concurrent_same_source;

> DECLARE c2 CURSOR FOR SELECT * FROM fetch_concurrent_same_source;

> FETCH ALL c1;
123
234
345

> FETCH ALL c2;
123
234
345

> DECLARE c3 CURSOR FOR TAIL fetch_concurrent_same_source;
> FETCH 3 c3;
<TIMESTAMP> 1 123
<TIMESTAMP> 1 234
<TIMESTAMP> 1 345

> DECLARE c4 CURSOR FOR SELECT * FROM fetch_concurrent_same_source;

> FETCH ALL c4;
123
234
345

> DECLARE c5 CURSOR FOR TAIL fetch_concurrent_same_source;
> FETCH 3 c5;
<TIMESTAMP> 1 123
<TIMESTAMP> 1 234
<TIMESTAMP> 1 345
