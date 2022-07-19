# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that the SNAPSHOT=FALSE option is observed
#

$ set-regex match=\d{13} replacement=<TIMESTAMP>
$ set int={"type": "record", "name": "field_int", "fields": [ {"name": "f1", "type": "int"} ] }

$ kafka-create-topic topic=tail-without-snapshot

$ kafka-ingest format=avro topic=tail-without-snapshot schema=${int} timestamp=1
{"f1": 123}
{"f1": 234}
{"f1": 345}

> CREATE SOURCE tail_without_snapshot
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-tail-without-snapshot-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${int}'
  ENVELOPE NONE

> SELECT * FROM tail_without_snapshot;
123
234
345

> BEGIN

> DECLARE c CURSOR FOR TAIL tail_without_snapshot WITH (SNAPSHOT = FALSE);

> FETCH 1 FROM c WITH (timeout = '2s')

$ kafka-ingest format=avro topic=tail-without-snapshot schema=${int} timestamp=1
{"f1": 567}

> FETCH 1 FROM c WITH (timeout = '60s')
<TIMESTAMP> 1 567
