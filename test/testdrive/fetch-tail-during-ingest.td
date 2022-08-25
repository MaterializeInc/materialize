# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that data that was ingested during the lifetime of a TAIL cursor can be FETCH-ed
#

$set-regex match=\d{13} replacement=<TIMESTAMP>

$ set int={"type": "record", "name": "field_int", "fields": [ {"name": "f1", "type": "int"} ] }

$ kafka-create-topic topic=tail-fetch-during-ingest

> CREATE SOURCE fetch_during_ingest
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-tail-fetch-during-ingest-${testdrive.seed}'
  LEGACYWITH( timestamp_frequency_ms = 100 )
  FORMAT AVRO USING SCHEMA '${int}'
  ENVELOPE NONE

> BEGIN

> DECLARE c CURSOR FOR TAIL fetch_during_ingest;

$ kafka-ingest format=avro topic=tail-fetch-during-ingest schema=${int} timestamp=1
{"f1": 123}

> FETCH 1 c WITH (timeout='60s');
<TIMESTAMP> 1 123

$ kafka-ingest format=avro topic=tail-fetch-during-ingest schema=${int} timestamp=2
{"f1": 234}

# The row just inserted is ours to fetch
> FETCH 1 c WITH (timeout='60s');
<TIMESTAMP> 1 234
