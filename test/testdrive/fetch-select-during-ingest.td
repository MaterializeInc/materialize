# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that data that was ingested during the lifetime of a SELECT cursor is *NOT* FETCH-ed
#

$ set-regex match=\d{13} replacement=<TIMESTAMP>

$ set int={"type": "record", "name": "field_int", "fields": [ {"name": "f1", "type": "int"} ] }

$ kafka-create-topic topic=tail-fetch-during-ingest

> CREATE SOURCE fetch_during_ingest
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-tail-fetch-during-ingest-${testdrive.seed}'
  WITH( timestamp_frequency_ms = 100 )
  FORMAT AVRO USING SCHEMA '${int}'
  ENVELOPE NONE

$ kafka-ingest format=avro topic=tail-fetch-during-ingest schema=${int} timestamp=1
{"f1": 123}

> SELECT * FROM fetch_during_ingest;
123

> BEGIN

> DECLARE c CURSOR FOR SELECT * FROM fetch_during_ingest;

> FETCH 1 c WITH (timeout='60s');
123

$ kafka-ingest format=avro topic=tail-fetch-during-ingest schema=${int} timestamp=2
{"f1": 234}

# Sleep here to make sure the entire machinery has run. Since we are in a transaction,
# we have no way of knowing that the source has progressed to '234' outside of the transaction

# NOTE(benesch): grumble. This is not a particularly robust way to write this
# test. It is, however, better than what was previously here, which used
# `SELECT mz_internal.mz_sleep(2)`, which had the extremely suboptimal property
# of wedging up the coordinator for 2s, instead of just pausing the test for 2s.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=2s

# This will return an empty result - nothing else available for fetching in the current transaction
> FETCH 1 c WITH (timeout='2s');

> COMMIT;

#
# The '234' row can now be fetched
#

> BEGIN

> DECLARE c CURSOR FOR SELECT * FROM fetch_during_ingest;

> FETCH 2 c WITH (timeout='60s');
123
234
