# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Create sources and verify they can ingest data while `environmentd` is online.

$ kafka-create-topic topic=data
$ kafka-ingest format=bytes topic=data
one

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SOURCE data
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  FORMAT TEXT;

> SELECT * from data
one

$ set-regex match=\d+ replacement=<NUMBER>

# Ensure that we can select from automatically generated remap collection
> SELECT * FROM data_progress
<NUMBER> <NUMBER>

# Ensure we report the write frontier of the progress subsource
$ set-regex match=(\d{13,20}|u\d{1,5}|\(\d+-\d\d-\d\d\s\d\d:\d\d:\d\d.\d\d\d\)|true|false) replacement=<>
> EXPLAIN TIMESTAMP FOR SELECT * FROM data_progress
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n\nsource materialize.public.data_progress (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n"

> CREATE SOURCE d
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  FORMAT TEXT
  EXPOSE PROGRESS AS exposed_progress_data;

> SELECT * FROM exposed_progress_data
<NUMBER> <NUMBER>
