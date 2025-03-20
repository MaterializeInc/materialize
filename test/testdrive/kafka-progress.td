# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

# Create sources and verify they can ingest data while `environmentd` is online.

$ kafka-create-topic topic=data partitions=1
$ kafka-ingest format=bytes topic=data
one

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE data
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}');

> CREATE TABLE data_tbl FROM SOURCE data (REFERENCE "testdrive-data-${testdrive.seed}")
  FORMAT TEXT;

> SELECT * from data_tbl
one

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'data_progress';
running

# Ensure that we can select from automatically generated remap collection
> SELECT partition::text, "offset" FROM data_progress
[0,0] 1
(0,) 0

# Ensure we report the write frontier of the progress subsource
$ set-regex match=(\s{12}0|\d{13,20}|u\d{1,5}|\(\d+-\d\d-\d\d\s\d\d:\d\d:\d\d\.\d\d\d\)|true|false) replacement=<>
> EXPLAIN TIMESTAMP FOR SELECT * FROM data_progress
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n              session wall time: <> <>\n\nsource materialize.public.data_progress (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n"

> CREATE SOURCE d
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  EXPOSE PROGRESS AS exposed_progress_data;

> CREATE TABLE d_tbl FROM SOURCE d (REFERENCE "testdrive-data-${testdrive.seed}")
  FORMAT TEXT;

> SELECT partition::text, "offset" FROM exposed_progress_data
[0,0] 1
(0,) 0
