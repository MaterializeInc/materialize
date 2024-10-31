# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=1

$ kafka-create-topic topic=append_only partitions=1

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CLUSTER ct_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE append_only
  IN CLUSTER ct_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-append_only-${testdrive.seed}')

> CREATE TABLE append_only_tbl FROM SOURCE append_only (REFERENCE "testdrive-append_only-${testdrive.seed}")
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  INCLUDE KEY
  ENVELOPE NONE

$ kafka-ingest topic=append_only format=bytes key-format=bytes key-terminator=:
x:0

# Wait for the ingestion to have completed before creating the CT, we want the x:0
# value to be in the continual task, since it only starts after it is ingested already.
> SELECT * FROM append_only_tbl
x 0

> CREATE CONTINUAL TASK ct_max FROM TRANSFORM append_only_tbl USING
    (SELECT max(text::int) FROM append_only_tbl)

$ kafka-ingest topic=append_only format=bytes key-format=bytes key-terminator=:
a:1

> SELECT * FROM ct_max
0
1

$ kafka-ingest topic=append_only format=bytes key-format=bytes key-terminator=:
b:2

> SELECT * FROM ct_max
0
1
2

$ kafka-ingest topic=append_only format=bytes key-format=bytes key-terminator=:
c:3

> SELECT * FROM ct_max
0
1
2
3

$ kafka-ingest topic=append_only format=bytes key-format=bytes key-terminator=:
d:1

> SELECT * FROM ct_max
0
1
2
3
