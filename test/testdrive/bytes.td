# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=scale=1,workers=1

# Test ingestion of and selection from a simple bytes-formatted topic.

$ kafka-create-topic topic=bytes partitions=1

$ kafka-ingest format=bytes topic=bytes timestamp=1
©1
©2

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CLUSTER data_cluster SIZE '${arg.default-storage-size}';
> BEGIN
> CREATE SOURCE data
  IN CLUSTER data_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-bytes-${testdrive.seed}')

> CREATE TABLE data_tbl FROM SOURCE data (REFERENCE "testdrive-bytes-${testdrive.seed}")
  FORMAT BYTES
  INCLUDE OFFSET
> COMMIT

> SHOW COLUMNS FROM data_tbl
name       nullable  type    comment
------------------------------------
data       false     bytea   ""
offset     false     uint8   ""

> SELECT * FROM data_tbl
data           offset
------------------------
"\\xc2\\xa91"  0
"\\xc2\\xa92"  1

# Test that CREATE SOURCE can specify a custom name for the column.

> CREATE CLUSTER data_named_col_cluster SIZE '${arg.default-storage-size}';
> BEGIN
> CREATE SOURCE data_named_col
  IN CLUSTER data_named_col_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-bytes-${testdrive.seed}')

> CREATE TABLE data_named_col_tbl (named_col) FROM SOURCE data_named_col (REFERENCE "testdrive-bytes-${testdrive.seed}")
  FORMAT BYTES
> COMMIT

> SHOW COLUMNS FROM data_named_col_tbl
name       nullable  type   comment
-----------------------------------
named_col  false     bytea  ""

> CREATE CLUSTER data_offset_cluster SIZE '${arg.default-storage-size}';
> BEGIN
> CREATE SOURCE data_offset
  IN CLUSTER data_offset_cluster
  FROM KAFKA CONNECTION kafka_conn (START OFFSET=[1], TOPIC 'testdrive-bytes-${testdrive.seed}')

> CREATE TABLE data_offset_tbl FROM SOURCE data_offset (REFERENCE "testdrive-bytes-${testdrive.seed}")
  FORMAT BYTES
  INCLUDE OFFSET
> COMMIT

> SELECT * FROM data_offset_tbl
data           offset
------------------------
"\\xc2\\xa92"  1

$ kafka-create-topic topic=bytes-partitions partitions=2

$ kafka-ingest format=bytes topic=bytes-partitions timestamp=1 partition=0
©1

$ kafka-ingest format=bytes topic=bytes-partitions timestamp=1 partition=1
©2

> CREATE CLUSTER data_offset_2_cluster SIZE '${arg.default-storage-size}';
> BEGIN
> CREATE SOURCE data_offset_2
  IN CLUSTER data_offset_2_cluster
  FROM KAFKA CONNECTION kafka_conn (START OFFSET=[0,1], TOPIC 'testdrive-bytes-partitions-${testdrive.seed}')

> CREATE TABLE data_offset_2_tbl FROM SOURCE data_offset_2 (REFERENCE "testdrive-bytes-partitions-${testdrive.seed}")
  FORMAT BYTES
  INCLUDE OFFSET
> COMMIT

> SELECT * FROM data_offset_2_tbl
data           offset
------------------------
"\\xc2\\xa91"  0
