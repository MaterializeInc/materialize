# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test ingestion of and selection from a simple bytes-formatted topic.

$ kafka-create-topic topic=bytes partitions=1

$ kafka-ingest format=bytes topic=bytes timestamp=1
©1
©2

> CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-bytes-${testdrive.seed}'
  FORMAT BYTES
  INCLUDE OFFSET

> SHOW COLUMNS FROM data
name       nullable  type
--------------------------
data       false     bytea
offset     false     bigint

> SELECT * FROM data
data           offset
------------------------
"\\xc2\\xa91"  1
"\\xc2\\xa92"  2

# Test that CREATE SOURCE can specify a custom name for the column.

> CREATE MATERIALIZED SOURCE data_named_col (named_col)
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-bytes-${testdrive.seed}'
  FORMAT BYTES

> SHOW COLUMNS FROM data_named_col
name       nullable  type
--------------------------
named_col  false     bytea

> CREATE MATERIALIZED SOURCE data_offset
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-bytes-${testdrive.seed}'
  WITH (start_offset=1)
  FORMAT BYTES
  INCLUDE OFFSET

> SELECT * FROM data_offset
data           offset
------------------------
"\\xc2\\xa92"  2

$ kafka-create-topic topic=bytes-partitions partitions=2

$ kafka-ingest format=bytes topic=bytes-partitions timestamp=1 partition=0
©1

$ kafka-ingest format=bytes topic=bytes-partitions timestamp=1 partition=1
©2

> CREATE MATERIALIZED SOURCE data_offset_2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-bytes-partitions-${testdrive.seed}'
  WITH (start_offset=[0,1])
  FORMAT BYTES
  INCLUDE OFFSET

> SELECT * FROM data_offset_2
data           offset
------------------------
"\\xc2\\xa91"  1
