# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=bytes

$ kafka-ingest format=bytes topic=bytes timestamp=1
©1
©2

> CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-bytes-${testdrive.seed}'
  FORMAT BYTES

> SHOW COLUMNS FROM data
Field      Nullable  Type
--------------------------
data       NO        bytea
mz_offset  NO        int8

> SELECT * FROM data
data           mz_offset
----
"\\xc2\\xa91"  1
"\\xc2\\xa92"  2

> select convert_from(data, 'utf8') FROM data
©1
©2

> CREATE MATERIALIZED SOURCE data_named_col (named_col)
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-bytes-${testdrive.seed}'
  FORMAT BYTES

> SHOW COLUMNS FROM data_named_col
Field      Nullable  Type
--------------------------
named_col       NO        bytea
mz_offset  NO        int8
