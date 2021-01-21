# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test ingestion of and selection from a simple bytes-formatted topic.

$ kafka-create-topic topic=bytes

$ kafka-ingest format=bytes topic=bytes timestamp=1
©1
©2

> CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-bytes-${testdrive.seed}'
  WITH (cache=true) FORMAT BYTES

> SELECT "offset", value FROM internal_read_cached_data('data') ORDER BY "offset"
 offset  value
--------------------
1  "\\xc2\\xa91"
2  "\\xc2\\xa92"

> SELECT DISTINCT split_part(filename, '-', 4) AS partition,
  split_part(filename, '-', 5) AS start_offset,
  split_part(filename, '-', 6) AS end_offset
  FROM internal_read_cached_data('data')
partition  start_offset  end_offset
--------------------------------------
0          0            2
