# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test monotonicity analyses which derive from ENVELOPE NONE sources.
# Note that these only test the implementation for monotonic sources,
# they do not test that the analysis doesn't have false positives on
# non-monotonic sources.

$ set non-dbz-schema={
    "type": "record",
    "name": "cpx",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "long"}
    ]
  }

$ kafka-create-topic topic=non-dbz-data

$ kafka-ingest format=avro topic=non-dbz-data schema=${non-dbz-schema} timestamp=1
{"a": 1, "b": 2}
{"a": 1, "b": 1048576}
{"a": 2, "b": 3}
{"a": 2, "b": 4}

> CREATE SOURCE non_dbz_data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-non-dbz-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  ENVELOPE NONE

> CREATE MATERIALIZED VIEW monotonic_min AS SELECT a, min(b) FROM non_dbz_data group by a

> SELECT * FROM monotonic_min
a min
---
1 2
2 3

> CREATE MATERIALIZED VIEW monotonic_max AS SELECT a, max(b) FROM non_dbz_data group by a

> SELECT * FROM monotonic_max
a max
---
1 1048576
2 4

# Smoke test for aggregate fusion
> CREATE MATERIALIZED VIEW monotonic_fused AS SELECT a, min(b), max(b + 1)  FROM non_dbz_data group by a

> SELECT * FROM monotonic_fused
a min max
---
1 2 1048577
2 3 5
