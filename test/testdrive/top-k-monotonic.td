# Copyright Materialize, Inc. and contributors. All rights reserved.
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
{"a": 1, "b": 1}
{"a": 1, "b": 2}
{"a": 1, "b": 3}
{"a": 1, "b": 4}
{"a": 1, "b": 5}
{"a": 2, "b": 1000}
{"a": 2, "b": 1001}
{"a": 2, "b": 1002}
{"a": 2, "b": 1003}
{"a": 2, "b": 1004}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SOURCE non_dbz_data
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-non-dbz-data-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${non-dbz-schema}'
  ENVELOPE NONE

# Create a monotonic topk plan that has both a limit and a group to test that thinning works as expected
> SELECT * FROM (SELECT DISTINCT a FROM non_dbz_data) grp, LATERAL (SELECT b FROM non_dbz_data WHERE a = grp.a ORDER BY b LIMIT 2);
a b
---------
1 1
1 2
2 1000
2 1001
