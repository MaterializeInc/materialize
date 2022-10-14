# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema={
    "type": "record",
    "name": "cpx",
    "fields": [
      {"name": "a", "type": "long"},
      {"name": "b", "type": "long"}
    ]
  }

> SELECT * FROM kafka_source;

a b
---
1 2
2 3

$ kafka-ingest format=avro topic=upgrade-kafka-source-${arg.upgrade-from-version} schema=${schema} timestamp=1
{"a": 10, "b": 20}
{"a": 20, "b": 30}

> SELECT * FROM kafka_source;

a b
---
1 2
2 3
10 20
20 30

> DROP SOURCE kafka_source;
