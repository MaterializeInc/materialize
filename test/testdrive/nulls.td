# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set foo-schema={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {"name": "a", "type": ["null", "long"]}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-ingest format=avro topic=foo schema=${foo-schema} timestamp=1
{"before": null, "after": {"a": 1}}
{"before": null, "after": {"a": 2}}
{"before": null, "after": {"a": null}}

$ kafka-ingest format=avro topic=foo schema=${foo-schema} timestamp=2
{"before": null, "after": null}

> CREATE SOURCE foo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-foo-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${foo-schema}' ENVELOPE DEBEZIUM

> CREATE MATERIALIZED VIEW test1 AS
  SELECT * FROM foo JOIN foo as foo2 USING (a);

> SELECT * FROM test1;
a
---
1
2
