# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

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

> CREATE SOURCE foo FROM 'kafka://${testdrive.kafka-addr}/testdrive-foo-${testdrive.seed}'
  USING SCHEMA '${foo-schema}'

> CREATE MATERIALIZED VIEW test1 AS
  SELECT * FROM foo JOIN foo as foo2 USING (a);

> SELECT * FROM test1;
a
---
1
2
