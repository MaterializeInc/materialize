# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

$ set foo-schema={
    "type": "record",
    "name": "na",
    "fields": [
        {"name": "a", "type": ["null", "long"]}
    ]
  }

$ kafka-ingest format=avro topic=foo schema=${foo-schema}
{"a": 1}
{"a": 2}
{"a": null}

> CREATE SOURCE foo FROM 'kafka://${testdrive.kafka-addr}/foo' USING SCHEMA '${foo-schema}'

> CREATE MATERIALIZED VIEW test1 AS
  SELECT * FROM foo JOIN foo as foo2 USING (a);

> PEEK test1;
a
---
1
2
