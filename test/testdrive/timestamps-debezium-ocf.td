# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema={
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
              {"name": "a", "type": "long"},
              {"name": "b", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ set trxschemakey={
      "name": "io.debezium.connector.common.TransactionMetadataKey",
      "type": "record",
      "fields": [
          {
              "name": "id",
              "type": "string"
          }
      ]
  }


$ set trxschema={
      "name": "io.debezium.connector.common.TransactionMetadataValue",
      "type": "record",
      "fields": [
        {
              "name": "id",
              "type": "string"
         },
         {
              "name": "status",
              "type": "string"
         },
          {
              "name": "event_count",
              "type": [
                  "long",
                  "null"
              ]
          },
          {
              "name": "data_collections",
              "type": [
                  {
                      "type": "array",
                      "items": {
                          "name": "data",
                          "type": "record",
                          "fields": [
                              {
                                  "name": "event_count",
                                  "type": "long"
                              },
                              {
                                  "name": "data_collection",
                                  "type": "string"
                              }
                          ]
                      }
                  },
                  "null"
              ]
          }
      ]
  }

$ avro-ocf-write path=consistency.ocf schema=${trxschema}
{"status":"BEGIN","id":"1","event_count":null,"data_collections":null}

$ avro-ocf-write path=foo.ocf schema=${schema}
{"before": null, "after": {"a": 1, "b": 1}}
{"before": null, "after": {"a": 2, "b": 2}}

$ avro-ocf-write path=bar.ocf schema=${schema}
{"before": null, "after": {"a": 10, "b": 1}}

> CREATE MATERIALIZED SOURCE data_foo FROM AVRO OCF '${testdrive.temp-dir}/foo.ocf' WITH (consistency='${testdrive.temp-dir}/consistency.ocf', tail=true) ENVELOPE DEBEZIUM

> CREATE MATERIALIZED SOURCE data_bar FROM AVRO OCF '${testdrive.temp-dir}/bar.ocf' WITH (consistency='${testdrive.temp-dir}/consistency.ocf', tail=true) ENVELOPE DEBEZIUM

> CREATE MATERIALIZED VIEW foo AS SELECT b, sum(a) FROM data_foo GROUP BY b

> CREATE MATERIALIZED VIEW bar AS SELECT b, sum(a) FROM data_bar GROUP BY b

> CREATE MATERIALIZED VIEW join AS SELECT * FROM foo JOIN bar USING (b);

! SELECT * FROM bar;
At least one input has no complete timestamps yet:

! SELECT * FROM foo;
At least one input has no complete timestamps yet:

! SELECT * FROM join ;
At least one input has no complete timestamps yet:

$ avro-ocf-append path=consistency.ocf
{"status":"BEGIN","id":"1","event_count":null,"data_collections":null}
{"status":"END","id":"1","event_count":0,"data_collections":[]}
{"status":"BEGIN","id":"2","event_count":null,"data_collections":null}
{"status":"END","id":"2","event_count":2,"data_collections":[{"event_count": 2, "data_collection": "${testdrive.temp-dir}/foo.ocf"},{"event_count": 1, "data_collection": "${testdrive.temp-dir}/bar.ocf"}]}

> SELECT * FROM foo;
b  sum
------
1  1
2  2

> SELECT * FROM bar;
b  sum
------
1  10

> SELECT * FROM join;
a  a  b
------
1  1  10

$ avro-ocf-append path=foo.ocf
{"before": null, "after": {"a": 3, "b": 3}}

$ avro-ocf-append path=bar.ocf
{"before": null, "after": {"a": 30, "b": 3}}

$ avro-ocf-append path=consistency.ocf
{"status":"BEGIN","id":"3","event_count":null,"data_collections":null}
{"status":"END","id":"3","event_count":2,"data_collections":[{"event_count": 2, "data_collection": "${testdrive.temp-dir}/foo.ocf"},{"event_count": 1, "data_collection": "${testdrive.temp-dir}/bar.ocf"}]}

> SELECT * FROM foo;
b  sum
------
1  1
2  2

> SELECT * FROM bar;
b  sum
------
1  10
3  30

> SELECT * FROM join;
a  a  b
------
1  1  10

$ avro-ocf-append path=foo.ocf
{"before": null, "after": {"a": 4, "b": 4}}

> SELECT * FROM foo;
b  sum
------
1  1
2  2
3  3
4  4

> SELECT * FROM join;
a  a  b
------
1  1  10
3  3  30
