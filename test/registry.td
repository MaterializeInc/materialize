# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

# Verify the error message is useful when a schema is not present in the
# registry.

! CREATE SOURCE noexist FROM 'kafka://${testdrive.kafka-addr}/testdrive-noexist-${testdrive.seed}'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}'
fetching latest schema for subject 'testdrive-noexist-${testdrive.seed}-value' from registry: subject not found

$ set schema_v1={
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
              {"name": "a", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ set schema_v2={
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
              {"name": "b", "type": "long", "default": 42}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-ingest format=avro topic=data schema=${schema_v1} publish=true timestamp=1
{"before": null, "after": {"a": 1}}

$ kafka-ingest format=avro topic=data schema=${schema_v1} publish=true timestamp=2
{"before": null, "after": null}

> CREATE SOURCE data_v1 FROM 'kafka://${testdrive.kafka-addr}/testdrive-data-${testdrive.seed}'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE VIEW data_v1_view as SELECT * from data_v1

> SELECT * FROM data_v1_view
a
---
1

$ kafka-ingest format=avro topic=data schema=${schema_v2} publish=true timestamp=3
{"before": null, "after": {"a": 2, "b": -1}}

$ kafka-ingest format=avro topic=data schema=${schema_v2} publish=true timestamp=4
{"before": null, "after": null}

> CREATE SOURCE data_v2 FROM 'kafka://${testdrive.kafka-addr}/testdrive-data-${testdrive.seed}'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE VIEW data_v2_view as SELECT * from data_v2

> SELECT * FROM data_v1_view
a
---
1
2

> SELECT * FROM data_v2_view
a b
----
1 42
2 -1

$ kafka-ingest format=avro topic=db.my_topic schema=${schema_v2} publish=true timestamp=1
{"before": null, "after": null}

# Hyphens and underscores in this topic should automatically be converted to
# underscores so that the topic is easier to name from SQL.
$ kafka-ingest format=avro topic=db.sp_ec-ia.lc-ch._ars schema=${schema_v2} publish=true timestamp=1
{"before": null, "after": null}

> CREATE SOURCES LIKE 'testdrive-db.%' FROM 'kafka://${testdrive.kafka-addr}/'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}'
Topic
---
materialize.public.testdrive_db_my_topic_${testdrive.seed}
materialize.public.testdrive_db_sp_ec_ia_lc_ch__ars_${testdrive.seed}

# Verify that the sources created by CREATE SOURCES are usable from SQL without
# quoting their names.
> CREATE VIEW create_sources as SELECT * FROM testdrive_db_my_topic_${testdrive.seed}
  UNION
  SELECT * FROM testdrive_db_sp_ec_ia_lc_ch__ars_${testdrive.seed}

> SELECT * FROM create_sources

> CREATE SOURCES LIKE 'testdrive%' FROM 'kafka://${testdrive.kafka-addr}/'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}'
Topic
---
materialize.public.testdrive_data_${testdrive.seed}

> CREATE SOURCES LIKE 'testdrive%' FROM 'kafka://${testdrive.kafka-addr}/'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}'
Topic
---


$ set key_schema_missing={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "b", "type": "long"}
    ]
  }

$ set key_schema_wrong_type={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "a", "type": "int"}
    ]
  }

$ set valid_key_schema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "a", "type": "long"}
    ]
  }

$ kafka-ingest format=avro topic=missing-data schema=${schema_v1} key_schema=${key_schema_missing}
  publish=true timestamp=1
{"before": null, "after": {"a": 1}}

$ kafka-ingest format=avro topic=missing-data schema=${schema_v1} key_schema=${key_schema_missing}
  publish=true timestamp=2
{"before": null, "after": null}

! CREATE SOURCE data_v3 FROM 'kafka://${testdrive.kafka-addr}/testdrive-missing-data-${testdrive.seed}'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}';
Value schema missing primary key column: b

$ kafka-ingest format=avro topic=mismatched-data schema=${schema_v1} key_schema=${key_schema_wrong_type}
  publish=true timestamp=3
{"before": null, "after": {"a": 1}}

$ kafka-ingest format=avro topic=mismatched-data schema=${schema_v1} key_schema=${key_schema_wrong_type}
  publish=true timestamp=4
{"before": null, "after": null}

! CREATE SOURCE data_v3 FROM 'kafka://${testdrive.kafka-addr}/testdrive-mismatched-data-${testdrive.seed}'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}';
key and value column types do not match

$ kafka-ingest format=avro topic=data schema=${schema_v1} key_schema=${valid_key_schema}
  publish=true timestamp=5
{"before": null, "after": {"a": 1}}

$ kafka-ingest format=avro topic=data schema=${schema_v1} key_schema=${valid_key_schema}
  publish=true timestamp=6
{"before": null, "after": null}

> CREATE SOURCE data_v3 FROM 'kafka://${testdrive.kafka-addr}/testdrive-data-${testdrive.seed}'
  USING SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE VIEW data_v3_view as SELECT * from data_v3

> SELECT * FROM data_v3_view
a
---
1 42
1 42
2 -1
