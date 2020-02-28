# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Test basic create and drop functionality

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
              {"name": "X", "type": "long"},
              {"name": "Y", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

> CREATE SOURCE s
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=33
{"before": null, "after": {"X": 1, "Y": 1}}
{"before": null, "after": {"X": 2, "Y": 1}}
{"before": null, "after": {"X": 3, "Y": 1}}
{"before": null, "after": {"X": 1, "Y": 2}}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=34
{"before": null, "after": null}

> CREATE MATERIALIZED VIEW v AS SELECT 42 AS a

> CREATE MATERIALIZED VIEW v2 AS SELECT 0 AS x

> CREATE INDEX i ON v(a)

> CREATE INDEX j on s("X");

# Test that creating objects of the same name does not work

! CREATE MATERIALIZED VIEW i AS SELECT 1.5 AS c
catalog item 'i' already exists

! CREATE INDEX i ON s("Y")
catalog item 'i' already exists

! CREATE INDEX j on v2(x)
catalog item 'j' already exists

! CREATE INDEX v ON v2(x)
catalog item 'v' already exists

$ set dummy={
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
              {"name": "X",
               "type": {
                  "type": "bytes",
                  "scale": 3,
                  "precision": 10,
                  "logicalType": "decimal"
                }}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

! CREATE SOURCE v2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${dummy}'
  ENVELOPE DEBEZIUM
catalog item 'v2' already exists

! CREATE SOURCE i
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${dummy}'
  ENVELOPE DEBEZIUM
catalog item 'i' already exists

! CREATE INDEX s ON v2(x)
catalog item 's' already exists

! CREATE MATERIALIZED VIEW s AS SELECT 'bloop' AS d
catalog item 's' already exists

# Test that objects do not get dropped if the drop command does not specify the correct type
! DROP SOURCE v
v is not of type SOURCE

! DROP SOURCE IF EXISTS v
v is not of type SOURCE

! DROP INDEX v
v is not of type INDEX

! DROP INDEX IF EXISTS v
v is not of type INDEX

! DROP SOURCE i
i is not of type SOURCE

! DROP SOURCE IF EXISTS i
i is not of type SOURCE

! DROP VIEW i
i is not of type VIEW

! DROP VIEW IF EXISTS i
i is not of type VIEW

! DROP INDEX s
s is not of type INDEX

! DROP INDEX IF EXISTS s
s is not of type INDEX

! DROP VIEW s
s is not of type VIEW

! DROP VIEW IF EXISTS s
s is not of type VIEW

# Delete objects

> DROP INDEX j

> DROP INDEX i

> DROP VIEW v

> DROP SOURCE s

# test that the names of the deleted objects can be used to create objects of other types
> CREATE MATERIALIZED VIEW s AS SELECT 84 AS b

> CREATE SOURCE i
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE DEBEZIUM

> CREATE INDEX v ON s(b)

# Test that if exists works when the referred objects do not exist

> DROP INDEX IF EXISTS v

> DROP VIEW IF EXISTS s

> DROP SOURCE IF EXISTS i

# Test that if exists works when the referred objects do not exist

> DROP INDEX IF EXISTS nonexistent

> DROP VIEW IF EXISTS nonexistent

> DROP SOURCE IF EXISTS nonexistent

# Test that drop without if exists does not work if the object does not exist

! DROP INDEX nonexistent
unknown catalog item 'nonexistent'

! DROP VIEW nonexistent
unknown catalog item 'nonexistent'

! DROP SOURCE nonexistent
unknown catalog item 'nonexistent'

# Test CREATE VIEW IF NOT EXISTS
> CREATE MATERIALIZED VIEW IF NOT EXISTS test1 AS SELECT 42 AS a

> SHOW CREATE VIEW test1
View                      Create View
----------------------------------------------------------------------------------------------------------------------------------------------------------------
materialize.public.test1  "CREATE VIEW \"materialize\".\"public\".\"test1\" AS SELECT 42 AS \"a\""

! CREATE MATERIALIZED VIEW test1 AS SELECT 43 AS b
catalog item 'test1' already exists

> SELECT * FROM test1
a
-----------
42

> CREATE MATERIALIZED VIEW IF NOT EXISTS test1 AS SELECT 42 AS a

> DROP VIEW test1

# todo@jldlaughlin: Add a MATERIALIZED VIEW on top of this
# source and drop it. Depends on #2103.
> CREATE SOURCE f
  FROM KINESIS ARN 'fake_arn'
  WITH (access_key = 'fake_access_key', secret_access_key = 'fake_secret_access_key', region = 'us-east-2')
  FORMAT BYTES;

> DROP SOURCE f
