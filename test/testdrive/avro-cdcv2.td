# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-replica-size=scale=1,workers=1
$ set-arg-default single-replica-cluster=quickstart

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_envelope_materialize = true

# Test support for Avro sources without using the Confluent Schema Registry.

$ set schema=[
  {
    "type": "array",
    "items": {
      "type": "record",
      "name": "update",
      "namespace": "com.materialize.cdc",
      "fields": [
        {
          "name": "data",
          "type": {
            "type": "record",
            "name": "data",
            "fields": [
              {
                "name": "id",
                "type": "long"
              },
              {
                "name": "price",
                "type": [
                  "null",
                  "int"
                ]
              }
            ]
          }
        },
        {
          "name": "time",
          "type": "long"
        },
        {
          "name": "diff",
          "type": "long"
        }
      ]
    }
  },
  {
    "type": "record",
    "name": "progress",
    "namespace": "com.materialize.cdc",
    "fields": [
      {
        "name": "lower",
        "type": {
          "type": "array",
          "items": "long"
        }
      },
      {
        "name": "upper",
        "type": {
          "type": "array",
          "items": "long"
        }
      },
      {
        "name": "counts",
        "type": {
          "type": "array",
          "items": {
            "type": "record",
            "name": "counts",
            "fields": [
              {
                "name": "time",
                "type": "long"
              },
              {
                "name": "count",
                "type": "long"
              }
            ]
          }
        }
      }
    ]
  }
  ]

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema}
{"array":[{"data":{"id":5,"price":{"int":10}},"time":5,"diff":1}]}
{"array":[{"data":{"id":5,"price":{"int":12}},"time":4,"diff":1}]}
{"array":[{"data":{"id":5,"price":{"int":12}},"time":5,"diff":-1}]}

# Create a source using an inline schema.

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE data_schema_inline
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')

> CREATE TABLE data_schema_inline_tbl FROM SOURCE data_schema_inline (REFERENCE "testdrive-data-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE MATERIALIZE
> COMMIT

$ kafka-ingest format=avro topic=data schema=${schema}
{"com.materialize.cdc.progress":{"lower":[0],"upper":[3],"counts":[]}}
{"com.materialize.cdc.progress":{"lower":[3],"upper":[10],"counts":[{"time":4,"count":1},{"time":5,"count":2}, {"time": 6, "count": 1}]}}

> SELECT * FROM data_schema_inline_tbl
id price
--------
5 10

$ kafka-ingest format=avro topic=data schema=${schema}
{"array":[{"data":{"id":5,"price":{"int":10}},"time":6,"diff":-1}]}

> SELECT * FROM data_schema_inline_tbl

# Inject "junk" with a previous timestamp, which could simulate a materialized
# that restarted and emits previously emitted data at a compacted timestamp

$ kafka-ingest format=avro topic=data schema=${schema}
{"array":[{"data":{"id":5,"price":{"int":10}},"time":5,"diff":1}]}
{"array":[{"data":{"id":5,"price":{"int":12}},"time":4,"diff":1}]}
{"array":[{"data":{"id":5,"price":{"int":12}},"time":5,"diff":-1}]}

$ kafka-ingest format=avro topic=data schema=${schema}
{"com.materialize.cdc.progress":{"lower":[3],"upper":[6],"counts":[{"time":4,"count":1},{"time":5,"count":2}]}}

> SELECT * FROM data_schema_inline_tbl

# and now, new data again

$ kafka-ingest format=avro topic=data schema=${schema}
{"array":[{"data":{"id":6,"price":{"int":10}},"time":10,"diff":1}]}

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=5
{"com.materialize.cdc.progress":{"lower":[10],"upper":[15],"counts":[{"time":10,"count":1}]}}

> SELECT * FROM data_schema_inline_tbl
id price
--------
6 10

# Regression: the accumulable reduce must reject a negative record count rather
# than returning it as an aggregate result.
#
# `AccumulableErrorCheck` accumulates a record count per key and `finalize_accum`
# returns it directly for `count(*)`. The check used to compare that count
# against zero only, so a NEGATIVE count passed every arm and surfaced as a
# user-visible negative number with no error and no log, and an MV over such an
# aggregate persisted the nonsense value with a positive multiplicity.
#
# ENVELOPE MATERIALIZE is the injection primitive here because it is the only
# source envelope that passes upstream diffs through verbatim. The check itself
# is source-agnostic.
#
# NOTE: the check fires on a key whose own record count is negative, so what it
# catches depends on the grouping. A global aggregate puts every record under one
# empty key, so an unmatched retraction is masked whenever the rest of the
# collection outweighs it, and is caught only once the whole collection goes
# net-negative. Grouping isolates the corrupt record in its own key, which is
# what makes it visible here regardless of the other keys.
#
# A separate topic and source keep the injected corruption away from
# data_schema_inline_tbl, which the assertions below still read.

$ kafka-create-topic topic=negative

> BEGIN
> CREATE SOURCE negative_multiplicity
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-negative-${testdrive.seed}')

> CREATE TABLE negative_multiplicity_tbl
  FROM SOURCE negative_multiplicity (REFERENCE "testdrive-negative-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${schema}'
  ENVELOPE MATERIALIZE
> COMMIT

# Positive control: real retractions must NOT trip the check. id=1 is retracted
# and reinserted at time 2.

$ kafka-ingest format=avro topic=negative schema=${schema}
{"array":[{"data":{"id":1,"price":{"int":10}},"time":1,"diff":1}]}
{"array":[{"data":{"id":2,"price":{"int":20}},"time":1,"diff":1}]}
{"array":[{"data":{"id":1,"price":{"int":10}},"time":2,"diff":-1}]}
{"array":[{"data":{"id":1,"price":{"int":11}},"time":2,"diff":1}]}
{"com.materialize.cdc.progress":{"lower":[0],"upper":[3],"counts":[{"time":1,"count":2},{"time":2,"count":2}]}}

> SELECT id, count(*) FROM negative_multiplicity_tbl GROUP BY id
id count
--------
1  1
2  1

# Inject an unmatched retraction, giving key id=99 a net record count of -1.

$ kafka-ingest format=avro topic=negative schema=${schema}
{"array":[{"data":{"id":99,"price":{"int":999}},"time":3,"diff":-1}]}
{"com.materialize.cdc.progress":{"lower":[3],"upper":[10],"counts":[{"time":3,"count":1}]}}

! SELECT id, count(*) FROM negative_multiplicity_tbl GROUP BY id
contains:saw negative record count

# The error must propagate into a materialized view over that aggregate rather
# than the negative value becoming durable in the derived shard.

> CREATE MATERIALIZED VIEW negative_multiplicity_counts
  IN CLUSTER ${arg.single-replica-cluster} AS
  SELECT id, count(*) AS n FROM negative_multiplicity_tbl GROUP BY id

! SELECT * FROM negative_multiplicity_counts
contains:saw negative record count

> DROP MATERIALIZED VIEW negative_multiplicity_counts
> DROP SOURCE negative_multiplicity CASCADE

# Test that tails report progress messages even without new data

# The ouput of SUBSCRIBE is dependent on the replica size
$ skip-if
SELECT '${arg.default-replica-size}' != 'scale=4,workers=4';

> BEGIN

> DECLARE c CURSOR FOR SUBSCRIBE data_schema_inline_tbl WITH (SNAPSHOT = FALSE, PROGRESS = TRUE);

> FETCH 2 FROM c WITH (timeout = '60s')
14 true <null> <null> <null>
15 true <null> <null> <null>

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=6
{"com.materialize.cdc.progress":{"lower":[15],"upper":[20],"counts":[]}}

> FETCH 1 FROM c WITH (timeout = '60s')
20 true <null> <null> <null>
