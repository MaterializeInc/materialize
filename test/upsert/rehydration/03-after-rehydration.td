# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }

# Ensure we rehydrate properly
> SELECT * from upsert_tbl
key           f1       f2
---------------------------
fish          fish     1001
birdmore      geese    56
mammalmore    moose    2

# Ensure that statistics are correctly updated. Note that the
# byte count could be lower or higher than before restarting,
# as rehydration has to store values differently.
> SELECT
    SUM(u.bytes_indexed) > 0,
    SUM(u.records_indexed),
    bool_and(u.rehydration_latency IS NOT NULL)
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('upsert_tbl')
  GROUP BY t.name
  ORDER BY t.name
true 3 true

# Save the size of the rehydrated state.
$ set-from-sql var=rehydrated-state-bytes
SELECT
    (SUM(u.bytes_indexed))::text
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('upsert_tbl')

# Ensure we process updates correctly.
$ kafka-ingest format=avro topic=upsert key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish"} {"f1": "muchlongerfish", "f2": 9000}

> SELECT * from upsert_tbl
key           f1                  f2
--------------------------------------
fish          muchlongerfish      9000
birdmore      geese               56
mammalmore    moose               2

# Wait for the value's new stats to propagate. We can't
# just check that the `muchlongerfish` value is larger here,
# because the rehydrated value may be more costly. This
# means we have to do this in 2 steps, like this.
#
# This is also != because different implementations use
# space differently during rehydration and normal operation.
> SELECT
    SUM(u.bytes_indexed) != ${rehydrated-state-bytes},
    SUM(u.records_indexed)
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('upsert_tbl')
  GROUP BY t.name
  ORDER BY t.name
true 3

$ set-from-sql var=state-bytes
SELECT
    (SUM(u.bytes_indexed))::text
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('upsert_tbl')

$ kafka-ingest format=avro topic=upsert key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish"} {"f1": "MUCHMUCHMUCHLONGERVALUE", "f2": 9000}

> SELECT
    SUM(u.bytes_indexed) > ${state-bytes},
    SUM(u.records_indexed)
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('upsert_tbl')
  GROUP BY t.name
  ORDER BY t.name
true 3


# Ensure deletes work.
$ kafka-ingest format=avro topic=upsert key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish"}

> SELECT * from upsert_tbl
key           f1                  f2
--------------------------------------
birdmore      geese               56
mammalmore    moose               2

> SELECT
    SUM(u.records_indexed)
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name IN ('upsert_tbl')
  GROUP BY t.name
  ORDER BY t.name
2
