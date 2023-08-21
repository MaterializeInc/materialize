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
> SELECT * from upsert
key           f1       f2
---------------------------
fish          fish     1001
birdmore      geese    56
mammalmore    moose    2

# Ensure that statistics are correctly updated. Note that the
# byte count could be lower or higher than before restarting,
# as rehydration has to store values differently.
> SELECT
    SUM(u.envelope_state_bytes) > 0,
    SUM(u.envelope_state_count)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('upsert')
  GROUP BY s.name
  ORDER BY s.name
true 3

# Save the size of the rehydrated state.
$ set-from-sql var=rehydrated-state-bytes
SELECT
    (SUM(u.envelope_state_bytes))::text
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('upsert')

# Ensure we process updates correctly.
$ kafka-ingest format=avro topic=upsert key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish"} {"f1": "longerfish", "f2": 9000}

> SELECT * from upsert
key           f1                  f2
--------------------------------------
fish          longerfish          9000
birdmore      geese               56
mammalmore    moose               2

# Wait for the value's new stats to propagate. We can't
# just check that the `longerfish` value is larger here,
# because the rehydrated value may be more costly. This
# means we have to do this in 2 steps, like this.
#
# This is also != because different implementations use
# space differently during rehydration and normal operation.
> SELECT
    SUM(u.envelope_state_bytes) != ${rehydrated-state-bytes},
    SUM(u.envelope_state_count)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('upsert')
  GROUP BY s.name
  ORDER BY s.name
true 3

$ set-from-sql var=state-bytes
SELECT
    (SUM(u.envelope_state_bytes))::text
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('upsert')

$ kafka-ingest format=avro topic=upsert key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish"} {"f1": "MUCHMUCHMUCHLONGERVALUE", "f2": 9000}

> SELECT
    SUM(u.envelope_state_bytes) > ${state-bytes},
    SUM(u.envelope_state_count)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('upsert')
  GROUP BY s.name
  ORDER BY s.name
true 3


# Ensure deletes work.
$ kafka-ingest format=avro topic=upsert key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish"}

> SELECT * from upsert
key           f1                  f2
--------------------------------------
birdmore      geese               56
mammalmore    moose               2

> SELECT
    SUM(u.envelope_state_count)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('upsert')
  GROUP BY s.name
  ORDER BY s.name
2
