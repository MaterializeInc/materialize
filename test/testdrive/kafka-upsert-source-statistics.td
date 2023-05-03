# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# This testdrive file is testing upsert state with envelope statistics

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

$ kafka-create-topic topic=textbytes

> CREATE SOURCE upsert
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-textbytes-${testdrive.seed}')
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  ENVELOPE UPSERT

# Ingesting values, the latest fruit will be kept
$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=:
fish:fish_1
bird:bird_1
fruit:fruit_1
fruit:new_fruit_1
fish:

> select key, text from upsert
key           text
---------------------------
bird         bird_1
fruit        new_fruit_1

# Should remove bird, add mammal and update fruit.
$ kafka-ingest format=bytes topic=textbytes key-format=bytes key-terminator=:
mammal:mammal_2
fruit:fruit_2
bird:
flower:flower_1

> select key, text from upsert
key           text
-------------------------
fruit         fruit_2
mammal        mammal_2
flower        flower_1

# There should eventually be three records in state
> SELECT
    s.name,
    bool_and(u.snapshot_committed),
    SUM(u.messages_received),
    SUM(u.bytes_received) > 0,
    SUM(u.envelope_state_bytes) > 0,
    SUM(u.envelope_state_count)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('upsert')
  GROUP BY s.name
  ORDER BY s.name
upsert true 9 true true 3

