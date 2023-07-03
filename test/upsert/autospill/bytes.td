# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=autospill

> CREATE CONNECTION conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

> CREATE SOURCE autospill
  FROM KAFKA CONNECTION conn (TOPIC
  'testdrive-autospill-${testdrive.seed}'
  )
  KEY FORMAT TEXT VALUE FORMAT TEXT
  ENVELOPE UPSERT


$ kafka-ingest format=bytes topic=autospill key-format=bytes key-terminator=:
bird:goose
animal:whale

> SELECT count(*) from autospill;
2

# Inserting large value should trigger auto spill
$ kafka-ingest format=bytes topic=autospill key-format=bytes key-terminator=:
fish:AREALLYBIGFISHAREALLYBIGFISHAREALLYBIGFISHAREALLYBIGFISH

> SELECT count(*) from autospill;
3

> SELECT
    SUM(u.envelope_state_bytes) > 0,
    SUM(u.envelope_state_count)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('autospill')
  GROUP BY s.name
  ORDER BY s.name
true 3

# Removing all the inserted keys
$ kafka-ingest format=bytes topic=autospill key-format=bytes key-terminator=:
fish:
bird:
animal:

> SELECT count(*) from autospill;
0

# Both envelope_state_bytes and envelope_state_count should be zero
> SELECT
    SUM(u.envelope_state_bytes),
    SUM(u.envelope_state_count)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('autospill')
  GROUP BY s.name
  ORDER BY s.name
0 0
