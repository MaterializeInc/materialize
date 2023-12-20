# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-from-sql var=previous-bytes
SELECT
  (SUM(u.envelope_state_bytes))::text
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name = 'autospill'

# Inserting large value should trigger auto spill
$ kafka-ingest format=bytes topic=autospill key-format=bytes key-terminator=:
fish:AREALLYBIGFISHAREALLYBIGFISHAREALLYBIGFISHAREALLYBIGFISH

> SELECT count(*) from autospill;
3

> SELECT
    SUM(u.envelope_state_bytes) > 0,
    -- This + the assertion below that ensures the byte count goes to 0 tests
    -- that we correctly transition the `envelope_state_bytes` count to the
    -- in-rocksdb. In the past, we would accidentally SUM the previous and
    -- new size here.
    SUM(u.envelope_state_bytes) < (${previous-bytes} * 2),
    SUM(u.envelope_state_records)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name = 'autospill'
  GROUP BY s.name
  ORDER BY s.name
true true 3

# Removing all the inserted keys
$ kafka-ingest format=bytes topic=autospill key-format=bytes key-terminator=:
fish:
bird:
animal:

> SELECT count(*) from autospill;
0

# Both envelope_state_bytes and envelope_state_records should be zero
> SELECT
    SUM(u.envelope_state_bytes),
    SUM(u.envelope_state_records)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name = 'autospill'
  GROUP BY s.name
  ORDER BY s.name
0 0
