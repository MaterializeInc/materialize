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
  (SUM(u.bytes_indexed))::text
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics_raw u ON t.id = u.id
  WHERE t.name = 'autospill_tbl'

# Inserting large value should trigger auto spill
$ kafka-ingest format=bytes topic=autospill key-format=bytes key-terminator=:
fish:AREALLYBIGFISHAREALLYBIGFISHAREALLYBIGFISHAREALLYBIGFISH

> SELECT count(*) from autospill_tbl;
3

> SELECT
    -- TODO: Reenable when database-issues#8802 is fixed
    -- SUM(u.bytes_indexed) > 0,
    -- This + the assertion below that ensures the byte count goes to 0 tests
    -- that we correctly transition the `bytes_indexed` count to the
    -- in-rocksdb. In the past, we would accidentally SUM the previous and
    -- new size here.
    SUM(u.bytes_indexed) < (${previous-bytes} * 2),
    SUM(u.records_indexed)
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics_raw u ON t.id = u.id
  WHERE t.name = 'autospill_tbl'
  GROUP BY t.name
  ORDER BY t.name
true 3

$ set-from-sql var=previous-bytes-2
SELECT
  (SUM(u.bytes_indexed))::text
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics_raw u ON t.id = u.id
  WHERE t.name = 'autospill_tbl'

# Removing all the inserted keys
$ kafka-ingest format=bytes topic=autospill key-format=bytes key-terminator=:
fish:
bird:
animal:

> SELECT count(*) from autospill_tbl;
0

# records_indexed should be zero.
# bytes_indexed will still have tombstones, but should be smaller than when
# there are full values.
> SELECT
    -- TODO: Reenable when database-issues#8802 is fixed
    -- SUM(u.bytes_indexed) < ${previous-bytes-2},
    SUM(u.records_indexed)
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics_raw u ON t.id = u.id
  WHERE t.name = 'autospill_tbl'
  GROUP BY t.name
  ORDER BY t.name
0
