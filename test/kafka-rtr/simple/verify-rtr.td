# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET allow_real_time_recency = true

> SET TRANSACTION_ISOLATION = 'STRICT SERIALIZABLE';
> SET REAL_TIME_RECENCY TO TRUE

$ kafka-ingest topic=input_1 format=bytes repeat=500000
A,B,0

$ kafka-ingest topic=input_2 format=bytes repeat=500000
A,B,0

$ kafka-ingest topic=input_1 format=bytes repeat=500001
A,B,0

$ kafka-ingest topic=input_2 format=bytes repeat=500002
A,B,0

# This is a RTR query, so the first result should be correct
$ set-max-tries max-tries=1

> SELECT sum(count)
  FROM (
      SELECT count(*) FROM input_1_tbl
      UNION ALL SELECT count(*) FROM input_2_tbl
      UNION ALL SELECT count(*) FROM t
  ) AS x;
2000204

# Do it again

$ kafka-ingest topic=input_1 format=bytes repeat=500001
A,B,0

$ kafka-ingest topic=input_2 format=bytes repeat=500002
A,B,0

> SELECT sum(count)
  FROM (
      SELECT count(*) FROM input_1_tbl
      UNION ALL SELECT count(*) FROM input_2_tbl
      UNION ALL SELECT count(*) FROM t
  ) AS x;
3000207

# Demo materialized views built on sources obey RTR.

> SET REAL_TIME_RECENCY TO FALSE

$ kafka-ingest topic=input_1 format=bytes repeat=500000
A,B,0

$ kafka-ingest topic=input_2 format=bytes repeat=500000
A,B,0

> SELECT sum < 4000207 FROM sum;
true

> SET REAL_TIME_RECENCY TO TRUE

> SELECT sum FROM sum;
4000207

# Do it again

$ kafka-ingest topic=input_1 format=bytes repeat=500000
A,B,0

$ kafka-ingest topic=input_2 format=bytes repeat=500000
A,B,0

> SELECT sum FROM sum;
5000207

$ set-regex match=(s\d+|\d{13}|u\d+|\(\d+-\d\d-\d\d\s\d\d:\d\d:\d\d\.\d\d\d\)|true|false) replacement=<>

# RTR timestamp should be present.
> EXPLAIN TIMESTAMP FOR SELECT sum FROM sum
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\n    real time recency timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n              session wall time: <> <>\n\nsource materialize.public.sum (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n"
