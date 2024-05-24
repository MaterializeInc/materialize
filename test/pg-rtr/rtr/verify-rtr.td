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

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO table_a SELECT 1,2 FROM generate_series(1, 100);
INSERT INTO table_b SELECT 1,2 FROM generate_series(1, 100);

INSERT INTO table_a SELECT 1,2 FROM generate_series(1, 101);
INSERT INTO table_b SELECT 1,2 FROM generate_series(1, 102);

# This is a RTR query, so the first result should be correct
$ set-max-tries max-tries=1

> SELECT sum(count)
  FROM (
      SELECT count(*) FROM table_a
      UNION ALL SELECT count(*) FROM table_b
      UNION ALL SELECT count(*) FROM t
  );
604

# Do it again
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO table_a SELECT 1,2 FROM generate_series(1, 101);
INSERT INTO table_b SELECT 1,2 FROM generate_series(1, 102);

> SELECT sum(count)
  FROM (
      SELECT count(*) FROM table_a
      UNION ALL SELECT count(*) FROM table_b
      UNION ALL SELECT count(*) FROM t
  );
807

# Demo materialized views built on sources obey RTR.

> SET REAL_TIME_RECENCY TO FALSE

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO table_a SELECT 1,2 FROM generate_series(1, 100);
INSERT INTO table_b SELECT 1,2 FROM generate_series(1, 100);

> SELECT sum < 1007 FROM sum;
true

> SET REAL_TIME_RECENCY TO TRUE

> SELECT sum FROM sum;
1007

# Do it again
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO table_a SELECT 1,2 FROM generate_series(1, 100);
INSERT INTO table_b SELECT 1,2 FROM generate_series(1, 100);

> SELECT sum FROM sum;
1207
