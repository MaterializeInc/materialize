# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Regression test for MySQL TIMESTAMP and DATETIME columns ingested via
# TEXT COLUMNS, with a mix of valid values, zero-dates, and out-of-range
# values. TEXT COLUMNS is the documented workaround for values that
# cannot be represented as a Materialize TIMESTAMP (notably
# '0000-00-00 00:00:00' and DATETIME values outside the TIMESTAMP range).
#
# Per https://dev.mysql.com/doc/refman/8.0/en/datetime.html:
#   * DATETIME range:  '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.499999'
#   * TIMESTAMP range: '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.499999'
#
# The DATETIME rows here exercise values that are valid DATETIMEs but
# outside the TIMESTAMP range (year 1001, year 9999), so they can only
# be represented as text.

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

> CREATE CONNECTION mysqc TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

# sql_mode = '' is required so MySQL accepts the zero-date that motivates
# the use of TEXT COLUMNS in the first place.
$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
SET SESSION sql_mode = '';
CREATE TABLE products (id INT PRIMARY KEY, created_at TIMESTAMP NULL, updated_at TIMESTAMP(6) NULL, archived_at DATETIME NULL, born_at DATETIME(6) NULL, mid_at DATETIME(4) NULL);
START TRANSACTION;
INSERT INTO products VALUES (1, '2024-04-03 10:15:13', '2024-04-03 10:15:13.123456', '2024-04-03 10:15:13', '2024-04-03 10:15:13.123456', '2024-04-03 10:15:13.1234'), (2, '0000-00-00 00:00:00', '0000-00-00 00:00:00.000000', '0000-00-00 00:00:00', '0000-00-00 00:00:00.000000', '0000-00-00 00:00:00.0000'), (3, NULL, NULL, NULL, NULL, NULL);
# Rows 7-8: DATETIME values that are outside the TIMESTAMP range
# (year 1001 is before TIMESTAMP's 1970 start; year 9999 is well beyond
# its 2038 end). TIMESTAMP columns must be NULL — these values can't
# be stored there at all.
INSERT INTO products VALUES (7, NULL, NULL, '1001-01-01 00:00:00', '1001-01-01 00:00:00.000001', '1001-01-01 00:00:00.0001'), (8, NULL, NULL, '9999-12-31 23:59:59', '9999-12-31 23:59:59.999999', '9999-12-31 23:59:59.9999');
COMMIT;

> BEGIN
> CREATE SOURCE da
  FROM MYSQL CONNECTION mysqc;
> CREATE TABLE products FROM SOURCE da (REFERENCE public.products)
  WITH (TEXT COLUMNS = (created_at, updated_at, archived_at, born_at, mid_at));
> COMMIT

# Block until the snapshot is fully ingested before issuing the
# post-snapshot inserts, so those rows go through the binlog decode
# path rather than being absorbed into the snapshot.
> SELECT id, created_at, updated_at, archived_at, born_at, mid_at FROM products ORDER BY id;
1 "2024-04-03 10:15:13" "2024-04-03 10:15:13.123456" "2024-04-03 10:15:13" "2024-04-03 10:15:13.123456" "2024-04-03 10:15:13.1234"
2 "0000-00-00 00:00:00" "0000-00-00 00:00:00.000000" "0000-00-00 00:00:00" "0000-00-00 00:00:00.000000" "0000-00-00 00:00:00.0000"
3 <null> <null> <null> <null> <null>
7 <null> <null> "1001-01-01 00:00:00" "1001-01-01 00:00:00.000001" "1001-01-01 00:00:00.0001"
8 <null> <null> "9999-12-31 23:59:59" "9999-12-31 23:59:59.999999" "9999-12-31 23:59:59.9999"

# Post-snapshot rows exercise the replication / binlog decode path,
# which uses a different mysql_common::Value variant than the snapshot.
$ mysql-execute name=mysql
USE public;
SET SESSION sql_mode = '';
START TRANSACTION;
INSERT INTO products VALUES (4, '2025-04-03 09:01:53', '2025-04-03 09:01:53.987654', '2025-04-03 09:01:53', '2025-04-03 09:01:53.987654', '2025-04-03 09:01:53.9876'), (5, '0000-00-00 00:00:00', '0000-00-00 00:00:00.000000', '0000-00-00 00:00:00', '0000-00-00 00:00:00.000000', '0000-00-00 00:00:00.0000'), (6, NULL, NULL, NULL, NULL, NULL);
INSERT INTO products VALUES (9, NULL, NULL, '1001-01-01 00:00:00', '1001-01-01 00:00:00.000001', '1001-01-01 00:00:00.0001'), (10, NULL, NULL, '9999-12-31 23:59:59', '9999-12-31 23:59:59.999999', '9999-12-31 23:59:59.9999');
COMMIT;

> SELECT id, created_at FROM products ORDER BY id;
1 "2024-04-03 10:15:13"
2 "0000-00-00 00:00:00"
3 <null>
4 "2025-04-03 09:01:53"
5 "0000-00-00 00:00:00"
6 <null>
7 <null>
8 <null>
9 <null>
10 <null>

> SELECT id, updated_at FROM products ORDER BY id;
1 "2024-04-03 10:15:13.123456"
2 "0000-00-00 00:00:00.000000"
3 <null>
4 "2025-04-03 09:01:53.987654"
5 "0000-00-00 00:00:00.000000"
6 <null>
7 <null>
8 <null>
9 <null>
10 <null>

> SELECT id, archived_at FROM products ORDER BY id;
1 "2024-04-03 10:15:13"
2 "0000-00-00 00:00:00"
3 <null>
4 "2025-04-03 09:01:53"
5 "0000-00-00 00:00:00"
6 <null>
7 "1001-01-01 00:00:00"
8 "9999-12-31 23:59:59"
9 "1001-01-01 00:00:00"
10 "9999-12-31 23:59:59"

> SELECT id, born_at FROM products ORDER BY id;
1 "2024-04-03 10:15:13.123456"
2 "0000-00-00 00:00:00.000000"
3 <null>
4 "2025-04-03 09:01:53.987654"
5 "0000-00-00 00:00:00.000000"
6 <null>
7 "1001-01-01 00:00:00.000001"
8 "9999-12-31 23:59:59.999999"
9 "1001-01-01 00:00:00.000001"
10 "9999-12-31 23:59:59.999999"

# DATETIME(4) — mid-precision case to exercise non-{0,6} fractional widths.
> SELECT id, mid_at FROM products ORDER BY id;
1 "2024-04-03 10:15:13.1234"
2 "0000-00-00 00:00:00.0000"
3 <null>
4 "2025-04-03 09:01:53.9876"
5 "0000-00-00 00:00:00.0000"
6 <null>
7 "1001-01-01 00:00:00.0001"
8 "9999-12-31 23:59:59.9999"
9 "1001-01-01 00:00:00.0001"
10 "9999-12-31 23:59:59.9999"

# Verify the column types were rewritten to text by TEXT COLUMNS.
> SELECT pg_typeof(created_at), pg_typeof(updated_at), pg_typeof(archived_at), pg_typeof(born_at), pg_typeof(mid_at) FROM products LIMIT 1;
text text text text text

# None of the data above should have caused the source to go into a stalled state. This may be
# slight overkill as a check. We've verified that all the rows are readable in previous
# queries above.
> SELECT name, status, error IS NULL FROM mz_internal.mz_source_statuses WHERE name IN ('da', 'products') ORDER BY name;
da running true
products running true

> DROP SOURCE da CASCADE;
