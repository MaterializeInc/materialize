# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Regression test for MySQL TIMESTAMP columns ingested via TEXT COLUMNS,
# where the same upstream column contains a mix of valid timestamps and
# invalid zero-dates. TEXT COLUMNS is the documented workaround for
# columns that may hold values (e.g. '0000-00-00 00:00:00') that cannot
# be represented as a Materialize TIMESTAMP.
#
# The bug: MySQL TIMESTAMP values arrive as several mysql_common::Value
# variants depending on the protocol path (Value::Date for binlog events,
# Value::Bytes("<unix-epoch>") and Value::Int for some query/binlog
# combinations). The native TIMESTAMP decoder handles all three, but the
# TEXT COLUMNS branch in src/mysql-util/src/decoding.rs only handled
# Value::Date, so any valid TIMESTAMP routed through the Bytes/Int path
# failed with:
#
#   error decoding value for '...' column '...': received unexpected
#   value for timestamp type: Bytes("17436613..")
#
# and forced the source to be dropped and recreated.

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
CREATE TABLE products (id INT PRIMARY KEY, created_at TIMESTAMP NULL, updated_at TIMESTAMP(6) NULL);
START TRANSACTION;
INSERT INTO products VALUES (1, '2024-04-03 10:15:13', '2024-04-03 10:15:13.123456'), (2, '0000-00-00 00:00:00', '0000-00-00 00:00:00.000000'), (3, NULL, NULL);
COMMIT;

> BEGIN
> CREATE SOURCE da
  FROM MYSQL CONNECTION mysqc;
> CREATE TABLE products FROM SOURCE da (REFERENCE public.products)
  WITH (TEXT COLUMNS = (created_at, updated_at));
> COMMIT

# Post-snapshot rows exercise the replication / binlog decode path,
# which uses a different mysql_common::Value variant than the snapshot.
$ mysql-execute name=mysql
USE public;
SET SESSION sql_mode = '';
START TRANSACTION;
INSERT INTO products VALUES (4, '2025-04-03 09:01:53', '2025-04-03 09:01:53.987654'), (5, '0000-00-00 00:00:00', '0000-00-00 00:00:00.000000'), (6, NULL, NULL);
COMMIT;

> SELECT id, created_at FROM products ORDER BY id;
1 "2024-04-03 10:15:13"
2 "0000-00-00 00:00:00"
3 <null>
4 "2025-04-03 09:01:53"
5 "0000-00-00 00:00:00"
6 <null>

> SELECT id, updated_at FROM products ORDER BY id;
1 "2024-04-03 10:15:13.123456"
2 "0000-00-00 00:00:00.000000"
3 <null>
4 "2025-04-03 09:01:53.987654"
5 "0000-00-00 00:00:00.000000"
6 <null>

# Verify the column types were rewritten to text by TEXT COLUMNS.
> SELECT pg_typeof(created_at), pg_typeof(updated_at) FROM products LIMIT 1;
text text

# The decode error stalls the source even after the queries above appear to
# succeed (the snapshot rows are already in dataflow and remain queryable).
# Asserting on the source status is what makes this a hard regression check.
# We poll until either the source is fully healthy or surfaces the stall;
# this avoids a race where the binlog error has not yet been recorded.
> SELECT name, status, error IS NULL FROM mz_internal.mz_source_statuses WHERE name IN ('da', 'products') ORDER BY name;
da running true
products running true

> DROP SOURCE da CASCADE;
