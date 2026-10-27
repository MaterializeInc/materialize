# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Regression test for MySQL DATE columns ingested via TEXT COLUMNS,
# with a mix of valid values and the zero-date sentinel. TEXT COLUMNS
# is the documented workaround for values that cannot be represented
# as a Materialize DATE (notably '0000-00-00').
#
# Per https://dev.mysql.com/doc/refman/8.0/en/datetime.html:
#   * DATE range: '1000-01-01' to '9999-12-31'
#
# Both the snapshot path and the binlog replication path decode DATE
# as Value::Date(y, m, d, 0, 0, 0, 0), but we exercise both paths to
# guard against future divergence (the TIMESTAMP/DATETIME path has
# multiple wire variants and previously regressed).

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
CREATE TABLE events (id INT PRIMARY KEY, event_date DATE NULL);
START TRANSACTION;
INSERT INTO events VALUES (1, '2024-04-03'), (2, '0000-00-00'), (3, NULL);
# Boundary rows: min and max valid DATE values.
INSERT INTO events VALUES (4, '1000-01-01'), (5, '9999-12-31');
# Partial-zero forms: zero month and zero day. MySQL accepts these
# under sql_mode='' and they, like the full zero-date, have no
# NaiveDate representation — so they only ingest via TEXT COLUMNS.
INSERT INTO events VALUES (11, '2024-00-01'), (12, '2024-01-00');
COMMIT;

> BEGIN
> CREATE SOURCE da
  FROM MYSQL CONNECTION mysqc;
> CREATE TABLE events FROM SOURCE da (REFERENCE public.events)
  WITH (TEXT COLUMNS = (event_date));
> COMMIT

# Block until the snapshot is fully ingested before issuing the
# post-snapshot inserts, so those rows go through the binlog decode
# path rather than being absorbed into the snapshot.
> SELECT id, event_date FROM events ORDER BY id;
1 "2024-04-03"
2 "0000-00-00"
3 <null>
4 "1000-01-01"
5 "9999-12-31"
11 "2024-00-01"
12 "2024-01-00"

# Post-snapshot rows exercise the replication / binlog decode path.
$ mysql-execute name=mysql
USE public;
SET SESSION sql_mode = '';
START TRANSACTION;
INSERT INTO events VALUES (6, '2025-04-03'), (7, '0000-00-00'), (8, NULL);
INSERT INTO events VALUES (9, '1000-01-01'), (10, '9999-12-31');
INSERT INTO events VALUES (13, '2024-00-01'), (14, '2024-01-00');
COMMIT;

> SELECT id, event_date FROM events ORDER BY id;
1 "2024-04-03"
2 "0000-00-00"
3 <null>
4 "1000-01-01"
5 "9999-12-31"
6 "2025-04-03"
7 "0000-00-00"
8 <null>
9 "1000-01-01"
10 "9999-12-31"
11 "2024-00-01"
12 "2024-01-00"
13 "2024-00-01"
14 "2024-01-00"

# Verify the column type was rewritten to text by TEXT COLUMNS.
> SELECT pg_typeof(event_date) FROM events LIMIT 1;
text

# None of the data above should have caused the source to go into a stalled state.
> SELECT name, status, error IS NULL FROM mz_internal.mz_source_statuses WHERE name IN ('da', 'events') ORDER BY name;
da running true
events running true

> DROP SOURCE da CASCADE;

#
# Negative path: a DATE column that is NOT declared as a TEXT COLUMN
# cannot ingest the '0000-00-00' zero-date. Materialize's DATE type
# (backed by chrono::NaiveDate) has no representation for it, so the
# source must surface a decode error. This guards the documented
# workaround: declare such columns in TEXT COLUMNS.
#

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
SET SESSION sql_mode = '';
CREATE TABLE reports (id INT PRIMARY KEY, created_at DATE NULL);
INSERT INTO reports VALUES (1, '2024-04-03'), (2, '0000-00-00');

> BEGIN
> CREATE SOURCE da
  FROM MYSQL CONNECTION mysqc;
> CREATE TABLE reports FROM SOURCE da (REFERENCE public.reports);
> COMMIT

# Snapshot path: zero-date present at source-creation time.
! SELECT * FROM reports;
contains: error decoding value

> DROP SOURCE da CASCADE;

# Binlog path: source starts clean, zero-date arrives via replication.
$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
SET SESSION sql_mode = '';
CREATE TABLE reports (id INT PRIMARY KEY, created_at DATE NULL);
INSERT INTO reports VALUES (1, '2024-04-03');

> BEGIN
> CREATE SOURCE da
  FROM MYSQL CONNECTION mysqc;
> CREATE TABLE reports FROM SOURCE da (REFERENCE public.reports);
> COMMIT

# Block until the snapshot lands before issuing the binlog-path insert,
# so the zero-date below is decoded as a replication event, not a
# snapshot row.
> SELECT * FROM reports;
1 "2024-04-03"

$ mysql-execute name=mysql
USE public;
SET SESSION sql_mode = '';
INSERT INTO reports VALUES (2, '0000-00-00');

! SELECT * FROM reports;
contains: error decoding value

> DROP SOURCE da CASCADE;
