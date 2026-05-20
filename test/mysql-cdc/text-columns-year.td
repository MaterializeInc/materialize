# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Regression test for MySQL YEAR columns ingested via TEXT COLUMNS,
# with a mix of valid values and the zero-year sentinel.
#
# YEAR is one of the MySQL types that cannot be ingested natively in
# Materialize (see schemas.rs: YEAR is only mapped in parse_as_text_column,
# not in the native parser). CREATE TABLE FROM SOURCE on a YEAR column
# without TEXT COLUMNS errors with "unsupported type"; declaring the column
# in TEXT COLUMNS is the documented workaround.
#
# Per https://dev.mysql.com/doc/refman/8.0/en/year.html:
#   * YEAR range: 1901 to 2155
#   * Zero value: 0000 (allowed when sql_mode lacks NO_ZERO_DATE)
#
# Note: the zero value surfaces in Materialize as the string "0", not "0000",
# because decoding.rs renders YEAR via u16::to_string(). DATE/TIMESTAMP zero
# values are preserved as their literal MySQL form ("0000-00-00",
# "0000-00-00 00:00:00") — YEAR diverges from that convention today.

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

> CREATE CONNECTION mysqc TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

# sql_mode = '' is required so MySQL accepts the zero-year that motivates
# the use of TEXT COLUMNS in the first place.
$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
SET SESSION sql_mode = '';
CREATE TABLE events (id INT PRIMARY KEY, event_year YEAR NULL);
START TRANSACTION;
INSERT INTO events VALUES (1, '2024'), (2, '0000'), (3, NULL);
# Boundary rows: min and max valid YEAR values.
INSERT INTO events VALUES (4, '1901'), (5, '2155');
COMMIT;

> BEGIN
> CREATE SOURCE da
  FROM MYSQL CONNECTION mysqc;
> CREATE TABLE events FROM SOURCE da (REFERENCE public.events)
  WITH (TEXT COLUMNS = (event_year));
> COMMIT

# Post-snapshot rows exercise the replication / binlog decode path.
$ mysql-execute name=mysql
USE public;
SET SESSION sql_mode = '';
START TRANSACTION;
INSERT INTO events VALUES (6, '2025'), (7, '0000'), (8, NULL);
INSERT INTO events VALUES (9, '1901'), (10, '2155');
COMMIT;

> SELECT id, event_year FROM events ORDER BY id;
1 "2024"
2 "0"
3 <null>
4 "1901"
5 "2155"
6 "2025"
7 "0"
8 <null>
9 "1901"
10 "2155"

# Verify the column type was rewritten to text by TEXT COLUMNS.
> SELECT pg_typeof(event_year) FROM events LIMIT 1;
text

# None of the data above should have caused the source to go into a stalled state.
> SELECT name, status, error IS NULL FROM mz_internal.mz_source_statuses WHERE name IN ('da', 'events') ORDER BY name;
da running true
events running true

> DROP SOURCE da CASCADE;

#
# Negative path: a YEAR column that is NOT declared as a TEXT COLUMN cannot be
# ingested. YEAR has no native mapping in Materialize, so CREATE TABLE FROM
# SOURCE must error. This guards the documented workaround: declare YEAR
# columns in TEXT COLUMNS.
#

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE reports (id INT PRIMARY KEY, reported_year YEAR NULL);
INSERT INTO reports VALUES (1, '2024');

> CREATE SOURCE da
  FROM MYSQL CONNECTION mysqc;
! CREATE TABLE reports FROM SOURCE da (REFERENCE public.reports);
contains: unsupported type

> DROP SOURCE da CASCADE;
