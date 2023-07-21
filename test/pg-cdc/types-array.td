# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test the BOOLEAN data type
#


# Insert data pre-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

# set up boolean
CREATE TABLE t_boolean_array (c boolean[]);
ALTER TABLE t_boolean_array REPLICA IDENTITY FULL;

INSERT INTO t_boolean_array VALUES ('{true,false}');

# set up smallint
CREATE TABLE t_smallint_array (c smallint[]);
ALTER TABLE t_smallint_array REPLICA IDENTITY FULL;

INSERT INTO t_smallint_array VALUES ('{1,2}');

# set up integer
CREATE TABLE t_integer_array (c integer[]);
ALTER TABLE t_integer_array REPLICA IDENTITY FULL;

INSERT INTO t_integer_array VALUES ('{1,2}');

# set up bigint
CREATE TABLE t_bigint_array (c bigint[]);
ALTER TABLE t_bigint_array REPLICA IDENTITY FULL;

INSERT INTO t_bigint_array VALUES ('{1,2}');

# set up date
CREATE TABLE t_date_array (c date[]);
ALTER TABLE t_date_array REPLICA IDENTITY FULL;

INSERT INTO t_date_array VALUES ('{''2007-02-01'',''2007-02-02''}');

# set up double_precision
CREATE TABLE t_double_precision_array (c double precision[]);
ALTER TABLE t_double_precision_array REPLICA IDENTITY FULL;

INSERT INTO t_double_precision_array VALUES ('{1.1,2.1}');

# set up interval
CREATE TABLE t_interval_array (c interval[]);
ALTER TABLE t_interval_array REPLICA IDENTITY FULL;

INSERT INTO t_interval_array VALUES ('{''1y'',''2d''}');

# set up numeric
CREATE TABLE t_numeric_array (c numeric[]);
ALTER TABLE t_numeric_array REPLICA IDENTITY FULL;

INSERT INTO t_numeric_array VALUES ('{1.1,2E2}');

# set up oid
CREATE TABLE t_oid_array (c oid[]);
ALTER TABLE t_oid_array REPLICA IDENTITY FULL;

INSERT INTO t_oid_array VALUES ('{123,234}');

# set up real
CREATE TABLE t_real_array (c real[]);
ALTER TABLE t_real_array REPLICA IDENTITY FULL;

INSERT INTO t_real_array VALUES ('{1.1,2.1}');

# set up text
CREATE TABLE t_text_array (c text[]);
ALTER TABLE t_text_array REPLICA IDENTITY FULL;

INSERT INTO t_text_array VALUES ('{a,b}');

# set up time
CREATE TABLE t_time_array (c time[]);
ALTER TABLE t_time_array REPLICA IDENTITY FULL;

INSERT INTO t_time_array VALUES ('{''01:23:45'',''01:23:46''}');

# set up timestamp
CREATE TABLE t_timestamp_array (c timestamp[]);
ALTER TABLE t_timestamp_array REPLICA IDENTITY FULL;

INSERT INTO t_timestamp_array VALUES ('{''2007-02-01 15:04:05'',''2007-02-01 15:04:06''}');

# set up timestamptz
CREATE TABLE t_timestamptz_array (c timestamp with time zone[]);
ALTER TABLE t_timestamptz_array REPLICA IDENTITY FULL;

INSERT INTO t_timestamptz_array VALUES ('{''2007-02-01 15:04:05+06'',''2007-02-01 15:04:06+06''}');

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

# Insert the same data post-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres

# rewrite boolean
INSERT INTO t_boolean_array SELECT * FROM t_boolean_array;

# rewrite smallint
INSERT INTO t_smallint_array SELECT * FROM t_smallint_array;

# rewrite integer
INSERT INTO t_integer_array SELECT * FROM t_integer_array;

# rewrite bigint
INSERT INTO t_bigint_array SELECT * FROM t_bigint_array;

# rewrite date
INSERT INTO t_date_array SELECT * FROM t_date_array;

# rewrite double_precision
INSERT INTO t_double_precision_array SELECT * FROM t_double_precision_array;

# rewrite interval
INSERT INTO t_interval_array SELECT * FROM t_interval_array;

# rewrite numeric
INSERT INTO t_numeric_array SELECT * FROM t_numeric_array;

# rewrite oid
INSERT INTO t_oid_array SELECT * FROM t_oid_array;

# rewrite real
INSERT INTO t_real_array SELECT * FROM t_real_array;

# rewrite text
INSERT INTO t_text_array SELECT * FROM t_text_array;

# rewrite time
INSERT INTO t_time_array SELECT * FROM t_time_array;

# rewrite timestamp
INSERT INTO t_timestamp_array SELECT * FROM t_timestamp_array;

# rewrite timestamptz
INSERT INTO t_timestamptz_array SELECT * FROM t_timestamptz_array;

# read boolean
> SELECT pg_typeof(c) FROM t_boolean_array LIMIT 1;
boolean[]

> SELECT * FROM t_boolean_array;
{true,false}
{true,false}

# read smallint
> SELECT pg_typeof(c) FROM t_smallint_array LIMIT 1;
smallint[]

> SELECT * FROM t_smallint_array;
{1,2}
{1,2}

# read integer
> SELECT pg_typeof(c) FROM t_integer_array LIMIT 1;
integer[]

> SELECT * FROM t_integer_array;
{1,2}
{1,2}

# read bigint
> SELECT pg_typeof(c) FROM t_bigint_array LIMIT 1;
bigint[]

> SELECT * FROM t_bigint_array;
{1,2}
{1,2}

# read date
> SELECT pg_typeof(c) FROM t_date_array LIMIT 1;
date[]

> SELECT * FROM t_date_array;
{2007-02-01,2007-02-02}
{2007-02-01,2007-02-02}

# read double_precision
> SELECT pg_typeof(c) FROM t_double_precision_array LIMIT 1;
"double precision[]"

> SELECT * FROM t_double_precision_array;
{1.1,2.1}
{1.1,2.1}

# read interval
> SELECT pg_typeof(c) FROM t_interval_array LIMIT 1;
interval[]

> SELECT * FROM t_interval_array;
"{1 year,2 days}"
"{1 year,2 days}"

# read numeric
> SELECT pg_typeof(c) FROM t_numeric_array LIMIT 1;
numeric[]

> SELECT * FROM t_numeric_array;
{1.1,200}
{1.1,200}

# read oid
> SELECT pg_typeof(c) FROM t_oid_array LIMIT 1;
oid[]

> SELECT * FROM t_oid_array;
{123,234}
{123,234}

# read real
> SELECT pg_typeof(c) FROM t_real_array LIMIT 1;
real[]

> SELECT * FROM t_real_array;
{1.1,2.1}
{1.1,2.1}

# read text
> SELECT pg_typeof(c) FROM t_text_array LIMIT 1;
text[]

> SELECT * FROM t_text_array;
{a,b}
{a,b}

# read time
> SELECT pg_typeof(c) FROM t_time_array LIMIT 1;
time[]

> SELECT * FROM t_time_array;
{01:23:45,01:23:46}
{01:23:45,01:23:46}

# read timestamp
> SELECT pg_typeof(c) FROM t_timestamp_array LIMIT 1;
timestamp[]

> SELECT * FROM t_timestamp_array;
"{2007-02-01 15:04:05,2007-02-01 15:04:06}"
"{2007-02-01 15:04:05,2007-02-01 15:04:06}"

# read timestamptz
> SELECT pg_typeof(c) FROM t_timestamptz_array LIMIT 1;
"timestamp with time zone[]"

> SELECT * FROM t_timestamptz_array;
"{2007-02-01 09:04:05 UTC,2007-02-01 09:04:06 UTC}"
"{2007-02-01 09:04:05 UTC,2007-02-01 09:04:06 UTC}"

# cannot handle multi-dimensional arrays

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t_integer_array VALUES ('{{1},{2}}');

INSERT INTO t_integer_array SELECT * FROM t_integer_array;

> SELECT * FROM t_integer_array;
 {1,2}
 {1,2}
 {1,2}
 {1,2}
 {{1},{2}}
 {{1},{2}}
