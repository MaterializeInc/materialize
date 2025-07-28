# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Setup SQL Server state.
#
# Create a table that has CDC enabled.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
DROP DATABASE IF EXISTS test_40;
CREATE DATABASE test_40;
USE test_40;

EXEC sys.sp_cdc_enable_db;
ALTER DATABASE test_40 SET ALLOW_SNAPSHOT_ISOLATION ON;

# Integer types.

CREATE TABLE table_tinyint (val tinyint);
INSERT INTO table_tinyint VALUES (0), (1), (2), (100), (254), (255), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_tinyint', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_smallint (val smallint);
INSERT INTO table_smallint VALUES (-32768), (-32767), (-1), (0), (1), (32766), (32767), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_smallint', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_int (val int);
INSERT INTO table_int VALUES (-2147483648), (-2147483647), (-2), (-1), (0), (1), (2), (2147483646), (2147483647), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_int', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_bigint (val bigint);
INSERT INTO table_bigint VALUES (-9223372036854775808), (-9223372036854775807), (-2), (-1), (0), (1), (2), (9223372036854775806), (9223372036854775807), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_bigint', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_bit (val bit);
INSERT INTO table_bit VALUES (0), (1), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_bit', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_decimal(small decimal(1, 0), medium decimal(18, 5), large decimal(38, 12));
INSERT INTO table_decimal VALUES (-9, 1234567898765.12345, 12345678900987654321123456.123456789098), (1, -1111111111111.22222, -99999999999999999999999999.999999999999), (NULL, NULL, NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_decimal', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_money(val money);
INSERT INTO table_money VALUES (-922337203685477.5808), (-922337203685477.5807), (-922337203685476.9999), (-1), (0), (0.99), (1), (922337203685476.9999), (922337203685477.5806), (922337203685477.5807), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_money', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_smallmoney(val smallmoney);
INSERT INTO table_smallmoney VALUES (-214748.3648), (-214748.3647), (-214747.9999), (-1), (0), (0.99), (1), (214747.9999), (214748.3646), (214748.3647), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_smallmoney', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_date(val date);
INSERT INTO table_date VALUES ('0001-01-01'), ('0001-01-02'), ('0010-01-01'), ('2000-02-29'), ('9999-12-31'), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_date', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_time(val time);
INSERT INTO table_time VALUES ('00:00:00.0000000'), ('01:00:00.0000'), ('22:22:22.22'), ('11:33:33.000003'), ('23:59:59.999999'), ('23:59:59.9999998'), ('23:59:59.9999999'), (NULL);
INSERT INTO table_time VALUES ('01:00:00.0000001'), ('01:00:00.0000004'), ('01:00:00.0000005'), ('01:00:00.0000006'), ('01:00:00.0000009');
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_time', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_datetime2(val datetime2);
INSERT INTO table_datetime2 VALUES ('0001-01-01 00:00:00'), ('9999-12-31 23:59:59.99999'), ('9999-12-31 23:59:59.999999'), ('9999-12-31 23:59:59.9999999'), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_datetime2', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_datetime(val datetime);
INSERT INTO table_datetime VALUES ('1753-01-01 00:00:00'), ('2000-02-29 12:00:00'), ('9999-12-31 23:59:59.997'), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_datetime', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_datetimeoffset(val datetimeoffset);
INSERT INTO table_datetimeoffset VALUES ('0001-01-01 12:00:00 -05:00'), ('2000-02-29 12:00:00 -05:00'), ('9999-12-31 23:59:59.99999'), ('9999-12-31 23:59:59.999999'), ('9999-12-31 23:59:59.9999999'), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_datetimeoffset', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_smalldatetime(val smalldatetime);
INSERT INTO table_smalldatetime VALUES ('1990-01-01 00:00:00'), ('2000-02-29 12:00:00'), ('2079-06-06 23:59:29'), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_smalldatetime', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_char(val char(17));
INSERT INTO table_char VALUES ('i need to be 17 c'), ('so do i !!!!!!!!!'), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_char', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_varchar(val varchar(999));
INSERT INTO table_varchar VALUES ('I dont need to be 999 characters'), ('because we are variable length'), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_varchar', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_nchar(val nchar(17));
INSERT INTO table_nchar VALUES (N'i can contain 😊'), (N'🦀🚀'), ('also ASCCII tho'), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_nchar', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE table_nvarchar(val nvarchar(999));
INSERT INTO table_nvarchar VALUES (N'🦀🚀🦀🚀🦀🚀🦀🚀🦀🚀🦀🚀🦀🚀'), (N'💯💯💯💯💯💯💯💯💯'), (NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_nvarchar', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE dummy (data int);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'dummy', @role_name = 'SA', @supports_net_changes = 0;

# Exercise Materialize.

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_sql_server_source = true;

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET log_filter = 'mz_storage::source::sql_server=debug,mz_sql_server_util=debug,info';

> CREATE CONNECTION sql_server_test_40_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test_40,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

# TODO - SQL server delay because it's terrible for testing and the initial_lsn selection
# may happen faster than CDC manages to process the above updates
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=5s

> CREATE SOURCE test_40_data_types
  FROM SQL SERVER CONNECTION sql_server_test_40_connection
  FOR ALL TABLES;


# Wait until snapshot has emitted stats and then insert a new row
# to force LSN in MS SQL to progress.
> SELECT s.name, ss.snapshot_records_known = ss.snapshot_records_staged
  FROM mz_internal.mz_source_statistics ss JOIN mz_sources s using (id)
  WHERE s.name = 'test_40_data_types' AND ss.snapshot_records_staged > 0;
test_40_data_types true

$ sql-server-execute name=sql-server
INSERT INTO dummy VALUES (1);

> SELECT * FROM table_tinyint ORDER BY val ASC;
0
1
2
100
254
255
<null>

> SELECT * FROM table_smallint ORDER BY val ASC;
-32768
-32767
-1
0
1
32766
32767
<null>

> SELECT * FROM table_int ORDER BY val ASC;
-2147483648
-2147483647
-2
-1
0
1
2
2147483646
2147483647
<null>

> SELECT * FROM table_bigint ORDER BY val ASC;
-9223372036854775808
-9223372036854775807
-2
-1
0
1
2
9223372036854775806
9223372036854775807
<null>

> SELECT * FROM table_bit;
<null>
false
true

> SELECT * FROM table_decimal ORDER BY small ASC;
-9 1234567898765.12345 12345678900987654321123456.123456789098
1 -1111111111111.22222 -99999999999999999999999999.999999999999
<null> <null> <null>

> SELECT * FROM table_money ORDER BY val ASC;
-922337203685477.5808
-922337203685477.5807
-922337203685476.9999
-1
0
0.99
1
922337203685476.9999
922337203685477.5806
922337203685477.5807
<null>

> SELECT * FROM table_smallmoney ORDER BY val ASC;
-214748.3648
-214748.3647
-214747.9999
-1
0
0.99
1
214747.9999
214748.3646
214748.3647
<null>

> SELECT * FROM table_date ORDER BY val ASC;
0001-01-01
0001-01-02
0010-01-01
2000-02-29
9999-12-31
<null>

> SELECT * FROM table_time ORDER BY val ASC;
00:00:00
01:00:00
01:00:00
01:00:00
01:00:00
01:00:00
01:00:00
22:22:22.220
11:33:33.000003
23:59:59.999999
23:59:59.999999
23:59:59.999999
<null>

# Note: When we replicate from SQL Server we round our to precision. SQL Server
# defaults to 7 decimal places of precision and our maximum is 6. Thus
# "9999-12-31 23:59:59.9999999" gets rounded up to "+10000-01-01 00:00:00".
> SELECT * FROM table_datetime2 ORDER by val ASC;
"0001-01-01 00:00:00"
"9999-12-31 23:59:59.999990"
"9999-12-31 23:59:59.999999"
"+10000-01-01 00:00:00"
<null>

> SELECT * FROM table_datetime ORDER BY val ASC;
"1753-01-01 00:00:00"
"2000-02-29 12:00:00"
"9999-12-31 23:59:59.997"
<null>

> SELECT * FROM table_datetimeoffset ORDER BY val ASC;
"0001-01-01 22:00:00 UTC"
"2000-02-29 22:00:00 UTC"
"9999-12-31 23:59:59.999990 UTC"
"9999-12-31 23:59:59.999999 UTC"
"+10000-01-01 00:00:00 UTC"
<null>

# Note: When inserting '2079-06-06 23:59:29' into SQL Server it gets truncated
# and returned as '2079-06-06 23:59:30'. So the missing 30 seconds here is
# expected.
> SELECT * FROM table_smalldatetime ORDER BY val ASC;
"1990-01-01 00:00:00"
"2000-02-29 12:00:00"
"2079-06-06 23:59:00"
<null>

> SELECT * FROM table_char ORDER BY val ASC;
"i need to be 17 c"
"so do i !!!!!!!!!"
<null>

> SELECT * FROM table_varchar ORDER BY val ASC;
"I dont need to be 999 characters"
"because we are variable length"
<null>

> SELECT * FROM table_nchar ORDER BY val ASC;
"i can contain 😊 "
"🦀🚀             "
"also ASCCII tho  "
<null>

> SELECT * FROM table_nvarchar ORDER BY val ASC;
"🦀🚀🦀🚀🦀🚀🦀🚀🦀🚀🦀🚀🦀🚀"
"💯💯💯💯💯💯💯💯💯"
<null>

> DROP SOURCE test_40_data_types CASCADE;
