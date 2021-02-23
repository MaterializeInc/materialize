# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Establish direct replication with Postgres
#

> CREATE MATERIALIZED SOURCE "pk_table"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'pk_table' (pk INTEGER, f2 TEXT);

> CREATE MATERIALIZED SOURCE "nonpk_table"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'nonpk_table' (f1 INTEGER, f2 INTEGER);

> CREATE MATERIALIZED SOURCE "types_table"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'types_table' (date_col DATE, time_col TIME, timestamp_col TIMESTAMP, uuid_col UUID, double_col DOUBLE PRECISION);

> CREATE MATERIALIZED SOURCE "large_text"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'large_text' (f1 TEXT);

> CREATE MATERIALIZED SOURCE "trailing_space_pk"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'trailing_space_pk' (f1 TEXT);

> CREATE MATERIALIZED SOURCE "trailing_space_nopk"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'trailing_space_nopk' (f1 TEXT);

> CREATE MATERIALIZED SOURCE "multipart_pk"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'multipart_pk' (f1 INTEGER, f2 TEXT, f3 TEXT);

> CREATE MATERIALIZED SOURCE "nulls_table"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'nulls_table' (f1 TEXT, f2 INTEGER);

> CREATE MATERIALIZED SOURCE "utf8_table"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres dbname=postgres'
  PUBLICATION 'mz_source' NAMESPACE 'public' TABLE 'utf8_table' (f1 TEXT, f2 TEXT);

#
# Perform sanity checks of the initial snapshot
#

> SELECT * FROM pk_table;
1 one
2 two
3 three

> SELECT * FROM nonpk_table;
1 1
1 1
2 2
2 2

> SELECT * FROM types_table;
"2011-11-11" "11:11:11" "2011-11-11 11:11:11" "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" "1234.56768"

> SELECT LENGTH(f1) FROM large_text;
16777216

> SELECT LENGTH(f1), f1 FROM trailing_space_pk;
"6" "abc   "

> SELECT LENGTH(f1), f1 FROM trailing_space_nopk;
"6" "abc   "

> SELECT * FROM multipart_pk;
1 abc xyz

> SELECT f1, f2, f1 IS NULL, f2 IS NULL FROM nulls_table;
<null> <null> true true

> SELECT * FROM utf8_table;
"това е текст" "това \'е\' \"текст\""

#
# Confirm that the new sources can be used to build upon
#

> CREATE MATERIALIZED VIEW join_view AS SELECT * FROM pk_table, nonpk_table WHERE pk_table.pk = nonpk_table.f1;

> SELECT * FROM join_view;
"1" "one" "1" "1"
"1" "one" "1" "1"
"2" "two" "2" "2"
"2" "two" "2" "2"

#
# Basic sanity check that the timestamps are reasonable
#

> SELECT COUNT(*) > 0 FROM pk_table AS OF NOW();
true

> SELECT COUNT(*) > 0 FROM nonpk_table AS OF NOW();
true

> SELECT COUNT(*) > 0 FROM join_view AS OF NOW();
true
