# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Checks that only the required indexes are imported in the dataflow

> create table t1 (f1 integer, f2 integer, f3 integer);
> create index i12 on t1 (f1, f2);
> create index i13 on t1 (f2);
> create index i14 on t1 (f3, f2);
> create table t2 (f1 integer, f2 integer, f3 integer);
> create index i21 on t2 (f1);
> create index i22 on t2 (f2);
> create index i23 on t2 (f3, f2);
> create table t3 (f1 integer, f2 integer, f3 integer);
> create index i31 on t3 (f1);
> create index i32 on t3 (f2);
> create index i33 on t3 (f3, f2);

# All tables are joined on column 1
> create materialized view v1 as select *  from t1, t2, t3 where t1.f2=t2.f2 AND t1.f2 = t3.f2;
> SELECT DISTINCT
    (SELECT name
     FROM mz_catalog.mz_tables
     WHERE id = parts[1]) AS tablename,
                  parts[2] AS columns
  FROM
    (SELECT regexp_match(name, 'Index\((u\d+), (.*)\)') AS parts
     FROM mz_catalog.mz_dataflow_operators
     WHERE id IN
         (SELECT id
          FROM mz_catalog.mz_dataflow_operator_addresses
          WHERE address[1] =
              (SELECT DISTINCT address[1]
               FROM mz_catalog.mz_dataflow_operator_addresses
               WHERE id =
                   (SELECT DISTINCT id
                    FROM mz_catalog.mz_dataflow_names
                    WHERE name LIKE '%.v1%')))
       AND name LIKE 'Index%')
  ORDER BY tablename,
           columns;
t1 "[Column(1)]"
t2 "[Column(1)]"
t3 "[Column(1)]"

> drop view v1;

# t2 tables is joined on column 0
> create materialized view v1 as select *  from t1, t2, t3 where t1.f2=t2.f1 AND t1.f2 = t3.f2;
> SELECT DISTINCT
    (SELECT name
     FROM mz_catalog.mz_tables
     WHERE id = parts[1]) AS tablename,
                  parts[2] AS columns
  FROM
    (SELECT regexp_match(name, 'Index\((u\d+), (.*)\)') AS parts
     FROM mz_catalog.mz_dataflow_operators
     WHERE id IN
         (SELECT id
          FROM mz_catalog.mz_dataflow_operator_addresses
          WHERE address[1] =
              (SELECT DISTINCT address[1]
               FROM mz_catalog.mz_dataflow_operator_addresses
               WHERE id =
                   (SELECT DISTINCT id
                    FROM mz_catalog.mz_dataflow_names
                    WHERE name LIKE '%.v1%')))
       AND name LIKE 'Index%')
  ORDER BY tablename,
           columns;
t1 "[Column(1)]"
t2 "[Column(0)]"
t3 "[Column(1)]"

> drop view v1;

# t1-t2 join using i12 and i23
> create materialized view v1 as select *  from t1, t2 where t1.f1=t2.f3 AND t1.f2 = t2.f2;
> SELECT DISTINCT
    (SELECT name
     FROM mz_catalog.mz_tables
     WHERE id = parts[1]) AS tablename,
                  parts[2] AS columns
  FROM
    (SELECT regexp_match(name, 'Index\((u\d+), (.*)\)') AS parts
     FROM mz_catalog.mz_dataflow_operators
     WHERE id IN
         (SELECT id
          FROM mz_catalog.mz_dataflow_operator_addresses
          WHERE address[1] =
              (SELECT DISTINCT address[1]
               FROM mz_catalog.mz_dataflow_operator_addresses
               WHERE id =
                   (SELECT DISTINCT id
                    FROM mz_catalog.mz_dataflow_names
                    WHERE name LIKE '%.v1%')))
       AND name LIKE 'Index%')
  ORDER BY tablename,
           columns;
t1 "[Column(0), Column(1)]"
t2 "[Column(2), Column(1)]"

> drop view v1;

# t1-t2 join using i12 and i23, t2-t3 join using i21 and i32
> create materialized view v1 as select *  from t1, t2, t3 where t1.f1=t2.f3 AND t1.f2 = t2.f2 AND t2.f1 = t3.f2;
> SELECT DISTINCT
    (SELECT name
     FROM mz_catalog.mz_tables
     WHERE id = parts[1]) AS tablename,
                  parts[2] AS columns
  FROM
    (SELECT regexp_match(name, 'Index\((u\d+), (.*)\)') AS parts
     FROM mz_catalog.mz_dataflow_operators
     WHERE id IN
         (SELECT id
          FROM mz_catalog.mz_dataflow_operator_addresses
          WHERE address[1] =
              (SELECT DISTINCT address[1]
               FROM mz_catalog.mz_dataflow_operator_addresses
               WHERE id =
                   (SELECT DISTINCT id
                    FROM mz_catalog.mz_dataflow_names
                    WHERE name LIKE '%.v1%')))
       AND name LIKE 'Index%')
  ORDER BY tablename,
           columns;
t1 "[Column(0), Column(1)]"
t2 "[Column(0)]"
t2 "[Column(2), Column(1)]"
t3 "[Column(1)]"
