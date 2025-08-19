# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# COPY TO expressions should immediately succeed or fail on their first runs
$ set-max-tries max-tries=1

> CREATE SCHEMA tpch10gb
> BEGIN
> CREATE SOURCE tpch10gb.source FROM LOAD GENERATOR TPCH (SCALE FACTOR 10)

> CREATE TABLE customer FROM SOURCE tpch10gb.source (REFERENCE customer);
> CREATE TABLE lineitem FROM SOURCE tpch10gb.source (REFERENCE lineitem);
> CREATE TABLE nation FROM SOURCE tpch10gb.source (REFERENCE nation);
> CREATE TABLE orders FROM SOURCE tpch10gb.source (REFERENCE orders);
> CREATE TABLE part FROM SOURCE tpch10gb.source (REFERENCE part);
> CREATE TABLE partsupp FROM SOURCE tpch10gb.source (REFERENCE partsupp);
> CREATE TABLE region FROM SOURCE tpch10gb.source (REFERENCE region);
> CREATE TABLE supplier FROM SOURCE tpch10gb.source (REFERENCE supplier);
> COMMIT

> COPY tpch10gb.customer TO 's3://copytos3/test/tpch10gb/csv/customer'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch10gb.lineitem TO 's3://copytos3/test/tpch10gb/csv/lineitem'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch10gb.nation TO 's3://copytos3/test/tpch10gb/csv/nation'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch10gb.orders TO 's3://copytos3/test/tpch10gb/csv/orders'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch10gb.part TO 's3://copytos3/test/tpch10gb/csv/part'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch10gb.partsupp TO 's3://copytos3/test/tpch10gb/csv/partsupp'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch10gb.region TO 's3://copytos3/test/tpch10gb/csv/region'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch10gb.supplier TO 's3://copytos3/test/tpch10gb/csv/supplier'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );

> COPY (SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal::text, c_mktsegment, c_comment FROM tpch10gb.customer) TO 's3://copytos3/test/tpch10gb/parquet/customer'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "10GB",
    FORMAT = 'parquet'
  );
> COPY (SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity::text, l_extendedprice::text, l_discount::text, l_tax::text, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment FROM tpch10gb.lineitem) TO 's3://copytos3/test/tpch10gb/parquet/lineitem'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "10GB",
    FORMAT = 'parquet'
  );
> COPY tpch10gb.nation TO 's3://copytos3/test/tpch10gb/parquet/nation'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "10GB",
    FORMAT = 'parquet'
  );
> COPY (SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice::text, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment FROM tpch10gb.orders) TO 's3://copytos3/test/tpch10gb/parquet/orders'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "10GB",
    FORMAT = 'parquet'
  );
> COPY (SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice::text, p_comment FROM tpch10gb.part) TO 's3://copytos3/test/tpch10gb/parquet/part'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "10GB",
    FORMAT = 'parquet'
  );
> COPY (SELECT ps_partkey, ps_suppkey, ps_availqty, ps_supplycost::text, ps_comment FROM tpch10gb.partsupp) TO 's3://copytos3/test/tpch10gb/parquet/partsupp'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "10GB",
    FORMAT = 'parquet'
  );
> COPY tpch10gb.region TO 's3://copytos3/test/tpch10gb/parquet/region'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "10GB",
    FORMAT = 'parquet'
  );
> COPY (SELECT s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal::text, s_comment FROM tpch10gb.supplier) TO 's3://copytos3/test/tpch10gb/parquet/supplier'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "10GB",
    FORMAT = 'parquet'
  );
