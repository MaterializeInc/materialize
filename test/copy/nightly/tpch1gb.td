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

> CREATE SCHEMA tpch1gb
> BEGIN
> CREATE SOURCE tpch1gb.source FROM LOAD GENERATOR TPCH (SCALE FACTOR 1)

> CREATE TABLE tpch1gb.customer FROM SOURCE tpch1gb.source (REFERENCE customer);
> CREATE TABLE tpch1gb.lineitem FROM SOURCE tpch1gb.source (REFERENCE lineitem);
> CREATE TABLE tpch1gb.nation FROM SOURCE tpch1gb.source (REFERENCE nation);
> CREATE TABLE tpch1gb.orders FROM SOURCE tpch1gb.source (REFERENCE orders);
> CREATE TABLE tpch1gb.part FROM SOURCE tpch1gb.source (REFERENCE part);
> CREATE TABLE tpch1gb.partsupp FROM SOURCE tpch1gb.source (REFERENCE partsupp);
> CREATE TABLE tpch1gb.region FROM SOURCE tpch1gb.source (REFERENCE region);
> CREATE TABLE tpch1gb.supplier FROM SOURCE tpch1gb.source (REFERENCE supplier);
> COMMIT

> COPY tpch1gb.customer TO 's3://copytos3/test/tpch1gb/csv/customer'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'csv'
  );
> COPY tpch1gb.lineitem TO 's3://copytos3/test/tpch1gb/csv/lineitem'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'csv'
  );
> COPY tpch1gb.nation TO 's3://copytos3/test/tpch1gb/csv/nation'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'csv'
  );
> COPY tpch1gb.orders TO 's3://copytos3/test/tpch1gb/csv/orders'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'csv'
  );
> COPY tpch1gb.part TO 's3://copytos3/test/tpch1gb/csv/part'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'csv'
  );
> COPY tpch1gb.partsupp TO 's3://copytos3/test/tpch1gb/csv/partsupp'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'csv'
  );
> COPY tpch1gb.region TO 's3://copytos3/test/tpch1gb/csv/region'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'csv'
  );
> COPY tpch1gb.supplier TO 's3://copytos3/test/tpch1gb/csv/supplier'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'csv'
  );

> COPY (SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal::text, c_mktsegment, c_comment FROM tpch1gb.customer) TO 's3://copytos3/test/tpch1gb/parquet/customer'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'parquet'
  );
> COPY (SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity::text, l_extendedprice::text, l_discount::text, l_tax::text, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment FROM tpch1gb.lineitem) TO 's3://copytos3/test/tpch1gb/parquet/lineitem'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'parquet'
  );
> COPY tpch1gb.nation TO 's3://copytos3/test/tpch1gb/parquet/nation'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'parquet'
  );
> COPY (SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice::text, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment FROM tpch1gb.orders) TO 's3://copytos3/test/tpch1gb/parquet/orders'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'parquet'
  );
> COPY (SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice::text, p_comment FROM tpch1gb.part) TO 's3://copytos3/test/tpch1gb/parquet/part'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'parquet'
  );
> COPY (SELECT ps_partkey, ps_suppkey, ps_availqty, ps_supplycost::text, ps_comment FROM tpch1gb.partsupp) TO 's3://copytos3/test/tpch1gb/parquet/partsupp'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'parquet'
  );
> COPY tpch1gb.region TO 's3://copytos3/test/tpch1gb/parquet/region'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'parquet'
  );
> COPY (SELECT s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal::text, s_comment FROM tpch1gb.supplier) TO 's3://copytos3/test/tpch1gb/parquet/supplier'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "50MB",
    FORMAT = 'parquet'
  );
