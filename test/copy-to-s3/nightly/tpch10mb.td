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

> CREATE SCHEMA tpch10mb
> CREATE SOURCE tpch10mb.source FROM LOAD GENERATOR TPCH (SCALE FACTOR 0.01)

> CREATE TABLE tpch10mb.customer FROM SOURCE tpch10mb.source (REFERENCE customer);
> CREATE TABLE tpch10mb.lineitem FROM SOURCE tpch10mb.source (REFERENCE lineitem);
> CREATE TABLE tpch10mb.nation FROM SOURCE tpch10mb.source (REFERENCE nation);
> CREATE TABLE tpch10mb.orders FROM SOURCE tpch10mb.source (REFERENCE orders);
> CREATE TABLE tpch10mb.part FROM SOURCE tpch10mb.source (REFERENCE part);
> CREATE TABLE tpch10mb.partsupp FROM SOURCE tpch10mb.source (REFERENCE partsupp);
> CREATE TABLE tpch10mb.region FROM SOURCE tpch10mb.source (REFERENCE region);
> CREATE TABLE tpch10mb.supplier FROM SOURCE tpch10mb.source (REFERENCE supplier);

> COPY tpch10mb.customer TO 's3://copytos3/test/tpch10mb/csv/customer'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch10mb.lineitem TO 's3://copytos3/test/tpch10mb/csv/lineitem'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch10mb.nation TO 's3://copytos3/test/tpch10mb/csv/nation'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch10mb.orders TO 's3://copytos3/test/tpch10mb/csv/orders'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch10mb.part TO 's3://copytos3/test/tpch10mb/csv/part'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch10mb.partsupp TO 's3://copytos3/test/tpch10mb/csv/partsupp'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch10mb.region TO 's3://copytos3/test/tpch10mb/csv/region'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );
> COPY tpch10mb.supplier TO 's3://copytos3/test/tpch10mb/csv/supplier'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'csv'
  );

> COPY (SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal::text, c_mktsegment, c_comment FROM tpch10mb.customer) TO 's3://copytos3/test/tpch10mb/parquet/customer'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'parquet'
  );
> COPY (SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity::text, l_extendedprice::text, l_discount::text, l_tax::text, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment FROM tpch10mb.lineitem) TO 's3://copytos3/test/tpch10mb/parquet/lineitem'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'parquet'
  );
> COPY tpch10mb.nation TO 's3://copytos3/test/tpch10mb/parquet/nation'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'parquet'
  );
> COPY (SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice::text, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment FROM tpch10mb.orders) TO 's3://copytos3/test/tpch10mb/parquet/orders'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'parquet'
  );
> COPY (SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice::text, p_comment FROM tpch10mb.part) TO 's3://copytos3/test/tpch10mb/parquet/part'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'parquet'
  );
> COPY (SELECT ps_partkey, ps_suppkey, ps_availqty, ps_supplycost::text, ps_comment FROM tpch10mb.partsupp) TO 's3://copytos3/test/tpch10mb/parquet/partsupp'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'parquet'
  );
> COPY tpch10mb.region TO 's3://copytos3/test/tpch10mb/parquet/region'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'parquet'
  );
> COPY (SELECT s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal::text, s_comment FROM tpch10mb.supplier) TO 's3://copytos3/test/tpch10mb/parquet/supplier'
  WITH (
    AWS CONNECTION = aws_conn,
    MAX FILE SIZE = "100MB",
    FORMAT = 'parquet'
  );
