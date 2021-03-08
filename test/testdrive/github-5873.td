# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Regression test for a panic fixed by https://github.com/MaterializeInc/materialize/pull/5873
#

$ set-sql-timeout duration=125ms

> CREATE TABLE customer (
    c_custkey     integer,
    c_name        text NOT NULL,
    c_address     text NOT NULL,
    c_nationkey   integer NOT NULL,
    c_phone       text NOT NULL,
    c_acctbal     decimal(15, 2) NOT NULL,
    c_mktsegment  text NOT NULL,
    c_comment     text NOT NULL
  );

> CREATE INDEX pk_customer_custkey ON customer (c_custkey);

> CREATE INDEX fk_customer_nationkey ON customer (c_nationkey ASC);

> CREATE TABLE orders (
    o_orderkey       integer,
    o_custkey        integer NOT NULL,
    o_orderstatus    text NOT NULL,
    o_totalprice     decimal(15, 2) NOT NULL,
    o_orderdate      DATE NOT NULL,
    o_orderpriority  text NOT NULL,
    o_clerk          text NOT NULL,
    o_shippriority   integer NOT NULL,
    o_comment        text NOT NULL
  );

> CREATE INDEX pk_orders_orderkey ON orders (o_orderkey);

> CREATE INDEX fk_orders_custkey ON orders (o_custkey ASC);

> CREATE TABLE lineitem (
    l_orderkey       integer NOT NULL,
    l_partkey        integer NOT NULL,
    l_suppkey        integer NOT NULL,
    l_linenumber     integer NOT NULL,
    l_quantity       decimal(15, 2) NOT NULL,
    l_extendedprice  decimal(15, 2) NOT NULL,
    l_discount       decimal(15, 2) NOT NULL,
    l_tax            decimal(15, 2) NOT NULL,
    l_returnflag     text NOT NULL,
    l_linestatus     text NOT NULL,
    l_shipdate       date NOT NULL,
    l_commitdate     date NOT NULL,
    l_receiptdate    date NOT NULL,
    l_shipinstruct   text NOT NULL,
    l_shipmode       text NOT NULL,
    l_comment        text NOT NULL
  );

> CREATE INDEX pk_lineitem_orderkey_linenumber ON lineitem (l_orderkey, l_linenumber);

> CREATE INDEX fk_lineitem_orderkey ON lineitem (l_orderkey ASC);
> CREATE INDEX fk_lineitem_partkey ON lineitem (l_partkey ASC);
> CREATE INDEX fk_lineitem_suppkey ON lineitem (l_suppkey ASC);
> CREATE INDEX fk_lineitem_partsuppkey ON lineitem (l_partkey ASC, l_suppkey ASC);

> SELECT o_orderkey , MIN( c_custkey ) , MAX( c_custkey )
  FROM lineitem JOIN orders ON ( l_commitDATE < o_orderdate + INTERVAL ' 1 MONTHS ' )
  JOIN customer ON ( o_custkey = c_custkey )
  WHERE l_receiptDATE = o_orderdate AND l_receiptDATE = o_orderdate + INTERVAL ' 6 DAYS ' AND l_shipDATE < o_orderdate GROUP BY 1;
