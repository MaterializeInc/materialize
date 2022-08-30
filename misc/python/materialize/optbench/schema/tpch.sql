-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- Table definitions for the schema of the `TPCH` benchmarking scenario.

CREATE TABLE nation (
    n_nationkey  integer ,
    n_name       char(25) NOT NULL,
    n_regionkey  integer NOT NULL,
    n_comment    varchar(152)
);

CREATE INDEX pk_nation_nationkey ON nation (n_nationkey ASC);

CREATE INDEX fk_nation_regionkey ON nation (n_regionkey ASC);

CREATE TABLE region  (
    r_regionkey  integer ,
    r_name       char(25) NOT NULL,
    r_comment    varchar(152)
);

CREATE INDEX pk_region_regionkey ON region (r_regionkey ASC);

CREATE TABLE part (
    p_partkey     integer ,
    p_name        varchar(55) NOT NULL,
    p_mfgr        char(25) NOT NULL,
    p_brand       char(10) NOT NULL,
    p_type        varchar(25) NOT NULL,
    p_size        integer NOT NULL,
    p_container   char(10) NOT NULL,
    p_retailprice decimal(15, 2) NOT NULL,
    p_comment     varchar(23) NOT NULL
);

CREATE INDEX pk_part_partkey ON part (p_partkey ASC);

CREATE TABLE supplier (
    s_suppkey     integer ,
    s_name        char(25) NOT NULL,
    s_address     varchar(40) NOT NULL,
    s_nationkey   integer NOT NULL,
    s_phone       char(15) NOT NULL,
    s_acctbal     decimal(15, 2) NOT NULL,
    s_comment     varchar(101) NOT NULL
);

CREATE INDEX pk_supplier_suppkey ON supplier (s_suppkey ASC);

CREATE INDEX fk_supplier_nationkey ON supplier (s_nationkey ASC);

CREATE TABLE partsupp (
    ps_partkey     integer NOT NULL,
    ps_suppkey     integer NOT NULL,
    ps_availqty    integer NOT NULL,
    ps_supplycost  decimal(15, 2) NOT NULL,
    ps_comment     varchar(199) NOT NULL
);

CREATE INDEX pk_partsupp_partkey_suppkey ON partsupp (ps_partkey ASC, ps_suppkey ASC);

CREATE INDEX fk_partsupp_partkey ON partsupp (ps_partkey ASC);

CREATE INDEX fk_partsupp_suppkey ON partsupp (ps_suppkey ASC);

CREATE TABLE customer (
    c_custkey     integer ,
    c_name        varchar(25) NOT NULL,
    c_address     varchar(40) NOT NULL,
    c_nationkey   integer NOT NULL,
    c_phone       char(15) NOT NULL,
    c_acctbal     decimal(15, 2) NOT NULL,
    c_mktsegment  char(10) NOT NULL,
    c_comment     varchar(117) NOT NULL
);

CREATE INDEX pk_customer_custkey ON customer (c_custkey ASC);

CREATE INDEX fk_customer_nationkey ON customer (c_nationkey ASC);

CREATE TABLE orders (
    o_orderkey       integer ,
    o_custkey        integer NOT NULL,
    o_orderstatus    char(1) NOT NULL,
    o_totalprice     decimal(15, 2) NOT NULL,
    o_orderdate      DATE NOT NULL,
    o_orderpriority  char(15) NOT NULL,
    o_clerk          char(15) NOT NULL,
    o_shippriority   integer NOT NULL,
    o_comment        varchar(79) NOT NULL
);

CREATE INDEX pk_orders_orderkey ON orders (o_orderkey ASC);

CREATE INDEX fk_orders_custkey ON orders (o_custkey ASC);

CREATE TABLE lineitem (
    l_orderkey       integer NOT NULL,
    l_partkey        integer NOT NULL,
    l_suppkey        integer NOT NULL,
    l_linenumber     integer NOT NULL,
    l_quantity       decimal(15, 2) NOT NULL,
    l_extendedprice  decimal(15, 2) NOT NULL,
    l_discount       decimal(15, 2) NOT NULL,
    l_tax            decimal(15, 2) NOT NULL,
    l_returnflag     char(1) NOT NULL,
    l_linestatus     char(1) NOT NULL,
    l_shipdate       date NOT NULL,
    l_commitdate     date NOT NULL,
    l_receiptdate    date NOT NULL,
    l_shipinstruct   char(25) NOT NULL,
    l_shipmode       char(10) NOT NULL,
    l_comment        varchar(44) NOT NULL
);

CREATE INDEX pk_lineitem_orderkey_linenumber ON lineitem (l_orderkey ASC, l_linenumber ASC);

CREATE INDEX fk_lineitem_orderkey ON lineitem (l_orderkey ASC);

CREATE INDEX fk_lineitem_partkey ON lineitem (l_partkey ASC);

CREATE INDEX fk_lineitem_suppkey ON lineitem (l_suppkey ASC);

CREATE INDEX fk_lineitem_partsuppkey ON lineitem (l_partkey ASC, l_suppkey ASC);

CREATE VIEW revenue (supplier_no, total_revenue) AS
SELECT
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
FROM
    lineitem
WHERE
    l_shipdate >= DATE '1996-01-01'
    AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' month
GROUP BY
    l_suppkey;
