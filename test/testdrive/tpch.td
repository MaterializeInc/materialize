# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the cardinality of TPCH tables. For tables that have a random
# cardinality, don't depend on exact count that could change if the rng
# implementation changes, just check that it's within the spec range.

! CREATE SOURCE gen FROM LOAD GENERATOR TPCH (SCALE FACTOR 0)
contains: multi-output sources require a FOR TABLES (..) or FOR ALL TABLES statement

! CREATE SOURCE gen FROM LOAD GENERATOR TPCH (SCALE FACTOR 9223372036854775807) FOR ALL TABLES
contains: unsupported scale factor 9223372036854776000

! CREATE SOURCE gen FROM LOAD GENERATOR TPCH (SCALE FACTOR -1) FOR ALL TABLES
contains: unsupported scale factor -1

> CREATE SOURCE gen FROM LOAD GENERATOR TPCH (SCALE FACTOR .01) FOR ALL TABLES

$ set-from-sql var=source-size
SELECT size FROM mz_sources WHERE name = 'gen'

> SHOW SOURCES
name         type       size
--------------------------------
 customer  subsource       <null>
 gen       load-generator  ${source-size}
 gen_progress  progress        <null>
 lineitem  subsource       <null>
 nation    subsource       <null>
 orders    subsource       <null>
 part      subsource       <null>
 partsupp  subsource       <null>
 region    subsource       <null>
 supplier  subsource       <null>

# SF * 150,000
> SELECT count(*) FROM customer
1500

# For each row in the ORDERS table, a random number of rows within [1 .. 7] in the LINEITEM table
> SELECT count(*) >= 15000 AND count(*) <= 15000 * 7 FROM lineitem
true

# 25 rows in the NATION table
> SELECT count(*) FROM nation
25

# For each row in the CUSTOMER table, ten rows in the ORDERS table
> SELECT count(*) FROM orders
15000

# SF * 200,000
> SELECT count(*) FROM part
2000

# For each row in the PART table, four rows in PartSupp table
> SELECT count(*) FROM partsupp
8000

# 5 rows in the REGION table
> SELECT count(*) FROM region
5

# SF * 10,000
> SELECT count(*) FROM supplier
100

##################
# Output testing #
##################

# It would be better to do the output testing in an slt for easier rewrites, but that is currently not possible, see
# https://materializeinc.slack.com/archives/C01LKF361MZ/p1666634493207499?thread_ts=1666621743.361779&cid=C01LKF361MZ

# Copied the queries from misc/python/materialize/optbench/workload/tpch.sql

> CREATE VIEW revenue (supplier_no, total_revenue) AS
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

################################################
# Without indexes
################################################

# Query 01
> SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
  FROM
    lineitem
  WHERE
    l_shipdate <= DATE '1998-12-01' - INTERVAL '60' day
  GROUP BY
    l_returnflag,
    l_linestatus
  ORDER BY
    l_returnflag,
    l_linestatus;
4 values hashing to e350d21706985f8be572176f2301222f

# Query 02
> SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
  FROM
    part, supplier, partsupp, nation, region
  WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = CAST (15 AS smallint)
    AND p_type LIKE '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost
        = (
                SELECT
                    min(ps_supplycost)
                FROM
                    partsupp, supplier, nation, region
                WHERE
                    p_partkey = ps_partkey
                    AND s_suppkey = ps_suppkey
                    AND s_nationkey = n_nationkey
                    AND n_regionkey = r_regionkey
                    AND r_name = 'EUROPE'
            )
  ORDER BY
    s_acctbal DESC, n_name, s_name, p_partkey;
5 values hashing to 746698506bf99abc24b74f03ffb7375a

# Query 03
> SELECT
      l_orderkey,
      sum(l_extendedprice * (1 - l_discount)) AS revenue,
      o_orderdate,
      o_shippriority
  FROM
      customer,
      orders,
      lineitem
  WHERE
      c_mktsegment = 'BUILDING'
      AND c_custkey = o_custkey
      AND l_orderkey = o_orderkey
      AND o_orderdate < DATE '1995-03-15'
      AND l_shipdate > DATE '1995-03-15'
  GROUP BY
      l_orderkey,
      o_orderdate,
      o_shippriority
  ORDER BY
      revenue DESC,
      o_orderdate;
127 values hashing to 637be0ff3f50cd612b004a69958bfccb

# Query 04
> SELECT
      o_orderpriority,
      count(*) AS order_count
  FROM
      orders
  WHERE
      o_orderdate >= DATE '1993-07-01'
      AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' month
      AND EXISTS (
          SELECT
              *
          FROM
              lineitem
          WHERE
              l_orderkey = o_orderkey
              AND l_commitdate < l_receiptdate
      )
  GROUP BY
      o_orderpriority
  ORDER BY
      o_orderpriority;
4 values hashing to 320decef503bc240bc73101fc659a1db

# Query 05
> SELECT
      n_name,
      sum(l_extendedprice * (1 - l_discount)) AS revenue
  FROM
      customer,
      orders,
      lineitem,
      supplier,
      nation,
      region
  WHERE
      c_custkey = o_custkey
      AND l_orderkey = o_orderkey
      AND l_suppkey = s_suppkey
      AND c_nationkey = s_nationkey
      AND s_nationkey = n_nationkey
      AND n_regionkey = r_regionkey
      AND r_name = 'ASIA'
      AND o_orderdate >= DATE '1994-01-01'
      AND o_orderdate < DATE '1995-01-01'
  GROUP BY
      n_name
  ORDER BY
      revenue DESC;
5 values hashing to 8e24fa90f7160fb0dc1e7bfaaa3e9c73

# Query 06
> SELECT
      sum(l_extendedprice * l_discount) AS revenue
  FROM
      lineitem
  WHERE
      l_quantity < 24
      AND l_shipdate >= DATE '1994-01-01'
      AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' year
      AND l_discount BETWEEN 0.06 - 0.01 AND 0.07;
1 values hashing to d9c979f1eed5940788ff3653321acac4

# Query 07
> SELECT
      supp_nation,
      cust_nation,
      l_year,
      sum(volume) AS revenue
  FROM
      (
          SELECT
              n1.n_name AS supp_nation,
              n2.n_name AS cust_nation,
              extract(year FROM l_shipdate) AS l_year,
              l_extendedprice * (1 - l_discount) AS volume
          FROM
              supplier,
              lineitem,
              orders,
              customer,
              nation n1,
              nation n2
          WHERE
              s_suppkey = l_suppkey
              AND o_orderkey = l_orderkey
              AND c_custkey = o_custkey
              AND s_nationkey = n1.n_nationkey
              AND c_nationkey = n2.n_nationkey
              AND (
                  (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                  or (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
              )
              AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
      ) AS shipping
  GROUP BY
      supp_nation,
      cust_nation,
      l_year
  ORDER BY
      supp_nation,
      cust_nation,
      l_year;
4 values hashing to 0ab7e162c0d17f7fbdebbcb6e6eb4e7f

# Query 08
> SELECT
      o_year,
      sum(case
          when nation = 'BRAZIL' then volume
          else 0
      end) / sum(volume) AS mkt_share
  FROM
      (
          SELECT
              extract(year FROM o_orderdate) AS o_year,
              l_extendedprice * (1 - l_discount) AS volume,
              n2.n_name AS nation
          FROM
              part,
              supplier,
              lineitem,
              orders,
              customer,
              nation n1,
              nation n2,
              region
          WHERE
              p_partkey = l_partkey
              AND s_suppkey = l_suppkey
              AND l_orderkey = o_orderkey
              AND o_custkey = c_custkey
              AND c_nationkey = n1.n_nationkey
              AND n1.n_regionkey = r_regionkey
              AND r_name = 'AMERICA'
              AND s_nationkey = n2.n_nationkey
              AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
              AND p_type = 'ECONOMY ANODIZED STEEL'
      ) AS all_nations
  GROUP BY
      o_year
  ORDER BY
      o_year;
2 values hashing to dbf3e9692c45719c7b16f17cbb102e8a

# Query 09
> SELECT
      nation,
      o_year,
      sum(amount) AS sum_profit
  FROM
      (
          SELECT
              n_name AS nation,
              extract(year FROM o_orderdate) AS o_year,
              l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
          FROM
              part,
              supplier,
              lineitem,
              partsupp,
              orders,
              nation
          WHERE
              s_suppkey = l_suppkey
              AND ps_suppkey = l_suppkey
              AND ps_partkey = l_partkey
              AND p_partkey = l_partkey
              AND o_orderkey = l_orderkey
              AND s_nationkey = n_nationkey
              AND p_name like '%green%'
      ) AS profit
  GROUP BY
      nation,
      o_year
  ORDER BY
      nation,
      o_year DESC;
172 values hashing to b0fbcce5ec14e4786bb47cc6a365d7e3

# Query 10
> SELECT
      c_custkey,
      c_name,
      sum(l_extendedprice * (1 - l_discount)) AS revenue,
      c_acctbal,
      n_name,
      c_address,
      c_phone,
      c_comment
  FROM
      customer,
      orders,
      lineitem,
      nation
  WHERE
      c_custkey = o_custkey
      AND l_orderkey = o_orderkey
      AND o_orderdate >= DATE '1993-10-01'
      AND o_orderdate < DATE '1994-01-01'
      AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' month
      AND l_returnflag = 'R'
      AND c_nationkey = n_nationkey
  GROUP BY
      c_custkey,
      c_name,
      c_acctbal,
      c_phone,
      n_name,
      c_address,
      c_comment
  ORDER BY
      revenue DESC;
366 values hashing to 23dd20e994576aec9e5898b128a6b5a5

# Query 11
> SELECT
      ps_partkey,
      sum(ps_supplycost * ps_availqty) AS value
  FROM
      partsupp,
      supplier,
      nation
  WHERE
      ps_suppkey = s_suppkey
      AND s_nationkey = n_nationkey
      AND n_name = 'GERMANY'
  GROUP BY
      ps_partkey having
          sum(ps_supplycost * ps_availqty) > (
              SELECT
                  sum(ps_supplycost * ps_availqty) * 0.0001
              FROM
                  partsupp,
                  supplier,
                  nation
              WHERE
                  ps_suppkey = s_suppkey
                  AND s_nationkey = n_nationkey
                  AND n_name = 'GERMANY'
          )
  ORDER BY
      value DESC;
103 values hashing to 2a4b6afc2de49adfe1cf81446d355cb1

# Query 12
> SELECT
      l_shipmode,
      sum(case
          when o_orderpriority = '1-URGENT'
              or o_orderpriority = '2-HIGH'
              then 1
          else 0
      end) AS high_line_count,
      sum(case
          when o_orderpriority <> '1-URGENT'
              AND o_orderpriority <> '2-HIGH'
              then 1
          else 0
      end) AS low_line_count
  FROM
      orders,
      lineitem
  WHERE
      o_orderkey = l_orderkey
      AND l_shipmode IN ('MAIL', 'SHIP')
      AND l_commitdate < l_receiptdate
      AND l_shipdate < l_commitdate
      AND l_receiptdate >= DATE '1994-01-01'
      AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' year
  GROUP BY
      l_shipmode
  ORDER BY
      l_shipmode;
2 values hashing to 3c31b94c99bd77e96003c2059416ed7a

# Query 13
> SELECT
      c_count,
      count(*) AS custdist
  FROM
      (
          SELECT
              c_custkey,
              count(o_orderkey) c_count
          FROM
              customer LEFT OUTER JOIN orders ON
                  c_custkey = o_custkey
                  AND o_comment NOT LIKE '%special%requests%'
          GROUP BY
              c_custkey
      ) AS c_orders
  GROUP BY
      c_count
  ORDER BY
      custdist DESC,
      c_count DESC;
24 values hashing to 0425a49d65f09ba044a8ae4e34fe2fef

# Query 14
> SELECT
      100.00 * sum(case
          when p_type like 'PROMO%'
              then l_extendedprice * (1 - l_discount)
          else 0
      end) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
  FROM
      lineitem,
      part
  WHERE
      l_partkey = p_partkey
      AND l_shipdate >= DATE '1995-09-01'
      AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' month;
1 values hashing to 52009b01ec2cea22a360dbb1de1e5acf

# Query 15
> SELECT
      s_suppkey,
      s_name,
      s_address,
      s_phone,
      total_revenue
  FROM
      supplier,
      revenue
  WHERE
      s_suppkey = supplier_no
      AND total_revenue = (
          SELECT
              max(total_revenue)
          FROM
              revenue
      )
  ORDER BY
      s_suppkey;
1 values hashing to cef6c8859cb05a1680529bf4b688bbdb

# Query 16
> SELECT
      p_brand,
      p_type,
      p_size,
      count(DISTINCT ps_suppkey) AS supplier_cnt
  FROM
      partsupp,
      part
  WHERE
      p_partkey = ps_partkey
      AND p_brand <> 'Brand#45'
      AND p_type NOT LIKE 'MEDIUM POLISHED%'
      AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
      AND ps_suppkey NOT IN (
          SELECT
              s_suppkey
          FROM
              supplier
          WHERE
              s_comment like '%Customer%Complaints%'
      )
  GROUP BY
      p_brand,
      p_type,
      p_size
  ORDER BY
      supplier_cnt DESC,
      p_brand,
      p_type,
      p_size;
309 values hashing to c4a6eb0207205cae9790096e7e4381d7

# Query 17
> SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
  FROM
    lineitem,
    part
  WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
      SELECT
        0.2 * avg(l_quantity)
      FROM
        lineitem
      WHERE
        l_partkey = p_partkey
    );
1 values hashing to 6ea48615d6dd1ff31045cd67a15ef60a

# Query 18
> SELECT
      c_name,
      c_custkey,
      o_orderkey,
      o_orderdate,
      o_totalprice,
      sum(l_quantity)
  FROM
      customer,
      orders,
      lineitem
  WHERE
      o_orderkey IN (
          SELECT
              l_orderkey
          FROM
              lineitem
          GROUP BY
              l_orderkey having
                  sum(l_quantity) > 300
      )
      AND c_custkey = o_custkey
      AND o_orderkey = l_orderkey
  GROUP BY
      c_name,
      c_custkey,
      o_orderkey,
      o_orderdate,
      o_totalprice
  ORDER BY
      o_totalprice DESC,
      o_orderdate;
3 values hashing to 321256b5aef1aedab78f125c5925de92

# Query 19
> SELECT
      sum(l_extendedprice* (1 - l_discount)) AS revenue
  FROM
      lineitem,
      part
  WHERE
      (
          p_partkey = l_partkey
          AND p_brand = 'Brand#12'
          AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
          AND l_quantity >= CAST (1 AS smallint) AND l_quantity <= CAST (1 + 10 AS smallint)
          AND p_size BETWEEN CAST (1 AS smallint) AND CAST (5 AS smallint)
          AND l_shipmode IN ('AIR', 'AIR REG')
          AND l_shipinstruct = 'DELIVER IN PERSON'
      )
      or
      (
          p_partkey = l_partkey
          AND p_brand = 'Brand#23'
          AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
          AND l_quantity >= CAST (10 AS smallint) AND l_quantity <= CAST (10 + 10 AS smallint)
          AND p_size BETWEEN CAST (1 AS smallint) AND CAST (10 AS smallint)
          AND l_shipmode IN ('AIR', 'AIR REG')
          AND l_shipinstruct = 'DELIVER IN PERSON'
      )
      or
      (
          p_partkey = l_partkey
          AND p_brand = 'Brand#34'
          AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
          AND l_quantity >= CAST (20 AS smallint) AND l_quantity <= CAST (20 + 10 AS smallint)
          AND p_size BETWEEN CAST (1 AS smallint) AND CAST (15 AS smallint)
          AND l_shipmode IN ('AIR', 'AIR REG')
          AND l_shipinstruct = 'DELIVER IN PERSON'
      );
1 values hashing to 7df1074bd6f415369eb335bff3ad1781

# Query 20
> SELECT
      s_name,
      s_address
  FROM
      supplier,
      nation
  WHERE
      s_suppkey IN (
          SELECT
              ps_suppkey
          FROM
              partsupp
          WHERE
              ps_partkey IN (
                  SELECT
                      p_partkey
                  FROM
                      part
                  WHERE
                      p_name like 'forest%'
              )
              AND ps_availqty > (
                  SELECT
                      0.5 * sum(l_quantity)
                  FROM
                      lineitem
                  WHERE
                      l_partkey = ps_partkey
                      AND l_suppkey = ps_suppkey
                      AND l_shipdate >= DATE '1995-01-01'
                      AND l_shipdate < DATE '1995-01-01' + INTERVAL '1' year
              )
      )
      AND s_nationkey = n_nationkey
      AND n_name = 'CANADA'
  ORDER BY
      s_name;
2 values hashing to d093f99c74b217399be0eccc998662af

# Query 21
> SELECT
      s_name,
      count(*) AS numwait
  FROM
      supplier,
      lineitem l1,
      orders,
      nation
  WHERE
      s_suppkey = l1.l_suppkey
      AND o_orderkey = l1.l_orderkey
      AND o_orderstatus = 'F'
      AND l1.l_receiptdate > l1.l_commitdate
      AND EXISTS (
          SELECT
              *
          FROM
              lineitem l2
          WHERE
              l2.l_orderkey = l1.l_orderkey
              AND l2.l_suppkey <> l1.l_suppkey
      )
      AND not EXISTS (
          SELECT
              *
          FROM
              lineitem l3
          WHERE
              l3.l_orderkey = l1.l_orderkey
              AND l3.l_suppkey <> l1.l_suppkey
              AND l3.l_receiptdate > l3.l_commitdate
      )
      AND s_nationkey = n_nationkey
      AND n_name = 'SAUDI ARABIA'
  GROUP BY
      s_name
  ORDER BY
      numwait DESC,
      s_name;
2 values hashing to fd48e843525a4c80b8fe0b7064b35254

# Query 22
> SELECT
      cntrycode,
      count(*) AS numcust,
      sum(c_acctbal) AS totacctbal
  FROM
      (
          SELECT
              substring(c_phone, 1, 2) AS cntrycode, c_acctbal
          FROM
              customer
          WHERE
              substring(c_phone, 1, 2)
              IN ('13', '31', '23', '29', '30', '18', '17')
              AND c_acctbal
                  > (
                          SELECT
                              avg(c_acctbal)
                          FROM
                              customer
                          WHERE
                              c_acctbal > 0.00
                              AND substring(c_phone, 1, 2)
                                  IN (
                                          '13',
                                          '31',
                                          '23',
                                          '29',
                                          '30',
                                          '18',
                                          '17'
                                      )
                      )
              AND NOT
                      EXISTS(
                          SELECT
                              *
                          FROM
                              orders
                          WHERE
                              o_custkey = c_custkey
                      )
      )
          AS custsale
  GROUP BY
      cntrycode
  ORDER BY
      cntrycode;
7 values hashing to 3782c67124c71b5bcb3460934dec46de

################################################
# With indexes
################################################

> CREATE INDEX pk_nation_nationkey ON nation (n_nationkey ASC)

> CREATE INDEX fk_nation_regionkey ON nation (n_regionkey ASC)

> CREATE INDEX pk_region_regionkey ON region (r_regionkey ASC)

> CREATE INDEX pk_part_partkey ON part (p_partkey ASC)

> CREATE INDEX pk_supplier_suppkey ON supplier (s_suppkey ASC)

> CREATE INDEX fk_supplier_nationkey ON supplier (s_nationkey ASC)

> CREATE INDEX pk_partsupp_partkey_suppkey ON partsupp (ps_partkey ASC, ps_suppkey ASC)

> CREATE INDEX fk_partsupp_partkey ON partsupp (ps_partkey ASC)

> CREATE INDEX fk_partsupp_suppkey ON partsupp (ps_suppkey ASC)

> CREATE INDEX pk_customer_custkey ON customer (c_custkey ASC)

> CREATE INDEX fk_customer_nationkey ON customer (c_nationkey ASC)

> CREATE INDEX pk_orders_orderkey ON orders (o_orderkey ASC)

> CREATE INDEX fk_orders_custkey ON orders (o_custkey ASC)

> CREATE INDEX pk_lineitem_orderkey_linenumber ON lineitem (l_orderkey ASC, l_linenumber ASC)

> CREATE INDEX fk_lineitem_orderkey ON lineitem (l_orderkey ASC)

> CREATE INDEX fk_lineitem_partkey ON lineitem (l_partkey ASC)

> CREATE INDEX fk_lineitem_suppkey ON lineitem (l_suppkey ASC)

> CREATE INDEX fk_lineitem_partsuppkey ON lineitem (l_partkey ASC, l_suppkey ASC)

# Query 01
> SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
  FROM
    lineitem
  WHERE
    l_shipdate <= DATE '1998-12-01' - INTERVAL '60' day
  GROUP BY
    l_returnflag,
    l_linestatus
  ORDER BY
    l_returnflag,
    l_linestatus;
4 values hashing to e350d21706985f8be572176f2301222f

# Query 02
> SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
  FROM
    part, supplier, partsupp, nation, region
  WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = CAST (15 AS smallint)
    AND p_type LIKE '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost
        = (
                SELECT
                    min(ps_supplycost)
                FROM
                    partsupp, supplier, nation, region
                WHERE
                    p_partkey = ps_partkey
                    AND s_suppkey = ps_suppkey
                    AND s_nationkey = n_nationkey
                    AND n_regionkey = r_regionkey
                    AND r_name = 'EUROPE'
            )
  ORDER BY
    s_acctbal DESC, n_name, s_name, p_partkey;
5 values hashing to 746698506bf99abc24b74f03ffb7375a

# Query 03
> SELECT
      l_orderkey,
      sum(l_extendedprice * (1 - l_discount)) AS revenue,
      o_orderdate,
      o_shippriority
  FROM
      customer,
      orders,
      lineitem
  WHERE
      c_mktsegment = 'BUILDING'
      AND c_custkey = o_custkey
      AND l_orderkey = o_orderkey
      AND o_orderdate < DATE '1995-03-15'
      AND l_shipdate > DATE '1995-03-15'
  GROUP BY
      l_orderkey,
      o_orderdate,
      o_shippriority
  ORDER BY
      revenue DESC,
      o_orderdate;
127 values hashing to 637be0ff3f50cd612b004a69958bfccb

# Query 04
> SELECT
      o_orderpriority,
      count(*) AS order_count
  FROM
      orders
  WHERE
      o_orderdate >= DATE '1993-07-01'
      AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' month
      AND EXISTS (
          SELECT
              *
          FROM
              lineitem
          WHERE
              l_orderkey = o_orderkey
              AND l_commitdate < l_receiptdate
      )
  GROUP BY
      o_orderpriority
  ORDER BY
      o_orderpriority;
4 values hashing to 320decef503bc240bc73101fc659a1db

# Query 05
> SELECT
      n_name,
      sum(l_extendedprice * (1 - l_discount)) AS revenue
  FROM
      customer,
      orders,
      lineitem,
      supplier,
      nation,
      region
  WHERE
      c_custkey = o_custkey
      AND l_orderkey = o_orderkey
      AND l_suppkey = s_suppkey
      AND c_nationkey = s_nationkey
      AND s_nationkey = n_nationkey
      AND n_regionkey = r_regionkey
      AND r_name = 'ASIA'
      AND o_orderdate >= DATE '1994-01-01'
      AND o_orderdate < DATE '1995-01-01'
  GROUP BY
      n_name
  ORDER BY
      revenue DESC;
5 values hashing to 8e24fa90f7160fb0dc1e7bfaaa3e9c73

# Query 06
> SELECT
      sum(l_extendedprice * l_discount) AS revenue
  FROM
      lineitem
  WHERE
      l_quantity < 24
      AND l_shipdate >= DATE '1994-01-01'
      AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' year
      AND l_discount BETWEEN 0.06 - 0.01 AND 0.07;
1 values hashing to d9c979f1eed5940788ff3653321acac4

# Query 07
> SELECT
      supp_nation,
      cust_nation,
      l_year,
      sum(volume) AS revenue
  FROM
      (
          SELECT
              n1.n_name AS supp_nation,
              n2.n_name AS cust_nation,
              extract(year FROM l_shipdate) AS l_year,
              l_extendedprice * (1 - l_discount) AS volume
          FROM
              supplier,
              lineitem,
              orders,
              customer,
              nation n1,
              nation n2
          WHERE
              s_suppkey = l_suppkey
              AND o_orderkey = l_orderkey
              AND c_custkey = o_custkey
              AND s_nationkey = n1.n_nationkey
              AND c_nationkey = n2.n_nationkey
              AND (
                  (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                  or (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
              )
              AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
      ) AS shipping
  GROUP BY
      supp_nation,
      cust_nation,
      l_year
  ORDER BY
      supp_nation,
      cust_nation,
      l_year;
4 values hashing to 0ab7e162c0d17f7fbdebbcb6e6eb4e7f

# Query 08
> SELECT
      o_year,
      sum(case
          when nation = 'BRAZIL' then volume
          else 0
      end) / sum(volume) AS mkt_share
  FROM
      (
          SELECT
              extract(year FROM o_orderdate) AS o_year,
              l_extendedprice * (1 - l_discount) AS volume,
              n2.n_name AS nation
          FROM
              part,
              supplier,
              lineitem,
              orders,
              customer,
              nation n1,
              nation n2,
              region
          WHERE
              p_partkey = l_partkey
              AND s_suppkey = l_suppkey
              AND l_orderkey = o_orderkey
              AND o_custkey = c_custkey
              AND c_nationkey = n1.n_nationkey
              AND n1.n_regionkey = r_regionkey
              AND r_name = 'AMERICA'
              AND s_nationkey = n2.n_nationkey
              AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
              AND p_type = 'ECONOMY ANODIZED STEEL'
      ) AS all_nations
  GROUP BY
      o_year
  ORDER BY
      o_year;
2 values hashing to dbf3e9692c45719c7b16f17cbb102e8a

# Query 09
> SELECT
      nation,
      o_year,
      sum(amount) AS sum_profit
  FROM
      (
          SELECT
              n_name AS nation,
              extract(year FROM o_orderdate) AS o_year,
              l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
          FROM
              part,
              supplier,
              lineitem,
              partsupp,
              orders,
              nation
          WHERE
              s_suppkey = l_suppkey
              AND ps_suppkey = l_suppkey
              AND ps_partkey = l_partkey
              AND p_partkey = l_partkey
              AND o_orderkey = l_orderkey
              AND s_nationkey = n_nationkey
              AND p_name like '%green%'
      ) AS profit
  GROUP BY
      nation,
      o_year
  ORDER BY
      nation,
      o_year DESC;
172 values hashing to b0fbcce5ec14e4786bb47cc6a365d7e3

# Query 10
> SELECT
      c_custkey,
      c_name,
      sum(l_extendedprice * (1 - l_discount)) AS revenue,
      c_acctbal,
      n_name,
      c_address,
      c_phone,
      c_comment
  FROM
      customer,
      orders,
      lineitem,
      nation
  WHERE
      c_custkey = o_custkey
      AND l_orderkey = o_orderkey
      AND o_orderdate >= DATE '1993-10-01'
      AND o_orderdate < DATE '1994-01-01'
      AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' month
      AND l_returnflag = 'R'
      AND c_nationkey = n_nationkey
  GROUP BY
      c_custkey,
      c_name,
      c_acctbal,
      c_phone,
      n_name,
      c_address,
      c_comment
  ORDER BY
      revenue DESC;
366 values hashing to 23dd20e994576aec9e5898b128a6b5a5

# Query 11
> SELECT
      ps_partkey,
      sum(ps_supplycost * ps_availqty) AS value
  FROM
      partsupp,
      supplier,
      nation
  WHERE
      ps_suppkey = s_suppkey
      AND s_nationkey = n_nationkey
      AND n_name = 'GERMANY'
  GROUP BY
      ps_partkey having
          sum(ps_supplycost * ps_availqty) > (
              SELECT
                  sum(ps_supplycost * ps_availqty) * 0.0001
              FROM
                  partsupp,
                  supplier,
                  nation
              WHERE
                  ps_suppkey = s_suppkey
                  AND s_nationkey = n_nationkey
                  AND n_name = 'GERMANY'
          )
  ORDER BY
      value DESC;
103 values hashing to 2a4b6afc2de49adfe1cf81446d355cb1

# Query 12
> SELECT
      l_shipmode,
      sum(case
          when o_orderpriority = '1-URGENT'
              or o_orderpriority = '2-HIGH'
              then 1
          else 0
      end) AS high_line_count,
      sum(case
          when o_orderpriority <> '1-URGENT'
              AND o_orderpriority <> '2-HIGH'
              then 1
          else 0
      end) AS low_line_count
  FROM
      orders,
      lineitem
  WHERE
      o_orderkey = l_orderkey
      AND l_shipmode IN ('MAIL', 'SHIP')
      AND l_commitdate < l_receiptdate
      AND l_shipdate < l_commitdate
      AND l_receiptdate >= DATE '1994-01-01'
      AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' year
  GROUP BY
      l_shipmode
  ORDER BY
      l_shipmode;
2 values hashing to 3c31b94c99bd77e96003c2059416ed7a

# Query 13
> SELECT
      c_count,
      count(*) AS custdist
  FROM
      (
          SELECT
              c_custkey,
              count(o_orderkey) c_count
          FROM
              customer LEFT OUTER JOIN orders ON
                  c_custkey = o_custkey
                  AND o_comment NOT LIKE '%special%requests%'
          GROUP BY
              c_custkey
      ) AS c_orders
  GROUP BY
      c_count
  ORDER BY
      custdist DESC,
      c_count DESC;
24 values hashing to 0425a49d65f09ba044a8ae4e34fe2fef

# Query 14
> SELECT
      100.00 * sum(case
          when p_type like 'PROMO%'
              then l_extendedprice * (1 - l_discount)
          else 0
      end) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
  FROM
      lineitem,
      part
  WHERE
      l_partkey = p_partkey
      AND l_shipdate >= DATE '1995-09-01'
      AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' month;
1 values hashing to 52009b01ec2cea22a360dbb1de1e5acf

# Query 15
> SELECT
      s_suppkey,
      s_name,
      s_address,
      s_phone,
      total_revenue
  FROM
      supplier,
      revenue
  WHERE
      s_suppkey = supplier_no
      AND total_revenue = (
          SELECT
              max(total_revenue)
          FROM
              revenue
      )
  ORDER BY
      s_suppkey;
1 values hashing to cef6c8859cb05a1680529bf4b688bbdb

# Query 16
> SELECT
      p_brand,
      p_type,
      p_size,
      count(DISTINCT ps_suppkey) AS supplier_cnt
  FROM
      partsupp,
      part
  WHERE
      p_partkey = ps_partkey
      AND p_brand <> 'Brand#45'
      AND p_type NOT LIKE 'MEDIUM POLISHED%'
      AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
      AND ps_suppkey NOT IN (
          SELECT
              s_suppkey
          FROM
              supplier
          WHERE
              s_comment like '%Customer%Complaints%'
      )
  GROUP BY
      p_brand,
      p_type,
      p_size
  ORDER BY
      supplier_cnt DESC,
      p_brand,
      p_type,
      p_size;
309 values hashing to c4a6eb0207205cae9790096e7e4381d7

# Query 17
> SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
  FROM
    lineitem,
    part
  WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
      SELECT
        0.2 * avg(l_quantity)
      FROM
        lineitem
      WHERE
        l_partkey = p_partkey
    );
1 values hashing to 6ea48615d6dd1ff31045cd67a15ef60a

# Query 18
> SELECT
      c_name,
      c_custkey,
      o_orderkey,
      o_orderdate,
      o_totalprice,
      sum(l_quantity)
  FROM
      customer,
      orders,
      lineitem
  WHERE
      o_orderkey IN (
          SELECT
              l_orderkey
          FROM
              lineitem
          GROUP BY
              l_orderkey having
                  sum(l_quantity) > 300
      )
      AND c_custkey = o_custkey
      AND o_orderkey = l_orderkey
  GROUP BY
      c_name,
      c_custkey,
      o_orderkey,
      o_orderdate,
      o_totalprice
  ORDER BY
      o_totalprice DESC,
      o_orderdate;
3 values hashing to 321256b5aef1aedab78f125c5925de92

# Query 19
> SELECT
      sum(l_extendedprice* (1 - l_discount)) AS revenue
  FROM
      lineitem,
      part
  WHERE
      (
          p_partkey = l_partkey
          AND p_brand = 'Brand#12'
          AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
          AND l_quantity >= CAST (1 AS smallint) AND l_quantity <= CAST (1 + 10 AS smallint)
          AND p_size BETWEEN CAST (1 AS smallint) AND CAST (5 AS smallint)
          AND l_shipmode IN ('AIR', 'AIR REG')
          AND l_shipinstruct = 'DELIVER IN PERSON'
      )
      or
      (
          p_partkey = l_partkey
          AND p_brand = 'Brand#23'
          AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
          AND l_quantity >= CAST (10 AS smallint) AND l_quantity <= CAST (10 + 10 AS smallint)
          AND p_size BETWEEN CAST (1 AS smallint) AND CAST (10 AS smallint)
          AND l_shipmode IN ('AIR', 'AIR REG')
          AND l_shipinstruct = 'DELIVER IN PERSON'
      )
      or
      (
          p_partkey = l_partkey
          AND p_brand = 'Brand#34'
          AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
          AND l_quantity >= CAST (20 AS smallint) AND l_quantity <= CAST (20 + 10 AS smallint)
          AND p_size BETWEEN CAST (1 AS smallint) AND CAST (15 AS smallint)
          AND l_shipmode IN ('AIR', 'AIR REG')
          AND l_shipinstruct = 'DELIVER IN PERSON'
      );
1 values hashing to 7df1074bd6f415369eb335bff3ad1781

# Query 20
> SELECT
      s_name,
      s_address
  FROM
      supplier,
      nation
  WHERE
      s_suppkey IN (
          SELECT
              ps_suppkey
          FROM
              partsupp
          WHERE
              ps_partkey IN (
                  SELECT
                      p_partkey
                  FROM
                      part
                  WHERE
                      p_name like 'forest%'
              )
              AND ps_availqty > (
                  SELECT
                      0.5 * sum(l_quantity)
                  FROM
                      lineitem
                  WHERE
                      l_partkey = ps_partkey
                      AND l_suppkey = ps_suppkey
                      AND l_shipdate >= DATE '1995-01-01'
                      AND l_shipdate < DATE '1995-01-01' + INTERVAL '1' year
              )
      )
      AND s_nationkey = n_nationkey
      AND n_name = 'CANADA'
  ORDER BY
      s_name;
2 values hashing to d093f99c74b217399be0eccc998662af

# Query 21
> SELECT
      s_name,
      count(*) AS numwait
  FROM
      supplier,
      lineitem l1,
      orders,
      nation
  WHERE
      s_suppkey = l1.l_suppkey
      AND o_orderkey = l1.l_orderkey
      AND o_orderstatus = 'F'
      AND l1.l_receiptdate > l1.l_commitdate
      AND EXISTS (
          SELECT
              *
          FROM
              lineitem l2
          WHERE
              l2.l_orderkey = l1.l_orderkey
              AND l2.l_suppkey <> l1.l_suppkey
      )
      AND not EXISTS (
          SELECT
              *
          FROM
              lineitem l3
          WHERE
              l3.l_orderkey = l1.l_orderkey
              AND l3.l_suppkey <> l1.l_suppkey
              AND l3.l_receiptdate > l3.l_commitdate
      )
      AND s_nationkey = n_nationkey
      AND n_name = 'SAUDI ARABIA'
  GROUP BY
      s_name
  ORDER BY
      numwait DESC,
      s_name;
2 values hashing to fd48e843525a4c80b8fe0b7064b35254

# Query 22
> SELECT
      cntrycode,
      count(*) AS numcust,
      sum(c_acctbal) AS totacctbal
  FROM
      (
          SELECT
              substring(c_phone, 1, 2) AS cntrycode, c_acctbal
          FROM
              customer
          WHERE
              substring(c_phone, 1, 2)
              IN ('13', '31', '23', '29', '30', '18', '17')
              AND c_acctbal
                  > (
                          SELECT
                              avg(c_acctbal)
                          FROM
                              customer
                          WHERE
                              c_acctbal > 0.00
                              AND substring(c_phone, 1, 2)
                                  IN (
                                          '13',
                                          '31',
                                          '23',
                                          '29',
                                          '30',
                                          '18',
                                          '17'
                                      )
                      )
              AND NOT
                      EXISTS(
                          SELECT
                              *
                          FROM
                              orders
                          WHERE
                              o_custkey = c_custkey
                      )
      )
          AS custsale
  GROUP BY
      cntrycode
  ORDER BY
      cntrycode;
7 values hashing to 3782c67124c71b5bcb3460934dec46de
