# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the `max_query_heap_size` session variable.

$ set-arg-default single-replica-cluster=quickstart

> CREATE SOURCE tpch
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR TPCH (SCALE FACTOR 1, UP TO 1000)
  FOR ALL TABLES;

> CREATE VIEW vq01 AS
  SELECT
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

> SET max_query_heap_size = '1kB';

! SELECT * FROM vq01;
contains:Dataflow limit exceeded

> SET max_query_heap_size = '128MB';

> SELECT * FROM vq01;
4 values hashing to 87d2cbecfc8cdcd9c9614dfb7e6d236f

> SET max_query_heap_size = '';

> SELECT * FROM vq01;
4 values hashing to 87d2cbecfc8cdcd9c9614dfb7e6d236f
