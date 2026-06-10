# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test for the issue uncovered in a previous version of this PR
# issue: https://github.com/MaterializeInc/materialize/pull/6291#issuecomment-816457944
# fix: https://github.com/MaterializeInc/materialize/pull/6291/commits/71bcd461446796e178584133a4dd232b4cef8382
#

> CREATE TABLE customer (
    c_custkey     integer
  );

> CREATE TABLE orders (
    o_orderkey       integer,
    o_custkey        integer NOT NULL
  );

> CREATE INDEX pk_orders_orderkey ON orders (o_orderkey);
> CREATE INDEX fk_orders_custkey ON orders (o_custkey ASC);

> CREATE TABLE lineitem (
    l_orderkey       integer NOT NULL
  );

> INSERT INTO "customer" VALUES (1),(14),(15);
> INSERT INTO lineitem VALUES ( 176);

> CREATE MATERIALIZED VIEW v1 AS
  SELECT * FROM lineitem JOIN orders ON ( l_orderkey = o_orderkey ) JOIN customer ON ( o_custkey = c_custkey );

> INSERT INTO orders VALUES ( 176, 14);

> CREATE VIEW v1_nonmaterialized AS
  SELECT * FROM lineitem JOIN orders ON ( l_orderkey = o_orderkey ) JOIN customer ON ( o_custkey = c_custkey );

# 1 row expected; prior to the fix 2 rows were returned

> SELECT * FROM v1;
176 176 14 14

> SELECT * FROM v1_nonmaterialized;
176 176 14 14
