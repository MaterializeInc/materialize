# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Regression test for https://github.com/MaterializeInc/materialize/issues/5774
#
# wrong result with inner join
#

$ set-sql-timeout duration=125ms

> CREATE TABLE customer (c_custkey integer);

> CREATE TABLE orders (o_orderkey integer, o_custkey integer NOT NULL);

> CREATE INDEX pk_orders_orderkey ON orders (o_orderkey);

> CREATE TABLE lineitem (l_orderkey integer NOT NULL);

> CREATE INDEX fk_lineitem_orderkey ON lineitem (l_orderkey ASC);

> INSERT INTO "lineitem" VALUES (1);

> INSERT INTO "orders" VALUES (1, 4);

> INSERT INTO "customer" VALUES (4);

> SELECT COUNT(*) FROM lineitem, orders, customer WHERE l_orderkey  = o_orderkey AND o_custkey = c_custkey;
1

> SELECT COUNT(*) FROM lineitem, orders, customer WHERE l_orderkey  = o_orderkey AND o_custkey = c_custkey;
1

> SELECT COUNT(*) FROM lineitem, orders, customer WHERE l_orderkey  = o_orderkey AND o_custkey = c_custkey;
1
