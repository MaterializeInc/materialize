# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test standing queries end-to-end.

# --- Setup ---

> CREATE TABLE orders (id INT, customer_id INT, amount INT)

> INSERT INTO orders VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300), (4, 20, 400), (5, 30, 500)

# --- CREATE STANDING QUERY ---

> CREATE STANDING QUERY orders_by_customer (cid INT) AS SELECT id, customer_id, amount FROM orders WHERE customer_id = cid

# --- EXECUTE: single param, multiple results ---

> EXECUTE STANDING QUERY orders_by_customer (10)
1 10 100
2 10 200

> EXECUTE STANDING QUERY orders_by_customer (20)
3 20 300
4 20 400

# --- EXECUTE: no matching rows ---

> EXECUTE STANDING QUERY orders_by_customer (99)

# --- EXECUTE: single matching row ---

> EXECUTE STANDING QUERY orders_by_customer (30)
5 30 500

# --- Standing query with SELECT * ---

> CREATE STANDING QUERY all_orders_by_customer (cid INT) AS SELECT * FROM orders WHERE customer_id = cid

> EXECUTE STANDING QUERY all_orders_by_customer (10)
1 10 100
2 10 200

# --- DROP STANDING QUERY ---

> DROP STANDING QUERY orders_by_customer

! EXECUTE STANDING QUERY orders_by_customer (10)
contains: unknown catalog item

> DROP STANDING QUERY all_orders_by_customer
