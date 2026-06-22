-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE TABLE users (user_id INT PRIMARY KEY, name TEXT, email TEXT);
CREATE TABLE orders (order_id INT PRIMARY KEY, user_id INT, amount NUMERIC, status TEXT);
INSERT INTO users VALUES (1, 'Alice', 'alice@test.com'), (2, 'Bob', 'bob@test.com');
INSERT INTO orders VALUES (1, 1, 100.00, 'completed'), (2, 2, 250.00, 'pending');
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
CREATE TABLE products (product_id INT PRIMARY KEY, name TEXT, price NUMERIC);
INSERT INTO products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 24.99);
ALTER TABLE products REPLICA IDENTITY FULL;
CREATE PUBLICATION mz_source FOR ALL TABLES;
CREATE ROLE mz_alt_user WITH LOGIN;
