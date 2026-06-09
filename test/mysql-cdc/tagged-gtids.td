# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=5s

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE orders (id INT AUTO_INCREMENT PRIMARY KEY, customer VARCHAR(255) NOT NULL);
INSERT INTO orders (customer) VALUES ('Alice'), ('Bob');
SET @@SESSION.GTID_NEXT = 'AUTOMATIC:seed_tag';
INSERT INTO orders (customer) VALUES ('Frank');
SET @@SESSION.GTID_NEXT = 'AUTOMATIC:seed_tag';
INSERT INTO orders (customer) VALUES ('Grace');
SET @@SESSION.GTID_NEXT = 'AUTOMATIC';

> BEGIN
> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE orders FROM SOURCE mz_source (REFERENCE public.orders);
> COMMIT

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
running

> SELECT * FROM orders ORDER BY id;
1 Alice
2 Bob
3 Frank
4 Grace
