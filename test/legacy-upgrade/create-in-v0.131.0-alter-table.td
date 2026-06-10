# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_alter_table_add_column = true;

> CREATE TABLE alter_table_t1 (a int);

> INSERT INTO alter_table_t1 VALUES (100), (NULL);

> CREATE VIEW alter_table_t1_v1 AS SELECT * FROM alter_table_t1;

> CREATE MATERIALIZED VIEW alter_table_t1_mv1 AS SELECT * FROM alter_table_t1_v1;

> ALTER TABLE alter_table_t1 ADD COLUMN b text;

> INSERT INTO alter_table_t1 VALUES (200, 'hello'), (300, NULL), (400, 'world');

> CREATE VIEW alter_table_t1_v2 AS SELECT * FROM alter_table_t1;

> CREATE MATERIALIZED VIEW alter_table_t1_mv2 AS SELECT * FROM alter_table_t1_v1;

> ALTER TABLE alter_table_t1 ADD COLUMN c bigint;

> INSERT INTO alter_table_t1 VALUES (500, 'apple', 101), (600, 'orange', NULL);
