# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> DROP TABLE IF EXISTS table1 CASCADE;

> CREATE TABLE table1 (f1 INTEGER);

> INSERT INTO table1 SELECT generate_series FROM generate_series(1, 10000);

> CREATE MATERIALIZED VIEW table_view1 AS SELECT COUNT(*) FROM table1;

> CREATE DEFAULT INDEX ON table_view1;

> INSERT INTO table1 SELECT generate_series FROM generate_series(1, 10000);

> SELECT * FROM table_view1;
20000
