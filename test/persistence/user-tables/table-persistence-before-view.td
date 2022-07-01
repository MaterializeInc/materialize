# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that materialized views that depend on user tables are properly hydrated
#

> CREATE TABLE view_table (f1 INTEGER);

> INSERT INTO view_table VALUES (NULL), (1), (1);

> CREATE MATERIALIZED VIEW view_view1 AS SELECT f1, COUNT(*) FROM view_table GROUP BY f1;

> INSERT INTO view_table VALUES (1), (2), (2);
