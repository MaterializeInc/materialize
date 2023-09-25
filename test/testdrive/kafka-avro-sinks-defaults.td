# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Limited test
> CREATE TYPE point AS (x integer, y integer);
> CREATE TYPE custom_map AS MAP (KEY TYPE = text, VALUE TYPE = bool)
> CREATE TABLE t (c1 point, c2 text NOT NULL, c3 integer[], c4 custom_map);
> INSERT INTO t SELECT ROW(1, 1)::point AS c1, 'text' AS c2, ARRAY[1 , 2] as c3, '{a=>true}'::custom_map as c4;

> CREATE MATERIALIZED VIEW v AS SELECT * from t;

> DROP SOURCE upsert_input CASCADE;
> DROP TABLE t CASCADE;
> DROP TYPE point;
> DROP TYPE custom_map;
