# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> DROP TYPE IF EXISTS composite_type;

> DROP TYPE IF EXISTS nested_composite_type;

> CREATE TYPE composite_type AS (a int, b text, c float8);

> CREATE MATERIALIZED VIEW composite_type_view AS SELECT (1, 'abc', 2.0)::composite_type as f1;

> CREATE TYPE nested_composite_type AS (a int, b composite_type);

> CREATE MATERIALIZED VIEW nested_composite_type_view AS SELECT (3, (1, 'abc', 2.0))::nested_composite_type as f1;
