# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Named composite type

> SELECT f1::text, pg_typeof(f1) FROM composite_type_view;
"(1,abc,2)" "composite_type"

> CREATE MATERIALIZED VIEW composite_type_view_after AS SELECT (1, 'abc', 2.0)::composite_type as f1;

> SELECT f1::text, pg_typeof(f1) FROM composite_type_view_after;
"(1,abc,2)" "composite_type"

> DROP MATERIALIZED VIEW composite_type_view_after;

# Nested named composite types

> SELECT f1::text, pg_typeof(f1) FROM nested_composite_type_view;
"(3,\"(1,abc,2)\")" "nested_composite_type"

> CREATE MATERIALIZED VIEW nested_composite_type_view_after AS SELECT (3, (1, 'abc', 2.0))::nested_composite_type as f1;

> SELECT f1::text, pg_typeof(f1) FROM nested_composite_type_view_after;
"(3,\"(1,abc,2)\")" "nested_composite_type"

> DROP MATERIALIZED VIEW nested_composite_type_view_after;
