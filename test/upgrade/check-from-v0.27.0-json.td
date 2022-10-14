# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW CREATE MATERIALIZED VIEW json_view;
"materialize.public.json_view" "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"json_view\" IN CLUSTER \"default\" AS SELECT \"a\" -> 1 AS \"c1\", \"a\" ->> 'b' AS \"c2\", \"a\" #> '{b,1}' AS \"c3\", \"a\" #>> '{b, 1}' AS \"c4\", \"a\" - 'b' AS \"c5\", \"a\" @> '{b, 1}' AS \"c6\", \"a\" <@ '{b, 1}'::\"pg_catalog\".\"jsonb\" AS \"c7\", \"a\" ? 'b' AS \"c8\" FROM \"materialize\".\"public\".\"json_table\""
