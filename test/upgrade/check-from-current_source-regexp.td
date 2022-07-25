# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW CREATE MATERIALIZED VIEW regexp_view;
"materialize.public.regexp_view" "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"regexp_view\" IN CLUSTER \"default\" AS SELECT \"a\" !~~ 'b' AS \"c1\", \"a\" ~~* 'b' AS \"c2\", \"a\" ~ 'b' AS \"c3\", \"a\" ~* 'b' AS \"c4\", \"a\" !~ 'b' AS \"c5\", \"a\" !~* 'b' AS \"c6\" FROM \"materialize\".\"public\".\"regexp_table\""
