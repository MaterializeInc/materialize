# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW CREATE VIEW view_with_avg_internal;
"materialize.public.view_with_avg_internal" "CREATE VIEW \"materialize\".\"public\".\"view_with_avg_internal\" (\"a\") AS SELECT \"mz_catalog\".\"avg_internal_v1\"(\"position\") FROM \"mz_catalog\".\"mz_columns\""

> SHOW CREATE MATERIALIZED VIEW mat_view_with_avg_internal;
"materialize.public.mat_view_with_avg_internal" "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"mat_view_with_avg_internal\" (\"a\") IN CLUSTER \"${arg.created-cluster}\" WITH (REFRESH = ON COMMIT) AS SELECT \"mz_catalog\".\"avg_internal_v1\"(\"position\") FROM \"mz_catalog\".\"mz_columns\""


> SHOW CREATE VIEW view_with_avg_post_v0_67;
"materialize.public.view_with_avg_post_v0_67" "CREATE VIEW \"materialize\".\"public\".\"view_with_avg_post_v0_67\" (\"a\") AS SELECT \"pg_catalog\".\"avg\"(\"position\") FROM \"mz_catalog\".\"mz_columns\""

> SHOW CREATE MATERIALIZED VIEW mat_view_with_avg_post_v0_67;
"materialize.public.mat_view_with_avg_post_v0_67" "CREATE MATERIALIZED VIEW \"materialize\".\"public\".\"mat_view_with_avg_post_v0_67\" (\"a\") IN CLUSTER \"${arg.created-cluster}\" WITH (REFRESH = ON COMMIT) AS SELECT \"pg_catalog\".\"avg\"(\"position\") FROM \"mz_catalog\".\"mz_columns\""
