# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# In v0.67 we changed the way types get promoted in the calculation of AVG(...). As such, we had to
# migrate all existing objects that used the AVG(...) function to a new name, in this case
# AVG_INTERNAL_V1(...), so we could update the implementation of AVG(...).

# Since we're creating in v0.67, these views should not get migrated.

> CREATE VIEW view_with_avg_post_v0_67 (a) AS SELECT avg(position) FROM mz_catalog.mz_columns;
> CREATE MATERIALIZED VIEW mat_view_with_avg_post_v0_67 (a) AS SELECT avg(position) FROM mz_catalog.mz_columns;

# HACK: We create views of the same names in `create-in-v0.27.0-object-with-avg`, when running with
# versions <0.67 those views function calls of AVG(...) should get migrated to use AVG_INTERNAL_V1.
# We skip running that file in versions >=0.67 because then those views would not get migrated, and
# our check in `check-from-v0.67.0-object-with-avg` would not pass. Instead we manually create the
# views with AVG_INTERNAL_V1(...) in versions >=0.67 so our aforementioned check continues to work.

> CREATE VIEW view_with_avg_internal (a) AS SELECT avg_internal_v1(position) FROM mz_catalog.mz_columns;
> CREATE MATERIALIZED VIEW mat_view_with_avg_internal (a) AS SELECT avg_internal_v1(position) FROM mz_catalog.mz_columns;
