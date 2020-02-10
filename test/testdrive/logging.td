# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test only verifies that the log relations are published, not that they
# have any specific output.

> CREATE MATERIALIZED VIEW count_operates AS SELECT count(*) FROM mz_dataflow_operators;
> SELECT count(*) FROM count_operates;
1

> CREATE MATERIALIZED VIEW count_channels AS SELECT count(*) FROM mz_dataflow_channels;
> SELECT count(*) FROM count_channels;
1

> CREATE MATERIALIZED VIEW count_elapsed AS SELECT count(*) FROM mz_scheduling_elapsed;
> SELECT count(*) FROM count_elapsed;
1

> CREATE MATERIALIZED VIEW count_histogram AS SELECT count(*) FROM mz_scheduling_histogram;
> SELECT count(*) FROM count_histogram;
1

> CREATE MATERIALIZED VIEW count_arrangement AS SELECT count(*) FROM mz_arrangement_sizes;
> SELECT count(*) FROM count_arrangement;
1

> CREATE MATERIALIZED VIEW count_materializations AS SELECT count(*) FROM mz_materializations;
> SELECT count(*) FROM count_materializations;
1

> CREATE MATERIALIZED VIEW count_frontiers AS SELECT count(*) FROM mz_materialization_frontiers;
> SELECT count(*) FROM count_frontiers;
1

> CREATE MATERIALIZED VIEW count_peeks AS SELECT count(*) FROM mz_peek_active;
> SELECT count(*) FROM count_peeks;
1

> CREATE MATERIALIZED VIEW count_peek_durations AS SELECT count(*) FROM mz_peek_durations;
> SELECT count(*) FROM count_peek_durations;
1
