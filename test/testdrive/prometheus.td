# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test validates that prometheus logs make it to testdrive, and that they get retained
# correctly.

# Ensure we retain metrics:
> CREATE MATERIALIZED VIEW metric_stats AS SELECT count(*) as total, max(time) as youngest, min(time) as oldest FROM mz_metrics;
> SELECT oldest<=youngest FROM metric_stats;
true

# The mzcompose parameters ensure that this should always be TRUE in CI (100ms poll interval with a
# logging granularity of 1s means we only present the latest metric this second, and there's a 1s
# retention window - so, only one reading retained).
> CREATE MATERIALIZED VIEW metric_entries AS SELECT COUNT(*), time from mz_metrics GROUP BY time;
> SELECT COUNT(*) = 1 FROM metric_entries;
true
> SELECT mz_internal.mz_sleep(1);
<null>
> SELECT COUNT(*) = 1 FROM metric_entries;
true

> SELECT COUNT(type) FROM mz_metrics LEFT JOIN mz_metrics_meta USING (metric) WHERE type IS NULL;
0

> SELECT COUNT(type) FROM mz_metric_histograms LEFT JOIN mz_metrics_meta USING (metric) WHERE type IS NULL;
0
