# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test validates that prometheus logs make it to testdrive, and that they get retained
# correctly.

# Some prometheus metrics should always exist

# Some of the metrics queried in this test measure the amount of pending peeks. However, since
# we only scrape metrics once per second, we occaisonally need to wait more than a second
# between issuing peeks in order to make sure that the metric will reflect the accurate value.
#
# To be more explicit, the following can happen:
# 1. We issue a SELECT at time T.
# 2. We record that there is one pending PEEK in metrics.
# 3. The SELECT from step 1. completes.
# 4. We issue a SELECT to find out "how many pending PEEKs are there according to metrics?"
# 5. The SELECT from step 4. completes, returning "1 pending PEEK" where we incorrectly
#    expected there would be 0 pending PEEKs.
#
# In order to not run into this race, we need to make sure that metrics get recorded between
# steps 3 and 4, which we can do through several mechanisms such as scraping metrics more
# frequently. However, that makes running the test slower because for now, mz_metrics is persisted
# to disk. Instead, by raising the timeout, we implicitly guarantee that as testdrive exponentially
# backs off from issuing SELECTs, it will eventually wait more than 1 second between consecutive
# SELECT statements, which is enough to guarantee that metrics will be scraped when they need
# to in CI (where we scrape metrics every second).
$ set-sql-timeout duration=60s

> SELECT value > 0 AND labels ? 'os' FROM mz_metrics WHERE metric = 'mz_server_metadata_seconds'
  ORDER BY time DESC LIMIT 1
true

> SELECT value FROM mz_metrics WHERE metric = 'mz_worker_pending_peeks_queue_size'
  ORDER BY time DESC LIMIT 1
0

> SELECT count(*) > 0 FROM mz_metrics WHERE metric = 'mz_worker_command_queue_size'
true

> SELECT count(*) > 0 FROM mz_metrics WHERE metric = 'mz_arrangement_maintenance_active_info'
true

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
