# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Various INTERVAL literals
#

> CREATE MATERIALIZED VIEW postgres_interval1 AS SELECT '1 year 2 months 3.4 days 5 hours 6 minutes 7.8 seconds'::interval AS interval;

> CREATE MATERIALIZED VIEW postgres_interval2 AS SELECT '1y 2mon 3.4d 5h 6m 7.8s'::interval AS interval;

> CREATE MATERIALIZED VIEW interval_second AS SELECT INTERVAL '123' SECOND AS interval;

> CREATE MATERIALIZED VIEW interval_minute AS SELECT INTERVAL '123' MINUTE AS interval;

> CREATE MATERIALIZED VIEW interval_hour AS SELECT INTERVAL '123' HOUR AS interval;

> CREATE MATERIALIZED VIEW interval_day AS SELECT INTERVAL '123' DAY AS interval;

> CREATE MATERIALIZED VIEW interval_month AS SELECT INTERVAL '123' MONTH AS interval;

> CREATE MATERIALIZED VIEW interval_year AS SELECT INTERVAL '123' YEAR AS interval;
