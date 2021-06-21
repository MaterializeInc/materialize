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

> CREATE MATERIALIZED VIEW postgres_interval1 AS SELECT '1 year 2 months 3.4 days 5 hours 6 minutes 7.8 seconds'::interval;

> CREATE MATERIALIZED VIEW postgres_interval2 AS SELECT '1y 2mon 3.4d 5h 6m 7.8s'::interval;

> CREATE MATERIALIZED VIEW interval_microsecond AS SELECT INTERVAL '123' MICROSECOND;

> CREATE MATERIALIZED VIEW interval_millisecond AS SELECT INTERVAL '123' MILLISECOND;

> CREATE MATERIALIZED VIEW interval_second AS SELECT INTERVAL '123' SECOND;

> CREATE MATERIALIZED VIEW interval_minute AS SELECT INTERVAL '123' MINUTE;

> CREATE MATERIALIZED VIEW interval_hour AS SELECT INTERVAL '123' HOUR;

> CREATE MATERIALIZED VIEW interval_day AS SELECT INTERVAL '123' DAY;

> CREATE MATERIALIZED VIEW interval_week AS SELECT INTERVAL '123' WEEK;

> CREATE MATERIALIZED VIEW interval_month AS SELECT INTERVAL '123' MONTH;

> CREATE MATERIALIZED VIEW interval_quarter AS SELECT INTERVAL '123' QUARTER;

> CREATE MATERIALIZED VIEW interval_year AS SELECT INTERVAL '123' YEAR;

> CREATE MATERIALIZED VIEW interval_decade AS SELECT INTERVAL '123' DECADE;
