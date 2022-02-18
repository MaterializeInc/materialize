# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Many of the INTERVAL types return an incorrect value, see
# Wrong result returned when SELECT-ing some INTERVAL types #7382
# so currently this is test is mostly about being able to at least
# read back the source table without erroring out.
#

> SELECT * FROM postgres_interval1;
"1 year 2 months 3 days 14:42:07.8"

> SELECT * FROM postgres_interval2;
"1 year 2 months 3 days 14:42:07.8"

> SELECT * FROM interval_second;
00:02:03

> SELECT * FROM interval_minute;
02:03:00

> SELECT * FROM interval_hour;
"123:00:00"

> SELECT * FROM interval_day;
"123 days"

> SELECT * FROM interval_month;
"10 years 3 months"

> SELECT * FROM interval_year;
"123 years"
