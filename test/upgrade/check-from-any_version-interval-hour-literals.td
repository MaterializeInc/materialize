# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# v0.20.X version broke backwards compatibility by separating the day
# and microsecond fields in Intervals.
$ skip-if
select split_part(ltrim(mz_version(), 'v'), '.', 1)::int = 0 and split_part(ltrim(mz_version(), 'v'), '.', 2)::int >= 20;

> SELECT * FROM interval_hour;
"5 days 3:00:00"
