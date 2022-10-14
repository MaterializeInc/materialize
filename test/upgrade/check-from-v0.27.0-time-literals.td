# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT * FROM time_literals;
"2007-02-01" "1 year 2 months 3 days 04:05:06.7" "01:23:45" "2007-02-01 15:04:05.123456" "2007-02-01 09:04:05.123456 UTC"
