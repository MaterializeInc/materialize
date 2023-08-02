# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# CREATE a view containing various time literals
#

> CREATE MATERIALIZED VIEW time_literals AS SELECT DATE '2007-02-01' c1, INTERVAL '1-2 3 4:5:6.7' c2, TIME '01:23:45'c3, TIMESTAMP '2007-02-01 15:04:05.123456' c4, TIMESTAMPTZ '2007-02-01 15:04:05.123456+06' c5;
