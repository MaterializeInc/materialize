# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT k, v1, v2, _fivetran_deleted FROM test.tester.green
k   v1       v2             _fivetran_deleted
---------------------------------------------
1   "hello"  "world"        false
2   "foo"    "bar"          true
3   "bing!"  "whomp whomp"  false

> SELECT COUNT(DISTINCT(_fivetran_synced)) FROM test.tester.green
3

> SELECT a, b, c, _fivetran_deleted FROM test.tester.blue
a     b       c             _fivetran_deleted
---------------------------------------------
100   "eek!"  "haw"        true
200   "vice"  "versa"      true

# Truncate does not update the _fivetran_synced column, so we have two unique _fivetran_synced
# values from the other operations.
> SELECT COUNT(DISTINCT(_fivetran_synced)) FROM test.tester.blue
2
