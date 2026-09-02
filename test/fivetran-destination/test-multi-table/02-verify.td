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
1   "hello"  "world"        <null>
2   "foo"    "bar"          true
3   "bing!"  "whomp whomp"  <null>

> SELECT a, b, c, _fivetran_deleted FROM test.tester.blue
a     b       c             _fivetran_deleted
---------------------------------------------
100   "eek!"  "haw"        true
200   "vice"  "versa"      true
