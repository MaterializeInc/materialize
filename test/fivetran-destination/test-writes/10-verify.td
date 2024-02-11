# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT k1, k2, v1, v2, _fivetran_deleted FROM test.tester.test_writes
k1  k2  v1      v2                     _fivetran_deleted
-------------------------------------------------
1   a   12.78   "{\"foo\":\"blue\"}"   false
1   b   91.28   "{\"foo\":\"green\"}"  false
2   a   0       null                   false
3   a   11.11   {}                     false

# We do an update for values with k1 = 1, and an upsert for the rest. These are two separate
# operations that will have two different synced times.
> SELECT COUNT(DISTINCT(_fivetran_synced)) FROM test.tester.test_writes
2
