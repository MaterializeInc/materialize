# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that the serving cluster index is still queryable under serializable
# isolation.

> SET cluster = serving
> SET transaction_isolation = serializable

> SELECT * FROM mv1
1

> SELECT * FROM mv2
4

> SELECT * FROM mv3
4
