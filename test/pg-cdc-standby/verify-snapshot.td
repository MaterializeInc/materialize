# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The initial snapshot, taken via logical decoding on the standby.

> SELECT count(*) FROM t
1000

> SELECT * FROM small ORDER BY id
1 a
2 b
3 c
