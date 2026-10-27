# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# 1000 initial + 500 inserted - 250 deleted = 1250
> SELECT count(*) FROM t
1250

> SELECT v FROM t WHERE id = 1000
updated

> SELECT min(id), max(id) FROM t
251 1500

> SELECT * FROM small ORDER BY id
2 B
3 c
4 d
