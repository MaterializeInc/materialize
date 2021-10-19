# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT * FROM second_completes_first;
1
2
3
4
5
6
7
8
9
10

> SELECT * FROM second_completes_last;
1
2
3
4
5
6
7
8
9
10

> SELECT * FROM same_values;
1
1
2
2
3
3
4
4
5
5
