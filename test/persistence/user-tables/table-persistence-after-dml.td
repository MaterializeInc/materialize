# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


> SELECT * FROM multi_row_insert
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

> SELECT * FROM multi_insert_transaction;
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

> SELECT COUNT(*) FROM insert_rollback;
0

> SELECT * FROM insert_select;
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

> SELECT * FROM delete_table;
6
7
8
9
10
11
12
13
14
15

> SELECT * FROM update_table;
11
12
13
14
15
16
17
18
19
20
