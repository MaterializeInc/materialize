# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> BEGIN;

> SELECT * FROM multi_table1;
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

> SELECT * FROM multi_table2;
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
