# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

> CREATE TABLE t (a int, b int);

> INSERT INTO t (a, b) VALUES (1, 1), (1, 2), (2, 3), (3, 1);

! SELECT a FROM t WHERE sum(b) = 3 GROUP BY a;
aggregate functions are not allowed in WHERE clause

> SELECT a FROM t GROUP BY a HAVING sum(b) = 3;
1
2
