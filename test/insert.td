# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

> CREATE TABLE t (a int, b int)

! INSERT INTO t VALUES (1)
INSERT has 1 expression(s) but 2 target column(s)

! INSERT INTO t VALUES (1, 2, 3)
INSERT has 3 expression(s) but 2 target column(s)

> INSERT INTO t VALUES (1, 2)

! INSERT INTO t (a) VALUES (1, 2)
Missing column in INSERT INTO ... COLUMNS (a), expected a, b

! INSERT INTO t VALUES ('a', 'b')
INSERT does not support casting from String to Int64
