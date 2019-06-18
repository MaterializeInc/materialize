# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

> create table foo (a integer);

> insert into foo values (37);

> create table bar (a integer, b integer);

> insert into bar values (37, 42);
> insert into bar values (37, 42);

> select *, count(*) from foo group by a;
37 1

> select *, count(*) from foo group by a, a, a+1;
37 1

> select a from foo group by a, foo.a;
37

! select *, count(*) from bar group by a;
no column named bar.b in scope

> select a, count(*) from bar group by a;
37 2

> select * from bar group by a, b;
37 42

> create table quux(a integer);

> select count(a) from quux where a < 0;
0

> select count(*) from quux where a < 0;
0

> insert into quux values (37);
> insert into quux values (null);

> select count(*) from quux;
2

> select count(a) from quux;
1

> select count(a) from quux where a < 0;
0

> select count(*) from quux where a < 0;
0

> select sum(a) from quux;
37

> select sum(a) from quux where a < 0;
<null>
