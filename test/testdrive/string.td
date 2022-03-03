# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#🔬 char

> CREATE TABLE c1 (a char(1));
> CREATE TABLE c2 (a char(2));
> CREATE TABLE v (a varchar);
> CREATE TABLE v1 (a varchar(1));
> CREATE TABLE v2 (a varchar(2));
> CREATE TABLE t (a text);

#🔬🔬 literal to char

#🔬🔬🔬 Explicit

> SELECT 'abc'::char(1)
a

#🔬🔬🔬 Assignment

> INSERT INTO c1 VALUES ('a')
! INSERT INTO c1 VALUES ('ab')
contains:value too long for type character(1)

> INSERT INTO c2 VALUES ('a')
> INSERT INTO c2 VALUES ('ab')
! INSERT INTO c2 VALUES ('abc')
contains:value too long for type character(2)

#🔬🔬 char to char

#🔬🔬🔬 Explicit

> SELECT 'abc'::char(3)::char(1) AS c
a

> SELECT octet_length('abc'::char(3)::char(4)) AS c
4

#🔬🔬🔬 Assignment

! INSERT INTO c1 VALUES ('ab'::char(2));
contains:value too long for type character(1)

> INSERT INTO c1 VALUES ('a  '::char(3));

> INSERT INTO c2 VALUES ('a'::char(1));

> SELECT DISTINCT octet_length(a) FROM c1
1

> SELECT DISTINCT octet_length(a) FROM c2
2

#🔬🔬 char to varchar

#🔬🔬🔬 Explicit

> SELECT octet_length('a'::char(1)::varchar(2)) AS v
1

> SELECT octet_length('ab'::char(2)::varchar) AS v
2

> SELECT octet_length('ab'::char(2)::varchar(1)) AS v
1

> SELECT octet_length('a   '::char(3)::varchar(3)) AS v
1

> SELECT pg_typeof('ab'::char(2)::varchar(1)) AS p
"character varying"

#🔬🔬🔬 Assignment

> INSERT INTO v VALUES ('a'::char(1));
> INSERT INTO v VALUES ('ab'::char(2));
> INSERT INTO v VALUES ('abc'::char(3));
> INSERT INTO v VALUES ('a  '::char(3));

> SELECT DISTINCT octet_length(a) AS l FROM v ORDER BY l
l
---
1
2
3

> INSERT INTO v1 VALUES ('a'::char(1));
! INSERT INTO v1 VALUES ('ab'::char(2));
contains:value too long for type character varying(1)

! INSERT INTO v1 VALUES ('abc'::char(3));
contains:value too long for type character varying(1)

> INSERT INTO v1 VALUES ('a  '::char(3));

> SELECT octet_length(a) AS l FROM v1 ORDER BY l
l
---
1
1

> INSERT INTO v2 VALUES ('a'::char(1));
> INSERT INTO v2 VALUES ('ab'::char(2));
! INSERT INTO v2 VALUES ('abc'::char(3));
contains:value too long for type character varying(2)

> INSERT INTO v2 VALUES ('a  '::char(3));

> SELECT octet_length(a) AS l FROM v2 ORDER BY l
l
----
1
1
2

#🔬🔬 char to text

#🔬🔬🔬 Explicit

> SELECT 'abc'::char(3)::text AS t
t
---
abc

> SELECT octet_length('a  '::char(3)::text) AS t
t
---
1

#🔬🔬🔬 Assignment

> INSERT INTO t VALUES ('a  '::char(3));
> SELECT octet_length(a) AS l FROM t
l
---
1

#🔬 varchar

> DROP TABLE c1;
> DROP TABLE c2;
> DROP TABLE v;
> DROP TABLE v1;
> DROP TABLE v2;
> DROP TABLE t;

> CREATE TABLE c1 (a char(1));
> CREATE TABLE c2 (a char(2));
> CREATE TABLE v (a varchar);
> CREATE TABLE v1 (a varchar(1));
> CREATE TABLE v2 (a varchar(2));
> CREATE TABLE t (a text);

#🔬🔬 literal to varchar

#🔬🔬🔬 Explicit

> SELECT 'abc'::varchar(1)
a

#🔬🔬🔬 Assignment

> INSERT INTO v1 VALUES ('a');
! INSERT INTO v1 VALUES ('ab');
contains:value too long for type character varying(1)

> INSERT INTO v2 VALUES ('a');
> INSERT INTO v2 VALUES ('ab');
! INSERT INTO v2 VALUES ('abc');
contains:value too long for type character varying(2)

> INSERT INTO v VALUES ('a'), ('ab'), ('abc');

#🔬🔬 varchar to char

#🔬🔬🔬 Explicit

> SELECT 'abc'::varchar::char(1) AS c
a

> SELECT octet_length('abc'::varchar::char(4)) AS c
4

#🔬🔬🔬 Assignment

! INSERT INTO v1 VALUES ('ab'::varchar);
contains:value too long for type character varying(1)

> INSERT INTO v1 VALUES ('a  '::varchar);

> INSERT INTO v2 VALUES ('a'::varchar);
> INSERT INTO v2 VALUES ('ab'::varchar);

> SELECT DISTINCT octet_length(a) AS l FROM v1 ORDER BY l
l
----
1

> SELECT DISTINCT octet_length(a) AS l FROM v2 ORDER BY l
l
----
1
2

#🔬🔬 varchar to varchar

#🔬🔬🔬 Explicit

> SELECT octet_length('a'::varchar(1)::varchar(2)) AS v
1

> SELECT octet_length('ab'::varchar(2)::varchar(1)) AS v
1

> SELECT octet_length('a   '::varchar(3)::varchar(2)) AS v
2

> SELECT pg_typeof('a'::varchar(1)::varchar(2)) AS p
"character varying"

#🔬🔬🔬 Assignment

> INSERT INTO v VALUES ('a'::varchar(1));
> INSERT INTO v VALUES ('ab'::varchar(2));
> INSERT INTO v VALUES ('abc'::varchar(3));
> INSERT INTO v VALUES ('a  '::varchar(3));

> SELECT DISTINCT octet_length(a) AS l FROM v ORDER BY l
l
---
1
2
3

> INSERT INTO v1 VALUES ('a'::varchar(1));
! INSERT INTO v1 VALUES ('ab'::varchar(2));
contains:value too long for type character varying(1)

! INSERT INTO v1 VALUES ('abc'::varchar(3));
contains:value too long for type character varying(1)

> INSERT INTO v1 VALUES ('a  '::varchar(3));

> SELECT DISTINCT octet_length(a) AS l FROM v1 ORDER BY l
l
---
1

> INSERT INTO v2 VALUES ('a'::varchar(1));
> INSERT INTO v2 VALUES ('ab'::varchar(2));
! INSERT INTO v2 VALUES ('abc'::varchar(3));
contains:value too long for type character varying(2)

> INSERT INTO v2 VALUES ('ab '::char(3));

> SELECT DISTINCT octet_length(a) AS l FROM v2 ORDER BY l
l
----
1
2

#🔬🔬 varchar to text

#🔬🔬🔬 Explicit

> SELECT 'abc'::varchar(3)::text AS t
t
---
abc

> SELECT octet_length('a  '::varchar(3)::text) AS t
t
---
3

> SELECT pg_typeof('abc'::varchar(3)::text);
text

#🔬🔬🔬 Assignment

> INSERT INTO t VALUES ('a  '::varchar(3));
> SELECT octet_length(a) AS l FROM t
l
---
3

#🔬 text

> DROP TABLE c1;
> DROP TABLE c2;
> DROP TABLE v;
> DROP TABLE v1;
> DROP TABLE v2;
> DROP TABLE t;

> CREATE TABLE c1 (a char(1));
> CREATE TABLE c2 (a char(2));
> CREATE TABLE v (a varchar);
> CREATE TABLE v1 (a varchar(1));
> CREATE TABLE v2 (a varchar(2));
> CREATE TABLE t (a text);

#🔬🔬 literal to text

#🔬🔬🔬 Explicit

> SELECT 'abc'::text
abc

#🔬🔬🔬 Assignment

> INSERT INTO v VALUES ('a'), ('ab'), ('abc');

#🔬🔬 text to char

#🔬🔬🔬 Explicit

> SELECT 'abc'::text::char(1) AS c
a

> SELECT octet_length('abc'::text::char(4)) AS c
4

#🔬🔬🔬 Assignment

! INSERT INTO c1 VALUES ('ab'::text);
contains:value too long for type character(1)

> INSERT INTO c1 VALUES ('a  '::text);

> INSERT INTO c2 VALUES ('a'::text);

> SELECT DISTINCT octet_length(a) FROM c1
1

> SELECT DISTINCT octet_length(a) FROM c2
2

#🔬🔬 char to varchar

#🔬🔬🔬 Explicit

> SELECT octet_length('a'::text::varchar(2)) AS v
1

> SELECT octet_length('ab'::text::varchar) AS v
2

> SELECT octet_length('ab'::text::varchar(1)) AS v
1

> SELECT octet_length('a   '::text::varchar(3)) AS v
3

> SELECT pg_typeof('ab'::text::varchar(1)) AS p
"character varying"

#🔬🔬🔬 Assignment

> INSERT INTO v VALUES ('a'::text);
> INSERT INTO v VALUES ('ab'::text);
> INSERT INTO v VALUES ('abc'::text);
> INSERT INTO v VALUES ('a  '::text);

> SELECT DISTINCT octet_length(a) AS l FROM v ORDER BY l
l
---
1
2
3

> INSERT INTO v1 VALUES ('a'::text);
! INSERT INTO v1 VALUES ('ab'::text);
contains:value too long for type character varying(1)

! INSERT INTO v1 VALUES ('abc'::text);
contains:value too long for type character varying(1)

> INSERT INTO v1 VALUES ('a  '::text);

> SELECT DISTINCT octet_length(a) AS l FROM v1 ORDER BY l
l
---
1

> INSERT INTO v2 VALUES ('a'::text);
> INSERT INTO v2 VALUES ('ab'::text);
! INSERT INTO v2 VALUES ('abc'::text);
contains:value too long for type character varying(2)

> INSERT INTO v2 VALUES ('a  '::text);

> SELECT DISTINCT octet_length(a) AS l FROM v2 ORDER BY l
l
----
1
2

#🔬🔬 char to text

#🔬🔬🔬 Explicit

> SELECT 'abc'::text::text AS t
t
---
abc

> SELECT octet_length('a  '::text) AS t
t
---
3

#🔬🔬🔬 Assignment

> INSERT INTO t VALUES ('a'::text);
> INSERT INTO t VALUES ('ab'::text);
> INSERT INTO t VALUES ('a  '::text);
> SELECT DISTINCT octet_length(a) AS l FROM t ORDER BY l
l
---
1
2
3
