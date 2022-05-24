# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


#
# Make sure the distinct operator inside a detaflow operates correctly
# with respect to CHAR/VARCHAR, especially in the presence of trailing
# spaces.
#

> SELECT 'a '::char(5) UNION DISTINCT SELECT 'a  '::char(5);
"a    "

# NOTE(benesch): this does not match PostgreSQL, which will choose to preserve
# the trailing spaces of *one* of the datums arbitrarily. We can't allow such
# nondeterminism in our dataflow layer, so we choose to strip all trailing
# spaces.
> SELECT 'a '::char(5) UNION DISTINCT SELECT 'a  '::char(10);
"a"

# Ditto.
> SELECT 'a '::char(10) UNION DISTINCT SELECT 'a  '::char(5);
"a"

> SELECT 'a '::varchar(5) UNION DISTINCT SELECT 'a  '::varchar(5);
"a "
"a  "

> SELECT 'a '::varchar(5) UNION DISTINCT SELECT 'a '::char(5);
a
"a "

> SELECT 'a '::varchar(5) UNION DISTINCT SELECT 'a '::text;
"a "

# This again does not match PostgreSQL for the reason described above.
> SELECT 'a '::char(5) UNION DISTINCT SELECT 'a '::text;
"a"

> CREATE TABLE char_table (f1 CHAR(20));

> INSERT INTO char_table VALUES ('a'), ('a '), ('a  '), (''), ('  '), (NULL);

> CREATE TABLE varchar_table (f1 VARCHAR(20));

> INSERT INTO varchar_table VALUES ('a'), ('a '), ('a  '), (''), ('  '), (NULL);

> CREATE TABLE string_table (f1 STRING);

> INSERT INTO string_table VALUES ('a'), ('a '), ('a  '), (''), ('  '), (NULL);

> SELECT DISTINCT f1 FROM char_table;
<null>
"                    "
"a                   "

> SELECT COUNT(DISTINCT f1) FROM char_table;
2

> SELECT f1 FROM char_table GROUP BY f1;
<null>
"                    "
"a                   "


> SELECT f1 FROM char_table UNION DISTINCT SELECT f1 FROM char_table;
<null>
"                    "
"a                   "

> SELECT f1, LENGTH(f1) , c FROM (SELECT f1, COUNT(*) AS c FROM char_table GROUP BY f1 ORDER BY f1 LIMIT 3);
<null> <null> 1
"                    " 0 2
"a                   " 1 3

> SELECT DISTINCT f1 FROM varchar_table;
""
"  "
"<null>"
"a"
"a "
"a  "

> SELECT f1, LENGTH(f1) , c FROM (SELECT f1, COUNT(*) AS c FROM varchar_table GROUP BY f1 ORDER BY f1 LIMIT 3);
"" 0 1
"  " 2 1
"a" 1 1

> SELECT DISTINCT f1 FROM string_table;
""
"  "
"<null>"
"a"
"a "
"a  "

> SELECT f1, LENGTH(f1) , c FROM (SELECT f1, COUNT(*) AS c FROM string_table GROUP BY f1 ORDER BY f1 LIMIT 3);
"" 0 1
"  " 2 1
"a" 1 1
