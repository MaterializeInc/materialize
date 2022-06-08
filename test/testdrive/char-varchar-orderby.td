# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure the ORDER BY semantics of CHAR/VARCHAR is correct regardless
# of whether the ordering is done at the end or within the dataflow.

> CREATE TABLE char_table (f1 CHAR(20), d TEXT);

> INSERT INTO char_table VALUES ('a', 'only a'), ('a ', 'a space'), ('', 'empty string'), (' ', 'only space'), ('b', 'only b'), (' a', 'space a');

> CREATE TABLE varchar_table (f1 VARCHAR(20));

> INSERT INTO varchar_table VALUES ('a'), ('a '), (''), (' '), ('b'), (' a');

#
# First, when sorting happens outside of the dataflow
#

# We validate the order in this painstaking way in order to prevent testdrive's
# internal automatic sorting from interfering

> SELECT * FROM char_table ORDER BY f1 LIMIT 1 OFFSET 0;
"                    " "only space"

# empty string is sorted after the single space, see
# https://github.com/MaterializeInc/materialize/pull/7522#issuecomment-895797576
> SELECT * FROM char_table ORDER BY f1 LIMIT 1 OFFSET 1;
"                    " "empty string"

> SELECT * FROM char_table ORDER BY f1 LIMIT 1 OFFSET 2;
" a                  " "space a"

> SELECT * FROM char_table ORDER BY f1 LIMIT 1 OFFSET 3;
"a                   " "only a"

> SELECT * FROM char_table ORDER BY f1 LIMIT 1 OFFSET 4;
"a                   " "a space"

> SELECT * FROM char_table ORDER BY f1 LIMIT 1 OFFSET 5;
"b                   " "only b"

> SELECT * FROM varchar_table ORDER BY f1 LIMIT 1 OFFSET 0;
""

> SELECT * FROM varchar_table ORDER BY f1 LIMIT 1 OFFSET 1;
" "

> SELECT * FROM varchar_table ORDER BY f1 LIMIT 1 OFFSET 2;
" a"

> SELECT * FROM varchar_table ORDER BY f1 LIMIT 1 OFFSET 3;
"a"

> SELECT * FROM varchar_table ORDER BY f1 LIMIT 1 OFFSET 4;
"a "

> SELECT * FROM varchar_table ORDER BY f1 LIMIT 1 OFFSET 5;
"b"

#
# Second, when sorting happens inside the dataflow
#

# Make sure the plan for those queries actually involve the TopK operator
$ set-regex match=u\d+ replacement=UID

? EXPLAIN SELECT * FROM (SELECT * FROM char_table ORDER BY f1 NULLS FIRST LIMIT 1 OFFSET 0)
Source materialize.public.char_table (UID):
| Project (#0, #1)

Query:
%0 =
| Get materialize.public.char_table (UID)
| TopK group=() order=(#0 asc nulls_first) limit=1 offset=0

> SELECT * FROM (SELECT * FROM char_table ORDER BY f1 LIMIT 1 OFFSET 0)
"                    " "empty string"

> SELECT * FROM (SELECT * FROM char_table ORDER BY f1 LIMIT 1 OFFSET 1)
"                    " "only space"

> SELECT * FROM (SELECT * FROM char_table ORDER BY f1 LIMIT 1 OFFSET 2)
" a                  " "space a"

# 'a' plus space is sorted before the stand-alone 'a', see
# https://github.com/MaterializeInc/materialize/pull/7522#issuecomment-895797576
> SELECT * FROM (SELECT * FROM char_table ORDER BY f1 LIMIT 1 OFFSET 3)
"a                   " "a space"

> SELECT * FROM (SELECT * FROM char_table ORDER BY f1 LIMIT 1 OFFSET 4)
"a                   " "only a"

> SELECT * FROM (SELECT * FROM char_table ORDER BY f1 LIMIT 1 OFFSET 5)
"b                   " "only b"
