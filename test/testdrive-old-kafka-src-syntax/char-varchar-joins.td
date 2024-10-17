# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Confirm that joins and the equality comparisons inherent to join
# procesisng work properly with the CHAR data type and trailing spaces.
#

> CREATE TABLE char_table (f1 CHAR(20));

> INSERT INTO char_table VALUES ('a'), ('a '), ('a  '), (''), ('  '), (NULL);

> CREATE TABLE varchar_table (f1 VARCHAR(20));

> INSERT INTO varchar_table VALUES ('a'), ('a '), ('a  '), (''), ('  '), (NULL);

> CREATE TABLE text_table (f1 TEXT);

> INSERT INTO text_table VALUES ('a'), ('a '), ('a  '), (''), ('  '), (NULL);

> SELECT * FROM char_table, varchar_table WHERE char_table.f1 = varchar_table.f1;
"                    " ""
"                    " ""
"a                   " "a"
"a                   " "a"
"a                   " "a"
"                    " "  "
"                    " "  "
"a                   " "a "
"a                   " "a "
"a                   " "a "
"a                   " "a  "
"a                   " "a  "
"a                   " "a  "

> SELECT * FROM char_table AS a1, char_table AS a2 WHERE a1.f1 = a2.f1;
"                    " "                    "
"                    " "                    "
"                    " "                    "
"                    " "                    "
"a                   " "a                   "
"a                   " "a                   "
"a                   " "a                   "
"a                   " "a                   "
"a                   " "a                   "
"a                   " "a                   "
"a                   " "a                   "
"a                   " "a                   "
"a                   " "a                   "

# Comparisons with TEXT are strict byte-by-byte

> SELECT * FROM char_table, text_table WHERE char_table.f1 = text_table.f1;
"                    " ""
"                    " ""
"a                   " "a"
"a                   " "a"
"a                   " "a"
