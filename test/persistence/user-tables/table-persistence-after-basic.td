# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# The table is expected to have its original data after restart
#

> SELECT * FROM t1;
"2011-11-11 11:11:11" "\\x124"
"<null>" "<null>"

> SELECT * FROM v1;
"2011-11-11 11:11:11" "\\x124"
"<null>" "<null>"

#
# Exotic types
#

> SELECT CAST(int4_list AS text), CAST(int4_list_list AS text), CAST(int4_map AS text), CAST(int4_map_map AS text) FROM exotic_types;
"{1,2}" "{{1,2}}" "{a=>1}" "{a=>{a=>1}}"

> SELECT * FROM char_type_quoted;
97

#
# A table that has been dropped previously should not have become resurrected
#

! SELECT * FROM to_be_dropped;
contains:unknown catalog item 'to_be_dropped'

#
# Confirm that same-named tables in different schemas still has different contents
#

> SELECT * FROM schema1.t1;
schema1

> SELECT * FROM schema2.t1;
schema2

#
# Recreated table
#

> INSERT INTO to_be_recreated VALUES (3);

> SELECT * FROM to_be_recreated
2
3

#
# Renamed table
#

> INSERT INTO already_renamed VALUES (3);

> SELECT * FROM already_renamed;
1
2
3
