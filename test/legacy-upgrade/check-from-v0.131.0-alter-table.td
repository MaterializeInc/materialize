# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SHOW CREATE TABLE alter_table_t1;
"materialize.public.alter_table_t1" "CREATE TABLE \"materialize\".\"public\".\"alter_table_t1\" (\"a\" \"pg_catalog\".\"int4\", \"b\" \"pg_catalog\".\"text\" VERSION ADDED 1, \"c\" \"pg_catalog\".\"int8\" VERSION ADDED 2)"

> SELECT * FROM alter_table_t1 ORDER BY a DESC;
<null> <null> <null>
600    orange <null>
500    apple  101
400    world  <null>
300    <null> <null>
200    hello  <null>
100    <null> <null>

> SELECT * FROM alter_table_t1_v1 ORDER BY a DESC;
<null>
600
500
400
300
200
100

> SELECT * FROM alter_table_t1_mv1 ORDER BY a DESC;
<null>
600
500
400
300
200
100

> SELECT * FROM alter_table_t1_v2 ORDER BY a DESC;
<null> <null>
600    orange
500    apple
400    world
300    <null>
200    hello
100    <null>

> SELECT * FROM alter_table_t1_mv2 ORDER BY a DESC;
<null>
600
500
400
300
200
100
