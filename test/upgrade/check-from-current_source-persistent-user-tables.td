# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# If mz_persist::gen::ProtoMeta::ENCODING_VERSION is changed, the version of this file needs to change to "current_source"

> SELECT COUNT(*) = 1 FROM persistent_user_table;
true

> SELECT f1 FROM persistent_user_table;
1234

> SELECT LENGTH(f2) = 10 * 1024 * 1024 FROM persistent_user_table;
true

# Check that the table can still be inserted into

> INSERT INTO persistent_user_table VALUES (1234, REPEAT('x', 1024 * 1024 * 10));

> SELECT LENGTH(f2) = 10 * 1024 * 1024 FROM persistent_user_table;
true
true

> SELECT * FROM persistent_user_table_view1;
12340
12340

> SELECT * FROM persistent_user_table_view2;
123400
123400

> SELECT * FROM persistent_schema.persistent_user_table;
abc

> SELECT * FROM persistent_user_table_txns;
1
2
3
4
5
11
12
13
14
15
21
22
23
24
25

> SELECT * FROM persistent_user_table_renamed;
1234

> INSERT INTO persistent_user_table1_for_sink VALUES (3);

> INSERT INTO persistent_user_table2_for_sink VALUES (3);

$ set-regex match=\d{13} replacement=<TIMESTAMP>

$ kafka-verify format=avro sink=materialize.public.persistent_user_table1_sink sort-messages=true
{"before":null,"after":{"row":{"f1":{"int":1}}},"transaction":{"id":"<TIMESTAMP>"}}
{"before":null,"after":{"row":{"f1":{"int":2}}},"transaction":{"id":"<TIMESTAMP>"}}
{"before":null,"after":{"row":{"f1":{"int":3}}},"transaction":{"id":"<TIMESTAMP>"}}

$ kafka-verify format=avro sink=materialize.public.persistent_user_table2_sink sort-messages=true
{"before":null,"after":{"row":{"f1":{"int":1}}},"transaction":{"id":"<TIMESTAMP>"}}
{"before":null,"after":{"row":{"f1":{"int":2}}},"transaction":{"id":"<TIMESTAMP>"}}
{"before":null,"after":{"row":{"f1":{"int":3}}},"transaction":{"id":"<TIMESTAMP>"}}
