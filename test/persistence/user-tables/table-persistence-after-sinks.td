# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> INSERT INTO sink_table VALUES (3)

> SELECT COUNT(*) FROM sink_table
3

$ set-regex match=\d{13} replacement=<TIMESTAMP>

$ kafka-verify format=avro sink=materialize.public.sink_sink sort-messages=true
{"before":null, "after":{"row":{"f1":{"int":1}}}, "transaction":{"id":"<TIMESTAMP>"}}
{"before":null, "after":{"row":{"f1":{"int":2}}}, "transaction":{"id":"<TIMESTAMP>"}}
{"before":null, "after":{"row":{"f1":{"int":3}}}, "transaction":{"id":"<TIMESTAMP>"}}
