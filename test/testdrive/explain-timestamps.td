# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-regex match=(\d{13}|u\d{1,3}|\(\d+-\d\d-\d\d\s\d\d:\d\d:\d\d.\d\d\d\)|true|false) replacement=<>

> CREATE TABLE t1 (a INT);

> CREATE TABLE t2 (a INT);

# Strict serializable doesn't look at every object in the same time domain
> SET TRANSACTION_ISOLATION = 'STRICT SERIALIZABLE';
> EXPLAIN TIMESTAMP FOR SELECT * FROM t1
"          query timestamp: <> <>\n                    since:[<> <>]\n                    upper:[<> <>]\n         global timestamp: <> <>\n  can respond immediately: <>\n\nsource materialize.public.t1 (<>, storage):\n read frontier:[<> <>]\nwrite frontier:[<> <>]\n"

# Serializable does look at every object in the same time domain
> SET TRANSACTION_ISOLATION = 'SERIALIZABLE';
> EXPLAIN TIMESTAMP FOR SELECT * FROM t1
"          query timestamp: <> <>\n                    since:[<> <>]\n                    upper:[<> <>]\n         global timestamp: <> <>\n  can respond immediately: <>\n\nsource materialize.public.t1 (<>, storage):\n read frontier:[<> <>]\nwrite frontier:[<> <>]\n\nsource materialize.public.t2 (<>, storage):\n read frontier:[<> <>]\nwrite frontier:[<> <>]\n"
