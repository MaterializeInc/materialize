# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET allow_real_time_recency = true

$ set-regex match=(\d{13}|u\d{1,3}|\(\d+-\d\d-\d\d\s\d\d:\d\d:\d\d.\d\d\d\)) replacement=<>

> CREATE TABLE t1 (a INT);

# Strict serializable doesn't look at every object in the same time domain
> SET TRANSACTION_ISOLATION = 'STRICT SERIALIZABLE';
> EXPLAIN TIMESTAMP FOR SELECT * FROM t1
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: true\n                       timeline: Some(EpochMilliseconds)\n              session wall time: <> <>\n\nsource materialize.public.t1 (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n"

# Serializable also doesn't look at every object in the same time domain
> SET TRANSACTION_ISOLATION = 'SERIALIZABLE';
> EXPLAIN TIMESTAMP FOR SELECT * FROM t1
"                query timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: true\n                       timeline: Some(EpochMilliseconds)\n              session wall time: <> <>\n\nsource materialize.public.t1 (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n"

# Real time recency shouldn't break anything
> SET TRANSACTION_ISOLATION = 'STRICT SERIALIZABLE';
> SET REAL_TIME_RECENCY TO TRUE
> EXPLAIN TIMESTAMP FOR SELECT * FROM t1
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: true\n                       timeline: Some(EpochMilliseconds)\n              session wall time: <> <>\n\nsource materialize.public.t1 (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n"
