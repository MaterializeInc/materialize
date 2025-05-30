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

$ set-regex match=(s\d+|\d{13}|u\d+|\(\d+-\d\d-\d\d\s\d\d:\d\d:\d\d\.\d\d\d\)|true|false|\((\d+)\)) replacement=<>

> CREATE TABLE t1 (a INT);

# Strict serializable doesn't look at every object in the same time domain
> SET TRANSACTION_ISOLATION = 'STRICT SERIALIZABLE';
> EXPLAIN TIMESTAMP FOR SELECT * FROM t1
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n              session wall time: <> <>\n\nsource materialize.public.t1 (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n\nbinding constraints:\nlower:\n  (IsolationLevel(StrictSerializable)): [<> <>]\n"

# Serializable also doesn't look at every object in the same time domain
> SET TRANSACTION_ISOLATION = 'SERIALIZABLE';
> EXPLAIN TIMESTAMP FOR SELECT * FROM t1
"                query timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n              session wall time: <> <>\n\nsource materialize.public.t1 (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n\nbinding constraints:\nlower:\n  (StorageInput([User<>])): [<> <>]\n"


# Real time recency shouldn't break anything
> SET TRANSACTION_ISOLATION = 'STRICT SERIALIZABLE';
> SET REAL_TIME_RECENCY TO TRUE
> EXPLAIN TIMESTAMP FOR SELECT * FROM t1
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\n    real time recency timestamp:             0 <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n              session wall time: <> <>\n\nsource materialize.public.t1 (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n\nbinding constraints:\nlower:\n  (IsolationLevel(StrictSerializable)): [<> <>]\n"

# Test autorouting explain timestamp queries
> EXPLAIN TIMESTAMP FOR SELECT * from mz_internal.mz_cluster_replica_metrics
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n              session wall time: <> <>\n\nsource mz_internal.mz_cluster_replica_metrics_ind (<>, compute):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n\nbinding constraints:\nlower:\n  (IsolationLevel(StrictSerializable)): [<> <>]\n"
