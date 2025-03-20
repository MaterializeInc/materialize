# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-regex match=\d{13,20} replacement=<TIMESTAMP>

$ postgres-connect name=mz2 url=postgres://materialize@${testdrive.materialize-sql-addr}

> DROP TABLE IF EXISTS t
> CREATE TABLE t (c int);

$ set-from-sql var=backend-pid
SELECT CAST(pg_backend_pid() AS text);

> BEGIN
> DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM t)

$ postgres-execute connection=mz2
INSERT INTO t VALUES (1);

> FETCH 1 c
<TIMESTAMP> 1 1

$ postgres-execute connection=mz2
SELECT pg_cancel_backend(CAST(${backend-pid} AS int4));

! FETCH 1 c
contains:canceling statement due to user request
