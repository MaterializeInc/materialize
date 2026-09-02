# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# A cancellation that arrives while no statement is running must not cancel
# the next statement on that connection. Regression test for SQL-275: within
# an explicit transaction, the per-connection cancel watch of a finished
# staged statement (e.g. EXPLAIN TIMESTAMP) lingered until transaction end,
# so a late cancellation was misattributed to the transaction's next
# statement.

> CREATE TABLE cancel_stale_t (a int)

$ postgres-connect name=main url=postgres://materialize@${testdrive.materialize-sql-addr}
$ postgres-connect name=canceler url=postgres://materialize@${testdrive.materialize-sql-addr}

> CREATE TABLE _cancel_pid (pid int4)

$ postgres-execute connection=main
INSERT INTO _cancel_pid SELECT pg_backend_pid();

$ set-from-sql var=main-pid
SELECT CAST(pid AS text) FROM _cancel_pid;

$ postgres-execute connection=main
BEGIN;

$ postgres-execute connection=main
EXPLAIN TIMESTAMP FOR SELECT * FROM cancel_stale_t;

$ postgres-execute connection=canceler
SELECT pg_cancel_backend(CAST(${main-pid} AS int4));

$ postgres-execute connection=main
EXPLAIN TIMESTAMP FOR SELECT * FROM cancel_stale_t;

$ postgres-execute connection=main
COMMIT;

> DROP TABLE _cancel_pid
> DROP TABLE cancel_stale_t
