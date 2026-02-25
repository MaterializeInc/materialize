# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_rbac_checks = true;
CREATE ROLE cancel_owner_${testdrive.seed} LOGIN PASSWORD 'ownerpass';
CREATE TABLE pid_store (pid int);
GRANT INSERT ON TABLE pid_store TO cancel_owner_${testdrive.seed};
GRANT SELECT ON TABLE pid_store TO materialize;

$ postgres-connect name=owner url=postgres://cancel_owner_${testdrive.seed}:ownerpass@${testdrive.materialize-sql-addr}

# Have the owner store its own backend PID in a table,
# so the default (materialize) connection can read it.
$ postgres-execute connection=owner
INSERT INTO pid_store VALUES (pg_backend_pid());

$ set-from-sql var=owner-backend-pid
SELECT CAST(pid AS text) FROM pid_store;

! SELECT pg_cancel_backend(CAST(${owner-backend-pid} AS int4));
contains:must be a member of "cancel_owner_${testdrive.seed}"

$ postgres-execute connection=mz_system
GRANT cancel_owner_${testdrive.seed} TO materialize;

> SELECT pg_cancel_backend(CAST(${owner-backend-pid} AS int4));
true

$ postgres-execute connection=mz_system
REVOKE cancel_owner_${testdrive.seed} FROM materialize;
