# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify the built-in postgres-execute connections that work without a prior
# postgres-connect registration.

# mz_system connects as the mz_system user via the internal SQL address.
$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_tables = 12345;

> SHOW max_tables
12345

# The lazily created client is cached, so repeated uses share a session, just
# like a connection registered via postgres-connect.
$ postgres-execute connection=materialize
CREATE TABLE builtin_connection_t (f1 INT);
BEGIN;
INSERT INTO builtin_connection_t VALUES (1);

$ postgres-execute connection=materialize
COMMIT;

> SELECT * FROM builtin_connection_t
1

# An explicit postgres-connect with the same name still works and takes
# precedence over the built-in.
$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
ALTER SYSTEM SET max_tables = 54321;

> SHOW max_tables
54321

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET max_tables;
