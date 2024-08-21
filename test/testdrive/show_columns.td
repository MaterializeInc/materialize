# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-connect name=mz_support url=postgres://mz_support:materialize@${testdrive.materialize-internal-sql-addr}
$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

> CREATE TABLE t(a int)

> SHOW COLUMNS FROM t
a true integer ""

$ postgres-execute connection=mz_support
SHOW COLUMNS FROM t

# enable RBAC

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_rbac_checks=true;

$ postgres-execute connection=mz_system
CREATE TABLE priv(a int)

$ postgres-execute connection=mz_system
REVOKE SELECT ON TABLE priv FROM materialize

! SELECT * from priv;
contains:permission denied for TABLE "materialize.public.priv"

> SHOW COLUMNS FROM priv
a true integer ""

# Make sure we can't exfiltrate data by supplying a where expression
# that references the underlying table
! SHOW COLUMNS FROM priv WHERE char_length(name) = any (select * from priv)
contains:permission denied for TABLE "materialize.public.priv"

$ postgres-execute connection=mz_support
SHOW COLUMNS FROM priv
