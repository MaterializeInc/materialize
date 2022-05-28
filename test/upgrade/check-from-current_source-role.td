# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT name FROM mz_roles;
mz_system
materialize
superuser_login
"space role"

$ postgres-execute connection=postgres://superuser_login:some_bogus_password@${testdrive.materialized-sql-addr}
SELECT 1;

> DROP ROLE superuser_login;

> DROP ROLE "space role";
