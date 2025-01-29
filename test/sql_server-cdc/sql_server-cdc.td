# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.


> CREATE SECRET sql_server_pass AS 'sql_server123?'

! CREATE CONNECTION ms TO SQL SERVER (
    HOST sql_server1,
    PORT 1433,
    DATABASE quickstart,
    USER s1,
    PASSWORD SECRET sql_server_pass
  )
contains:Creating a SQL SERVER source is not available

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_sql_server_source = true;

! CREATE CONNECTION ms TO SQL SERVER (
    HOST sql_server1,
    PORT 1433,
    DATABASE quickstart,
    USER s1,
    PASSWORD SECRET sql_server_pass
  )
contains:SQL SERVER not yet supported
