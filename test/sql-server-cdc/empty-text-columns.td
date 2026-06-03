# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for ss-159: an empty `TEXT COLUMNS = ()` on a
# `CREATE TABLE .. FROM SOURCE` statement used to trip a soft-panic during
# purification.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE empty_text_cols (key_col VARCHAR(20) PRIMARY KEY, val_col VARCHAR(1024));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'empty_text_cols', @role_name = 'SA', @supports_net_changes = 0;

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION empty_text_cols_conn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE empty_text_cols_src FROM SQL SERVER CONNECTION empty_text_cols_conn;

! CREATE TABLE empty_text_cols FROM SOURCE empty_text_cols_src (REFERENCE dbo.empty_text_cols) WITH (TEXT COLUMNS = ());
contains:invalid TEXT COLUMNS option value: cannot be empty

> DROP SOURCE empty_text_cols_src CASCADE;
