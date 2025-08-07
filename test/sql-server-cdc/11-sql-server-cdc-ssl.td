# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Setup SQL Server state.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_sql_server_source = true;

> CREATE SECRET ssl_ca AS '${arg.ssl-ca}'
> CREATE SECRET alt_ssl_ca AS '${arg.alt-ssl-ca}'
> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

# Create a table that has CDC enabled.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE t1_pk (key_col VARCHAR(20) PRIMARY KEY, val_col VARCHAR(1024));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1_pk', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t1_pk VALUES ('a', 'hello world'), ('b', 'foobar'), ('c', 'anotha one');

CREATE TABLE t2_no_cdc (key_col VARCHAR(20) PRIMARY KEY, val_col VARCHAR(1024));

CREATE TABLE t3_text (value VARCHAR(100));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't3_text', @role_name = 'SA', @supports_net_changes = 0;

# Exercise Materialize.

# Test SSL MODE disabled.

> CREATE CONNECTION no_ssl_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass,
    SSL MODE disabled
  );

> VALIDATE CONNECTION no_ssl_connection;

> SELECT name, type from mz_connections WHERE name = 'no_ssl_connection';
name                    type
---------------------------------------
no_ssl_connection       sql-server


> DROP CONNECTION no_ssl_connection;

# Test SSL MODE required.
> CREATE CONNECTION required_ssl_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass,
    SSL MODE required
  );

> VALIDATE CONNECTION required_ssl_connection;

> SELECT name, type from mz_connections WHERE name = 'required_ssl_connection';
name                      type
---------------------------------------
required_ssl_connection   sql-server

> DROP CONNECTION required_ssl_connection;


# Test SSL MODE verify_ca.''
# verify_ca requires a CA
! CREATE CONNECTION missing_ca TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD SECRET sql_server_pass,
    SSL MODE verify_ca
  );
contains:invalid CONNECTION: SSL MODE 'verify_ca' requires SSL CERTIFICATE AUTHORITY

# verify_ca fails with incorrect CA
! CREATE CONNECTION invalid_ca TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD SECRET sql_server_pass,
    SSL MODE verify_ca,
    SSL CERTIFICATE AUTHORITY SECRET alt_ssl_ca
  );
contains:certificate verify failed

> SELECT count(*) from mz_connections WHERE name = 'invalid_ca';
0


# verify_ca works with correct CA
> CREATE CONNECTION verify_ca_ssl_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD SECRET sql_server_pass,
    SSL MODE verify_ca,
    SSL CERTIFICATE AUTHORITY SECRET ssl_ca
  );

> VALIDATE CONNECTION verify_ca_ssl_connection;

> SELECT name, type from mz_connections WHERE name = 'verify_ca_ssl_connection';
name                       type
---------------------------------------
verify_ca_ssl_connection   sql-server


> DROP CONNECTION verify_ca_ssl_connection;
