# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test creating a SQL Server source using SSH and SSL options


> CREATE SECRET ssl_ca AS '${arg.ssl-ca}'
> CREATE SECRET alt_ssl_ca AS '${arg.alt-ssl-ca}'
> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'


# Basic TLS
> CREATE CONNECTION sql_server_ssl TO SQL SERVER (
    HOST 'sql-server',
    USER '${arg.default-sql-server-user}',
    DATABASE 'test',
    PASSWORD SECRET sql_server_pass,
    SSH TUNNEL thancred,
    SSL MODE required
  );
> DROP CONNECTION sql_server_ssl;

# TLS with custom CA
> CREATE CONNECTION sql_server_ssl TO SQL SERVER (
    HOST 'sql-server',
    USER '${arg.default-sql-server-user}',
    DATABASE 'test',
    PASSWORD SECRET sql_server_pass,
    SSL MODE verify_ca,
    SSL CERTIFICATE AUTHORITY SECRET ssl_ca,
    SSH TUNNEL thancred
  );

# Create a table that has CDC enabled.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE tls_data (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'tls_data', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO tls_data VALUES (1), (2);

> BEGIN
> CREATE SOURCE sql_server_source_ssl FROM SQL SERVER
  CONNECTION sql_server_ssl;

> CREATE TABLE tls_data FROM SOURCE sql_server_source_ssl (REFERENCE tls_data);
> COMMIT

> SELECT COUNT(*) FROM tls_data;
2
