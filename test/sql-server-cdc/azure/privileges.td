# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Provision a least-privilege Materialize user following the steps in the Azure
# SQL Database ingestion guide, and validate that it can ingest. Namespaced
# `mz_ci_*` names are used in place of the guide's `materialize` /
# `materialize_role` to avoid colliding with real objects on a shared database.
# The provisioning mirrors the guide:
#   - a contained database user with a password,
#   - a gating role for the capture instances,
#   - db_datareader for SELECT on the source and cdc change tables, and
#   - VIEW DATABASE STATE in place of the server-scoped VIEW SERVER STATE.

$ sql-server-connect name=sql-server
server=tcp:${arg.azure-sql-host},${arg.azure-sql-port};Encrypt=true;TrustServerCertificate=true;User ID={${arg.azure-sql-user}};Password={${arg.azure-sql-password}};Database=${arg.azure-sql-database}

$ sql-server-execute name=sql-server
CREATE USER mz_ci_materialize WITH PASSWORD = '${arg.mz-least-priv-password}';
CREATE ROLE mz_ci_role;
ALTER ROLE mz_ci_role ADD MEMBER mz_ci_materialize;
ALTER ROLE db_datareader ADD MEMBER mz_ci_materialize;
GRANT VIEW DATABASE STATE TO mz_ci_materialize;

# Source table, with CDC gated by the role created above.
$ sql-server-execute name=sql-server
CREATE TABLE mz_azure_priv (key_col int PRIMARY KEY, val_col int);
INSERT INTO mz_azure_priv VALUES (1, 10), (2, 20), (3, 30);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'mz_azure_priv', @role_name = 'mz_ci_role', @supports_net_changes = 0;

> CREATE SECRET IF NOT EXISTS mz_least_priv_pass AS '${arg.mz-least-priv-password}'

> CREATE CONNECTION azure_priv_conn TO SQL SERVER (
    HOST '${arg.azure-sql-host}',
    PORT ${arg.azure-sql-port},
    DATABASE '${arg.azure-sql-database}',
    USER 'mz_ci_materialize',
    PASSWORD = SECRET mz_least_priv_pass,
    SSL MODE required
  );

> VALIDATE CONNECTION azure_priv_conn;

> CREATE SOURCE mz_azure_priv_src
  FROM SQL SERVER CONNECTION azure_priv_conn;

> CREATE TABLE mz_azure_priv FROM SOURCE mz_azure_priv_src (REFERENCE dbo.mz_azure_priv);

> SELECT * FROM mz_azure_priv;
1 10
2 20
3 30

# Negative check: a user provisioned the same way but denied SELECT on the
# capture table must be rejected. With the new syntax the table-level privilege
# check runs when the table is added, so the bare CREATE SOURCE succeeds and the
# CREATE TABLE ... FROM SOURCE is what fails. This confirms the permission check
# still fires on Azure.
$ sql-server-execute name=sql-server
CREATE USER mz_ci_denied WITH PASSWORD = '${arg.mz-least-priv-password}';
ALTER ROLE mz_ci_role ADD MEMBER mz_ci_denied;
ALTER ROLE db_datareader ADD MEMBER mz_ci_denied;
GRANT VIEW DATABASE STATE TO mz_ci_denied;
DENY SELECT ON OBJECT::cdc.dbo_mz_azure_priv_CT(__$start_lsn) TO mz_ci_denied;

> CREATE CONNECTION azure_denied_conn TO SQL SERVER (
    HOST '${arg.azure-sql-host}',
    PORT ${arg.azure-sql-port},
    DATABASE '${arg.azure-sql-database}',
    USER 'mz_ci_denied',
    PASSWORD = SECRET mz_least_priv_pass,
    SSL MODE required
  );

> CREATE SOURCE mz_azure_denied_src
  FROM SQL SERVER CONNECTION azure_denied_conn;

! CREATE TABLE mz_azure_denied_tbl FROM SOURCE mz_azure_denied_src (REFERENCE dbo.mz_azure_priv);
contains:insufficient permissions

# Clean up the Materialize objects and the provisioned SQL Server principals.
# The happy-path source (mz_azure_src) is intentionally left in place for the
# restart check.
> DROP SOURCE mz_azure_priv_src CASCADE;
> DROP SOURCE mz_azure_denied_src CASCADE;
> DROP CONNECTION azure_denied_conn;
> DROP CONNECTION azure_priv_conn;

$ sql-server-execute name=sql-server split-lines=false
DROP USER IF EXISTS mz_ci_denied;
DROP USER IF EXISTS mz_ci_materialize;
DROP ROLE IF EXISTS mz_ci_role;
IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.mz_azure_priv'))
    EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'mz_azure_priv', @capture_instance = 'all';
DROP TABLE IF EXISTS dbo.mz_azure_priv;
