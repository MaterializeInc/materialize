# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# These goal of these tests is to exercise what isn't supported in the SQL
# Server source and that we properly handle these scenarios.

# Setup SQL Server state.
#
# Create a table that has CDC enabled.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE table_xml (good varchar(128), bad xml);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_xml', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_xml VALUES ('i work', NULL), ('maybe me', NULL);

CREATE TABLE table_image (good int, bad image);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_image', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_image VALUES (42, NULL);

CREATE TABLE table_geography (good int, bad geography);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_geography', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_geography VALUES (42, NULL);

CREATE TABLE table_geometry (good int, bad geometry);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_geometry', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_geometry VALUES (42, NULL);

CREATE TABLE table_varbinary_max (good int, bad varbinary(max));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'table_varbinary_max', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO table_varbinary_max VALUES (42, NULL);


# Exercise Materialize.

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_test_50_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

! CREATE SOURCE table_xml_source
  FROM SQL SERVER CONNECTION sql_server_test_50_connection
  FOR TABLES (dbo.table_xml);
contains: SQL SERVER source validation: column dbo.table_xml.bad of type xml is not supported

> CREATE SOURCE table_xml_source
  FROM SQL SERVER CONNECTION sql_server_test_50_connection (
    EXCLUDE COLUMNS (dbo.table_xml.bad)
  )
  FOR TABLES (dbo.table_xml);

> SHOW COLUMNS FROM table_xml;
good true "character varying" ""

> SELECT * FROM table_xml;
"i work"
"maybe me"

> DROP SOURCE table_xml_source CASCADE;

! CREATE SOURCE table_image_source
  FROM SQL SERVER CONNECTION sql_server_test_50_connection
  FOR TABLES (dbo.table_image);
contains: SQL SERVER source validation: column dbo.table_image.bad of type image is not supported

! CREATE SOURCE table_geography_source
  FROM SQL SERVER CONNECTION sql_server_test_50_connection
  FOR TABLES (dbo.table_geography);
contains: SQL SERVER source validation: column dbo.table_geography.bad of type geography is not supported

! CREATE SOURCE table_geometry_source
  FROM SQL SERVER CONNECTION sql_server_test_50_connection
  FOR TABLES (dbo.table_geometry);
contains: SQL SERVER source validation: column dbo.table_geometry.bad of type geometry is not supported

! CREATE SOURCE table_varbinary_max_source
  FROM SQL SERVER CONNECTION sql_server_test_50_connection
  FOR TABLES (dbo.table_varbinary_max);
contains: SQL SERVER source validation: column dbo.table_varbinary_max.bad of type varbinary is not supported


## same tests for source tables

# Creating a source with no subsources should succeed
> CREATE SOURCE ms_src
  FROM SQL SERVER CONNECTION sql_server_test_50_connection;

! CREATE TABLE table_xml
  FROM SOURCE ms_src (REFERENCE dbo.table_xml);
contains: SQL SERVER source validation: column dbo.table_xml.bad of type xml is not supported

> CREATE TABLE table_xml
  FROM SOURCE ms_src (REFERENCE dbo.table_xml)
  WITH (
    EXCLUDE COLUMNS (bad)
  );

> SHOW COLUMNS FROM table_xml;
good true "character varying" ""

> SELECT * FROM table_xml;
"i work"
"maybe me"

! CREATE TABLE table_image
  FROM SOURCE ms_src (REFERENCE dbo.table_image);
contains: SQL SERVER source validation: column dbo.table_image.bad of type image is not supported

! CREATE TABLE table_geography
  FROM SOURCE ms_src (REFERENCE dbo.table_geography);
contains: SQL SERVER source validation: column dbo.table_geography.bad of type geography is not supported

! CREATE TABLE table_geometry
  FROM SOURCE ms_src (REFERENCE dbo.table_geometry);
contains: SQL SERVER source validation: column dbo.table_geometry.bad of type geometry is not supported

! CREATE TABLE table_varbinary_max
  FROM SOURCE ms_src (REFERENCE dbo.table_varbinary_max);
contains: SQL SERVER source validation: column dbo.table_varbinary_max.bad of type varbinary is not supported

> DROP SOURCE ms_src CASCADE;
