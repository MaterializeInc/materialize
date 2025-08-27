# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password};Database=test

$ sql-server-execute name=sql-server
USE test;
DROP TABLE IF EXISTS ten;
CREATE TABLE ten (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'ten', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO ten VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (f1 INTEGER, f2 NVARCHAR(256));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't0', @role_name = 'SA', @supports_net_changes = 0;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (f1 INTEGER, f2 NVARCHAR(256));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't1', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t1 SELECT a1.f1 + a6.f1 , N'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (f1 INTEGER, f2 NVARCHAR(256));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 't2', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO t2 SELECT a1.f1 + a6.f1 , N'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

DROP TABLE IF EXISTS alter_fail_drop_col;
# SQL Server CDC will continue to replicate a dropped NULL column as NULL so dropped column must be NOT NULL.
CREATE TABLE alter_fail_drop_col (f1 INTEGER NOT NULL, f2 INTEGER NOT NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_fail_drop_col ', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO alter_fail_drop_col VALUES (1, 1);

DROP TABLE IF EXISTS alter_fail_drop_constraint;
CREATE TABLE alter_fail_drop_constraint (f1 INTEGER NOT NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'alter_fail_drop_constraint', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO alter_fail_drop_constraint VALUES (1);
