# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reproduction for SS-531: renaming a PRIMARY KEY or UNIQUE constraint
# upstream leaves the key Materialize planned with unchanged, but the table
# stalls with a permanent error.
#
# `sp_rename` on a constraint succeeds while CDC is enabled and is not
# recorded in `cdc.ddl_history`, so the rename is only seen by the constraint
# re-read on the next poll.
#
# This file asserts the current behavior. Each stall marked SS-531 is the
# behavior the issue wants changed: a rename should be a non-event.

$ set-sql-timeout duration=60s

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;

CREATE TABLE rename_uq (id INT NOT NULL CONSTRAINT rename_uq_pk PRIMARY KEY, wallet VARCHAR(64) NOT NULL CONSTRAINT rename_uq_wallet UNIQUE);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'rename_uq', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO rename_uq VALUES (1, 'a');

CREATE TABLE rename_pk (id INT NOT NULL CONSTRAINT rename_pk_pk PRIMARY KEY, val VARCHAR(64));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'rename_pk', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO rename_pk VALUES (1, 'a');

CREATE TABLE rename_composite (a INT NOT NULL, b INT NOT NULL, val VARCHAR(64), CONSTRAINT rename_composite_ab UNIQUE (a, b));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'rename_composite', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO rename_composite VALUES (1, 1, 'a');

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_conn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE sql_server_src FROM SQL SERVER CONNECTION sql_server_conn;
> CREATE TABLE rename_uq FROM SOURCE sql_server_src (REFERENCE dbo.rename_uq);
> CREATE TABLE rename_pk FROM SOURCE sql_server_src (REFERENCE dbo.rename_pk);
> CREATE TABLE rename_composite FROM SOURCE sql_server_src (REFERENCE dbo.rename_composite);

> SELECT * FROM rename_uq;
1 a

> SELECT * FROM rename_pk;
1 a

> SELECT * FROM rename_composite;
1 1 a

#
# Rename a UNIQUE constraint. The constraint keeps its object_id and columns.
#

$ sql-server-execute name=sql-server
EXEC sp_rename 'dbo.rename_uq_wallet', 'rename_uq_wallet_v2', 'OBJECT';
INSERT INTO rename_uq VALUES (2, 'b');

# SS-531: the key is unchanged, but constraints are matched by name, so the
# rename is reported as a drop and the table stalls permanently.
! SELECT * FROM rename_uq;
contains:incompatible schema change on dbo.rename_uq: UNIQUE constraint "rename_uq_wallet" (wallet) was dropped upstream

#
# Rename a PRIMARY KEY constraint.
#

$ sql-server-execute name=sql-server
EXEC sp_rename 'dbo.rename_pk_pk', 'rename_pk_pk_v2', 'OBJECT';
INSERT INTO rename_pk VALUES (2, 'b');

# SS-531: same for the primary key.
! SELECT * FROM rename_pk;
contains:incompatible schema change on dbo.rename_pk: PRIMARY KEY constraint "rename_pk_pk" (id) was dropped upstream

#
# Rename a composite UNIQUE constraint.
#

$ sql-server-execute name=sql-server
EXEC sp_rename 'dbo.rename_composite_ab', 'rename_composite_ab_v2', 'OBJECT';
INSERT INTO rename_composite VALUES (2, 2, 'b');

# SS-531: same for a composite key.
! SELECT * FROM rename_composite;
contains:incompatible schema change on dbo.rename_composite: UNIQUE constraint "rename_composite_ab" (a, b) was dropped upstream

> DROP SOURCE sql_server_src CASCADE;
