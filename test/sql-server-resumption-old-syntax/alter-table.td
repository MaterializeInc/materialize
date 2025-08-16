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
ALTER TABLE alter_fail_drop_col DROP COLUMN f2;
INSERT INTO alter_fail_drop_col VALUES (2);

ALTER TABLE alter_fail_drop_constraint ALTER COLUMN f1 INTEGER NULL;
INSERT INTO alter_fail_drop_constraint VALUES (NULL);
