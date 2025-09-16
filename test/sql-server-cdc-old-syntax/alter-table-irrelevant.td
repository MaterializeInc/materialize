# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test ALTER TABLE -- tables that are created after the source are irrelevant
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> DROP CONNECTION IF EXISTS sql_server_test_connection CASCADE
> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> VALIDATE CONNECTION sql_server_test_connection;

$ sql-server-execute name=sql-server
CREATE TABLE base_table (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'base_table', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO base_table VALUES (1);

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

> SELECT * FROM base_table;
1

# Create "irrelevant" table after the materialized source took a snapshot of the publication
$ sql-server-execute name=sql-server
CREATE TABLE irrelevant_table (f1 INTEGER);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'irrelevant_table', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO irrelevant_table VALUES (1);
INSERT INTO base_table VALUES (2);

# A new table arriving does not prevent queries on existing views for this materialized source
> SELECT * FROM base_table;
1
2

# Alter the irrelevant table and insert a row to force a second relation message that would be incompatible
$ sql-server-execute name=sql-server
ALTER TABLE irrelevant_table ADD f2 varchar(2);
ALTER TABLE irrelevant_table DROP COLUMN f1;
INSERT INTO irrelevant_table VALUES ('ab');

# Query still works because the relation was ignored for being irrelevant
> SELECT * FROM base_table;
1
2

# Recreate the source and views to verify the irrelevant_table is part of the publication,
# which will fail due to extra columns in the upstream for irrelevant_table.
> DROP SOURCE mz_source CASCADE;
! CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;
contains:missing column

# recreate the capture instance to contain the f3 column
$ sql-server-execute name=sql-server
EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'irrelevant_table', @capture_instance = 'all';
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'irrelevant_table', @role_name = 'SA', @supports_net_changes = 0;

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;

# Check the first view still works
> SELECT * FROM base_table;
1
2

# Confirm the second table now has a corresponding view and it has the expected data
> SELECT * FROM irrelevant_table
<null>
ab

# Alter the irrelevant_table now that it is relevant
$ sql-server-execute name=sql-server
ALTER TABLE irrelevant_table ADD f3 char(2);
INSERT INTO irrelevant_table VALUES ('bc', 'de');

> SELECT * FROM base_table;
1
2

> SELECT * FROM irrelevant_table
<null>
ab
bc

# Alter in an incompatible way and ensure replication error does not occur
$ sql-server-execute name=sql-server
ALTER TABLE irrelevant_table DROP COLUMN f2;
INSERT INTO irrelevant_table VALUES ('gh');

! SELECT * FROM irrelevant_table
contains:Incompatible schema change

> SELECT * FROM base_table;
1
2

> DROP SOURCE irrelevant_table CASCADE;
