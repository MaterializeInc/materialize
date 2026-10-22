# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Perform various schema updates to the upstream table

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password}

$ sql-server-execute name=sql-server
USE test;
CREATE TABLE foo (id BIGINT IDENTITY(1,1), name VARCHAR(16), value VARCHAR(32));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foo', @role_name = 'SA', @supports_net_changes = 0;

INSERT INTO foo VALUES ('a', 'apple'), ('b', 'banana');


> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> CREATE CONNECTION sql_server_conn TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> CREATE SOURCE sql_server_src
  FROM SQL SERVER CONNECTION sql_server_conn;
> CREATE TABLE foo1 FROM SOURCE sql_server_src (REFERENCE foo);

> SELECT * FROM foo1;
1 a apple
2 b banana

$ sql-server-execute name=sql-server
ALTER TABLE foo ADD meta_col VARCHAR(32);
INSERT INTO foo VALUES ('c', 'cherry', 'wild');

> SELECT * FROM foo1;
1 a apple
2 b banana
3 c cherry

$ sql-server-execute name=sql-server
ALTER TABLE foo ALTER COLUMN meta_col VARCHAR(64);
INSERT INTO foo VALUES ('d', 'date', 'ajwa');

# Altering the newly added `meta_col` column does not brick `foo1` subsource because `foo1` is
# not following/replicating `meta_col`, so schema updates involving it are inconsequential.
> SELECT * FROM foo1;
1 a apple
2 b banana
3 c cherry
4 d date

> DROP TABLE foo1;

# The upstream table `foo` was initially created without the `meta_col` column, as was the CDC
# table.  In the current state, the upstream table now has the `meta_col` column, but the CDC table
# does not.  Source creation must fail in this case as the snapshot and table description would
# contain this column, but the CDC events would not.  Rewinding to the initial LSN could result in
# negative accumulations.


! CREATE TABLE foo2 FROM SOURCE sql_server_src (REFERENCE dbo.foo);
contains:missing columns

> CREATE TABLE foo2 FROM SOURCE sql_server_src (REFERENCE dbo.foo) WITH (EXCLUDE COLUMNS = (meta_col));
> SELECT * FROM foo2;
1 a apple
2 b banana
3 c cherry
4 d date

# To support the new column without downtime, we can create a new CDC capture instance for the table
# in the upstream SQL server db that will contain the new column.

$ sql-server-execute name=sql-server
USE test;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foo', @role_name = 'SA', @capture_instance = 'dbo_foo2', @supports_net_changes = 0;

> CREATE SOURCE sql_server_src2
  FROM SQL SERVER CONNECTION sql_server_conn;

> CREATE SCHEMA v2;

> CREATE TABLE v2.foo2 FROM SOURCE sql_server_src2 (REFERENCE dbo.foo);

> SELECT * FROM v2.foo2;
1 a apple <null>
2 b banana <null>
3 c cherry wild
4 d date ajwa

$ sql-server-execute name=sql-server
ALTER TABLE foo DROP COLUMN id;

# DROP COLUMN stalls the source
! SELECT * FROM foo2;
contains:Incompatible schema change

> DROP TABLE foo2;

# The id column was dropped, so it exists in the CDC table, but not in the upstream.
# The SQL Server source decodes rows using column name instead of index, and since the id column
# will not exist in the table description, it will not be read from the CDC rows.
> CREATE TABLE foo3 FROM SOURCE sql_server_src (REFERENCE dbo.foo) WITH (EXCLUDE COLUMNS = (meta_col));
> SELECT * FROM foo3;
a apple
b banana
c cherry
d date

> DROP TABLE foo3;

$ sql-server-execute name=sql-server
USE test;
EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'foo', @capture_instance = 'all';
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'foo', @role_name = 'SA', @supports_net_changes = 0;

> CREATE TABLE foo4 FROM SOURCE sql_server_src (REFERENCE dbo.foo);
> SELECT * FROM foo4;
a apple <null>
b banana <null>
c cherry wild
d date ajwa

$ sql-server-execute name=sql-server
INSERT INTO foo VALUES ('e', 'elderberry', 'montypython');

> SELECT * FROM foo4;
a apple <null>
b banana <null>
c cherry wild
d date ajwa
e elderberry montypython

$ sql-server-execute name=sql-server
CREATE TABLE bar (id BIGINT IDENTITY(1,1), name VARCHAR(16), value VARCHAR(32), age BIGINT, location VARCHAR(32), meta_col VARCHAR(32));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'bar', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO bar VALUES ('a', 'apple', 5, 'orchard', 'blah'), ('b', 'banana', 7, 'tree', 'blahblah');

> CREATE TABLE bar1 FROM SOURCE sql_server_src (REFERENCE dbo.bar) WITH (EXCLUDE COLUMNS = (meta_col, name));

$ sql-server-execute name=sql-server
ALTER TABLE bar DROP COLUMN meta_col;

# Dropping an excluded column is okay
> SELECT * FROM bar1;
1 apple 5 orchard
2 banana 7 tree

$ sql-server-execute name=sql-server
ALTER TABLE bar DROP COLUMN name, location;

# Dropping a non-excluded column stalls the source
! SELECT * FROM bar1;
contains:Incompatible schema change

# Constraint changes are not recorded in the CDC DDL history, so they are
# detected by re-reading the table's constraints on each poll. A dropped
# PRIMARY KEY or UNIQUE constraint stalls the table, because Materialize uses
# the recorded key to plan queries. SQL Server does not allow dropping a PRIMARY
# KEY while CDC is enabled, so the drop scenarios use UNIQUE constraints.
# A recorded UNIQUE constraint is dropped.
$ sql-server-execute name=sql-server
CREATE TABLE unique_table (id INT NOT NULL CONSTRAINT unique_table_primary_key PRIMARY KEY, wallet VARCHAR(64) NOT NULL CONSTRAINT unique_table_wallet_unique UNIQUE);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'unique_table', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO unique_table VALUES (1, 'a');

> CREATE TABLE unique_table FROM SOURCE sql_server_src (REFERENCE dbo.unique_table);

> SELECT * FROM unique_table;
1 a

$ sql-server-execute name=sql-server
ALTER TABLE unique_table DROP CONSTRAINT unique_table_wallet_unique;

! SELECT * FROM unique_table;
contains:incompatible schema change on dbo.unique_table: UNIQUE constraint "unique_table_wallet_unique" (wallet) was dropped upstream

# Rows written after the error are still ingested. The error makes the table
# unreadable, so this is visible only in the export's statistics.
$ set-from-sql var=unique-table-updates-committed
SELECT SUM(u.updates_committed)::text
  FROM mz_internal.mz_source_statistics u
  JOIN mz_tables t ON t.id = u.id
  WHERE t.name = 'unique_table'

$ sql-server-execute name=sql-server
INSERT INTO unique_table VALUES (2, 'a'), (3, 'c');

> SELECT SUM(u.updates_committed) > ${unique-table-updates-committed}
  FROM mz_internal.mz_source_statistics u
  JOIN mz_tables t ON t.id = u.id
  WHERE t.name = 'unique_table';
true

> DROP TABLE unique_table;

# A constraint added after the table was created was never recorded as a key,
# so adding and dropping it are non-events.
$ sql-server-execute name=sql-server
CREATE TABLE added_constraint (id INT NOT NULL CONSTRAINT added_constraint_primary_key PRIMARY KEY, wallet VARCHAR(64) NOT NULL);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'added_constraint', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO added_constraint VALUES (1, 'a');

> CREATE TABLE added_constraint FROM SOURCE sql_server_src (REFERENCE dbo.added_constraint);

> SELECT * FROM added_constraint;
1 a

$ sql-server-execute name=sql-server
ALTER TABLE added_constraint ADD CONSTRAINT added_constraint_wallet_unique UNIQUE (wallet);
INSERT INTO added_constraint VALUES (2, 'b');
ALTER TABLE added_constraint DROP CONSTRAINT added_constraint_wallet_unique;
INSERT INTO added_constraint VALUES (3, 'b');

> SELECT * FROM added_constraint;
1 a
2 b
3 b

> DROP TABLE added_constraint;

# Dropping one of several constraints names that constraint, with its columns
# in key order.
$ sql-server-execute name=sql-server
CREATE TABLE multiple_constraints (id INT NOT NULL CONSTRAINT multiple_constraints_primary_key PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, c INT NOT NULL, CONSTRAINT multiple_constraints_a_unique UNIQUE (a), CONSTRAINT multiple_constraints_b_c_unique UNIQUE (b, c));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'multiple_constraints', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO multiple_constraints VALUES (1, 10, 100, 1000);

> CREATE TABLE multiple_constraints FROM SOURCE sql_server_src (REFERENCE dbo.multiple_constraints);

> SELECT * FROM multiple_constraints;
1 10 100 1000

$ sql-server-execute name=sql-server
ALTER TABLE multiple_constraints DROP CONSTRAINT multiple_constraints_b_c_unique;
INSERT INTO multiple_constraints VALUES (2, 20, 100, 1000);

! SELECT * FROM multiple_constraints;
contains:incompatible schema change on dbo.multiple_constraints: UNIQUE constraint "multiple_constraints_b_c_unique" (b, c) was dropped upstream

> DROP TABLE multiple_constraints;

# A constraint over an excluded column was never recorded as a key, so dropping
# it is a non-event.
$ sql-server-execute name=sql-server
CREATE TABLE excluded_column (id INT NOT NULL CONSTRAINT excluded_column_primary_key PRIMARY KEY, a INT NOT NULL, meta VARCHAR(32) NOT NULL, CONSTRAINT unique_on_excluded_column UNIQUE (meta));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'excluded_column', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO excluded_column VALUES (1, 10, 'x');

> CREATE TABLE excluded_column FROM SOURCE sql_server_src (REFERENCE dbo.excluded_column) WITH (EXCLUDE COLUMNS = (meta));

> SELECT * FROM excluded_column;
1 10

$ sql-server-execute name=sql-server
ALTER TABLE excluded_column DROP CONSTRAINT unique_on_excluded_column;
INSERT INTO excluded_column VALUES (2, 20, 'x');

> SELECT * FROM excluded_column;
1 10
2 20

> DROP TABLE excluded_column;

# Recreating a constraint under the same name over different columns is
# reported as an alteration. The statements run as one batch so a poll cannot
# observe the intermediate state where the constraint is simply gone.
$ sql-server-execute name=sql-server
CREATE TABLE recreated_constraint (id INT NOT NULL CONSTRAINT recreated_constraint_primary_key PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, CONSTRAINT recreated_constraint_unique UNIQUE (a));
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'recreated_constraint', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO recreated_constraint VALUES (1, 10, 100);

> CREATE TABLE recreated_constraint FROM SOURCE sql_server_src (REFERENCE dbo.recreated_constraint);

> SELECT * FROM recreated_constraint;
1 10 100

$ sql-server-execute name=sql-server split-lines=false
ALTER TABLE recreated_constraint DROP CONSTRAINT recreated_constraint_unique;
ALTER TABLE recreated_constraint ADD CONSTRAINT recreated_constraint_unique UNIQUE (b);
INSERT INTO recreated_constraint VALUES (2, 10, 200);

! SELECT * FROM recreated_constraint;
contains:incompatible schema change on dbo.recreated_constraint: UNIQUE constraint "recreated_constraint_unique" (a) was renamed or recreated upstream

> DROP TABLE recreated_constraint;

# Renaming the upstream table is a non-event. The capture instance, its change
# table, and the constraint re-read follow the table by object id, not by name,
# and a constraint dropped after the rename is still reported, under the name
# the Materialize table was created with.
$ sql-server-execute name=sql-server
CREATE TABLE renamed_table (id INT NOT NULL CONSTRAINT renamed_table_primary_key PRIMARY KEY, wallet VARCHAR(64) NOT NULL CONSTRAINT renamed_table_wallet_unique UNIQUE);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'renamed_table', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO renamed_table VALUES (1, 'a');

> CREATE TABLE renamed_table FROM SOURCE sql_server_src (REFERENCE dbo.renamed_table);

> SELECT * FROM renamed_table;
1 a

$ sql-server-execute name=sql-server
EXEC sp_rename 'dbo.renamed_table', 'renamed_table_after';
INSERT INTO renamed_table_after VALUES (2, 'b');

> SELECT * FROM renamed_table;
1 a
2 b

$ sql-server-execute name=sql-server
ALTER TABLE renamed_table_after DROP CONSTRAINT renamed_table_wallet_unique;

! SELECT * FROM renamed_table;
contains:incompatible schema change on dbo.renamed_table: UNIQUE constraint "renamed_table_wallet_unique" (wallet) was dropped upstream

> DROP TABLE renamed_table;

# A table tracked by two capture instances: both exports see the table's
# constraints, so neither reports a phantom dropped key, and a real drop is
# reported on both.
$ sql-server-execute name=sql-server
CREATE TABLE two_capture_instances (id INT NOT NULL CONSTRAINT two_capture_instances_primary_key PRIMARY KEY, wallet VARCHAR(64) NOT NULL CONSTRAINT two_capture_instances_wallet_unique UNIQUE);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'two_capture_instances', @role_name = 'SA', @supports_net_changes = 0, @capture_instance = 'two_capture_instances_v1';
INSERT INTO two_capture_instances VALUES (1, 'a');

> CREATE TABLE two_capture_instances_v1 FROM SOURCE sql_server_src (REFERENCE dbo.two_capture_instances);

> SELECT * FROM two_capture_instances_v1;
1 a

# The new table picks the most recently created capture instance.
$ sql-server-execute name=sql-server
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'two_capture_instances', @role_name = 'SA', @supports_net_changes = 0, @capture_instance = 'two_capture_instances_v2';
INSERT INTO two_capture_instances VALUES (2, 'b');

> CREATE TABLE two_capture_instances_v2 FROM SOURCE sql_server_src (REFERENCE dbo.two_capture_instances);

$ sql-server-execute name=sql-server
INSERT INTO two_capture_instances VALUES (3, 'c');

> SELECT * FROM two_capture_instances_v1;
1 a
2 b
3 c

> SELECT * FROM two_capture_instances_v2;
1 a
2 b
3 c

$ sql-server-execute name=sql-server
ALTER TABLE two_capture_instances DROP CONSTRAINT two_capture_instances_wallet_unique;
INSERT INTO two_capture_instances VALUES (4, 'c');

! SELECT * FROM two_capture_instances_v1;
contains:incompatible schema change on dbo.two_capture_instances: UNIQUE constraint "two_capture_instances_wallet_unique" (wallet) was dropped upstream

! SELECT * FROM two_capture_instances_v2;
contains:incompatible schema change on dbo.two_capture_instances: UNIQUE constraint "two_capture_instances_wallet_unique" (wallet) was dropped upstream

# A SQL Server ingestion runs on one replica of its cluster at a time, the one
# with the lowest id, and moves only when that replica goes away. Constraints
# are observed by whichever replica is polling, so a constraint dropped and
# recreated while r1 runs the ingestion is seen by r1 and never by r2. The
# definite error r1 wrote into the table's shard is permanent: after r1 is
# dropped, r2 takes over, reads an intact constraint, records no change, keeps
# ingesting, and the table stays in error.
$ sql-server-execute name=sql-server
CREATE TABLE replica_failover (id INT NOT NULL CONSTRAINT replica_failover_primary_key PRIMARY KEY, wallet VARCHAR(64) NOT NULL CONSTRAINT replica_failover_wallet_unique UNIQUE);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'replica_failover', @role_name = 'SA', @supports_net_changes = 0;
INSERT INTO replica_failover VALUES (1, 'a');

> CREATE CLUSTER failover_cluster REPLICAS (r1 (SIZE 'scale=1,workers=1'), r2 (SIZE 'scale=1,workers=1'));

> CREATE SOURCE replica_failover_src IN CLUSTER failover_cluster FROM SQL SERVER CONNECTION sql_server_conn;

> CREATE TABLE replica_failover FROM SOURCE replica_failover_src (REFERENCE dbo.replica_failover);

> SELECT * FROM replica_failover;
1 a

> SELECT r.name
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON r.id = h.replica_id
  JOIN mz_sources s ON s.id = h.object_id
  WHERE s.name = 'replica_failover_src' AND h.hydrated;
r1

$ sql-server-execute name=sql-server
ALTER TABLE replica_failover DROP CONSTRAINT replica_failover_wallet_unique;

! SELECT * FROM replica_failover;
contains:incompatible schema change on dbo.replica_failover: UNIQUE constraint "replica_failover_wallet_unique" (wallet) was dropped upstream

$ sql-server-execute name=sql-server
ALTER TABLE replica_failover ADD CONSTRAINT replica_failover_wallet_unique UNIQUE (wallet);

> DROP CLUSTER REPLICA failover_cluster.r1;

> SELECT r.name
  FROM mz_internal.mz_hydration_statuses h
  JOIN mz_cluster_replicas r ON r.id = h.replica_id
  JOIN mz_sources s ON s.id = h.object_id
  WHERE s.name = 'replica_failover_src' AND h.hydrated;
r2

$ sql-server-execute name=sql-server
INSERT INTO replica_failover VALUES (2, 'b');

# r2 ingests the new row with no error of its own, and the error r1 persisted
# is not retracted.
> SELECT status, error IS NULL
  FROM mz_internal.mz_source_statuses
  WHERE name = 'replica_failover_src';
running true

! SELECT * FROM replica_failover;
contains:incompatible schema change on dbo.replica_failover: UNIQUE constraint "replica_failover_wallet_unique" (wallet) was dropped upstream

> DROP CLUSTER failover_cluster CASCADE;

> DROP SOURCE sql_server_src CASCADE;
