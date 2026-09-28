# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Catalog read protection is enabled at environment creation by mzcompose.
# Exercise timeline protection through SQL, including collections that have no
# readable replica yet. Testdrive bounds queries and retries hydration reads.
$ set-sql-timeout duration=60s force=true
$ set-arg-default single-replica-cluster=quickstart
$ set-arg-default default-replica-size=scale=1,workers=1

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_alter_table_add_column = true;
ALTER SYSTEM SET enable_index_options = true;
ALTER SYSTEM SET enable_logical_compaction_window = true;
ALTER SYSTEM SET enable_unlimited_retain_history = true;

> SET statement_timeout = '30s'
> SET cluster = ${arg.single-replica-cluster}

> CREATE TABLE timeline_rows (a int)
> INSERT INTO timeline_rows VALUES (1), (2)
> SELECT * FROM timeline_rows
1
2

# Index installation must finish without a replica. Its pending collection
# must not prevent catalog writes or reads on a healthy cluster.
> CREATE CLUSTER timeline_indexed REPLICAS ()
> CREATE INDEX timeline_rows_idx IN CLUSTER timeline_indexed ON timeline_rows (a)
> INSERT INTO timeline_rows VALUES (3)
> SELECT * FROM timeline_rows
1
2
3

> CREATE CLUSTER REPLICA timeline_indexed.r1 SIZE '${arg.default-replica-size}'
> SET cluster = timeline_indexed
> SELECT * FROM timeline_rows
1
2
3

# Log collections use the new cluster's replica-local timeline.
> SELECT count(*) > 0 FROM mz_introspection.mz_dataflows
true

# Hold an indexed snapshot while another connection advances the table and
# tightens both retention policies. A 1ms window allows aggressive compaction,
# whereas RETAIN HISTORY FOR 0 disables compaction.
$ postgres-connect name=timeline_writer url=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
$ postgres-execute connection=timeline_writer
SET statement_timeout = '30s';
SET cluster = timeline_indexed;

> BEGIN
> SELECT * FROM timeline_rows
1
2
3

$ postgres-execute connection=timeline_writer
ALTER TABLE timeline_rows SET (RETAIN HISTORY = FOR '1ms');
ALTER INDEX timeline_rows_idx SET (RETAIN HISTORY = FOR '1ms');
UPDATE timeline_rows SET a = a + 10;

> SELECT * FROM timeline_rows
1
2
3
> COMMIT

> SELECT * FROM timeline_rows
11
12
13

# Adding a column creates a new table version. Read it through storage on the
# healthy cluster, independently of the index on the older version.
> ALTER TABLE timeline_rows ADD COLUMN b text
> SET cluster = ${arg.single-replica-cluster}
> SELECT * FROM timeline_rows
11 <null>
12 <null>
13 <null>
> INSERT INTO timeline_rows VALUES (14, 'new')
> SELECT * FROM timeline_rows
11 <null>
12 <null>
13 <null>
14 new

# Cleanup must also progress while an index is pending, including dropping the
# cluster that owns it before it ever has a replica.
> CREATE CLUSTER timeline_pending REPLICAS ()
> CREATE INDEX timeline_pending_idx IN CLUSTER timeline_pending ON timeline_rows (a)
> DROP INDEX timeline_rows_idx
> DROP CLUSTER timeline_indexed
> SELECT * FROM timeline_rows
11 <null>
12 <null>
13 <null>
14 new
> DROP CLUSTER timeline_pending CASCADE
> DROP TABLE timeline_rows

$ postgres-execute connection=mz_system
ALTER SYSTEM RESET enable_alter_table_add_column;
ALTER SYSTEM RESET enable_index_options;
ALTER SYSTEM RESET enable_logical_compaction_window;
ALTER SYSTEM RESET enable_unlimited_retain_history;
