# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# These just test that statement
# logging is set up in CI. The values
# in prod won't necessarily be the same.

> SHOW statement_logging_max_sample_rate
statement_logging_max_sample_rate
----
1.0

> SHOW statement_logging_sample_rate
statement_logging_sample_rate
----
1.0

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_statement_lifecycle_logging = true

# Verify that we can get application names
# that are set after session initialization
> SET application_name TO my_app

# Later, we need to ignore everything
# that happened before the real test started. Sleep here,
# so we can be sure that their `began_at` is different.
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="1s"

# This should fail due to RBAC checks.
! SELECT count(*) FROM mz_internal.mz_statement_execution_history;
contains: permission denied

# Make it so we can query the tables later
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_rbac_checks = false

# No peek result stash for large results, so that result sizes match the expectations below.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_compute_peek_response_stash = false

# Now the real test begins

# This will be executed on `mz_catalog_server`, due to auto-routing of "simple" queries.
> SELECT 'beginning real test!'
"beginning real test!"

> SHOW OBJECTS;

! SELECT 1/0;
contains: division by zero

> PREPARE p AS values ($1)

> EXECUTE p('hello world')
"hello world"

> CREATE TABLE t(f int)

> INSERT INTO t VALUES (1)

! SELECT f/0 FROM t
contains: division by zero

> CREATE DEFAULT INDEX i ON t

> SELECT * FROM t
1

> SELECT count(*) FROM t
1

# This one does not show up in the results. See
# point "(2)" in the comment below.
! SELECT f_nonexistent FROM t
contains: column "f_nonexistent" does not exist

> BEGIN

> DECLARE c CURSOR FOR VALUES (1), (2)

> FETCH c
1

> FETCH c
2

> FETCH c

> COMMIT

# Create a cluster, run a query on it, and drop it to ensure that we retain the cluster name
> CREATE CLUSTER c REPLICAS (r1 (SIZE 'scale=1,workers=1'))

> SET cluster TO c

> SELECT count(*) FROM t
1

> DROP CLUSTER c

# This will be executed on mz_catalog_server, due to auto-routing of catalog queries.
> SELECT count(*) > 0 FROM mz_internal.mz_cluster_replica_metrics
true

# Test that we're correctly recording transaction isolation
> SET transaction_isolation TO serializable

> SELECT 'serializable'
serializable

# Assumptions encoded here:
# (1) "Inner" statement executions are not logged. For example,
#     if SQL-level `PREPARE` and `EXECUTE` are used, those statements will
#     be logged, but not the statements they reference.
# (2) We don't have a way to log errors that happen during statement preparation,
#     but only during statement execution.
# (3) SQL-level statements always use a prepared statement with a useless name beginning with 's'
# (4) `FETCH` has the somewhat confusing behavior of reporting the `result_size` and `rows_returned`
#     of the *total* result set in the first `FETCH`, and null thereafter.
#
# All of these (except (3), which is just how the postgres library TD uses works) are working as designed.
# However, we recognize that they are confusing, so we will work on making them
# less confusing as follow-ups.
#
# When we do so, we will need to change this test to encode the improved behavior.

> WITH all_stmts AS (SELECT mseh.*, mpsh.*, mst.sql, mst.redacted_sql FROM mz_internal.mz_statement_execution_history mseh RIGHT JOIN mz_internal.mz_prepared_statement_history mpsh ON mseh.prepared_statement_id = mpsh.id RIGHT JOIN (SELECT DISTINCT sql_hash, sql, redacted_sql FROM mz_internal.mz_sql_text) mst ON mpsh.sql_hash = mst.sql_hash),
       test_begin AS (SELECT began_at FROM all_stmts WHERE sql = 'SELECT ''beginning real test!''' ORDER BY began_at DESC LIMIT 1)
  SELECT c.name, all_stmts.cluster_name, all_stmts.application_name, all_stmts.sample_rate, all_stmts.params, all_stmts.finished_status, all_stmts.error_message, all_stmts.result_size, all_stmts.rows_returned, all_stmts.execution_strategy, all_stmts.name LIKE 's%', all_stmts.sql, all_stmts.transaction_isolation, all_stmts.transient_index_id ~ '^t[0-9]+$',
  all_stmts.database_name, all_stmts.search_path::text
  FROM all_stmts, test_begin LEFT JOIN mz_clusters c ON c.id = all_stmts.cluster_id WHERE all_stmts.began_at >= test_begin.began_at AND all_stmts.sql NOT LIKE '%sduiahsdfuoiahsdf%'
<null> <null> my_app 1 {} success <null> <null> <null> <null> true "SET transaction_isolation TO serializable" "strict serializable" <null> materialize {public}
<null> c my_app 1 {} success <null> 18 1 standard true "SELECT count(*) FROM t" "strict serializable" true materialize {public}
<null> c my_app 1 {} success <null> <null> <null> <null> true "DROP CLUSTER c" "strict serializable" <null> materialize {public}
quickstart quickstart my_app 1 {} error "division by zero" <null> <null> <null> true "SELECT 1/0" "strict serializable" <null> materialize {public}
quickstart quickstart my_app 1 {} error "Evaluation error: division by zero" <null> <null> <null> true "SELECT f/0 FROM t" "strict serializable" true materialize {public}
quickstart quickstart my_app 1 {} success <null> 13 1 constant true "EXECUTE p ('<REDACTED>')" "strict serializable" <null> materialize {public}
quickstart quickstart my_app 1 {} success <null> 18 1 fast-path true "SELECT * FROM t" "strict serializable" <null> materialize {public}
quickstart quickstart my_app 1 {} success <null> 18 1 standard true "SELECT count(*) FROM t" "strict serializable" true materialize {public}
quickstart quickstart my_app 1 {} success <null> 4 2 constant true "FETCH c" "strict serializable" <null> materialize {public}
quickstart quickstart my_app 1 {} success <null> <null> <null> <null> true BEGIN "strict serializable" <null> materialize {public}
quickstart quickstart my_app 1 {} success <null> <null> <null> <null> true COMMIT "strict serializable" <null> materialize {public}
quickstart quickstart my_app 1 {} success <null> <null> <null> <null> true "CREATE CLUSTER c REPLICAS (r1 (SIZE 'scale=1,workers=1'))" "strict serializable" <null> materialize {public}
quickstart quickstart my_app 1 {} success <null> <null> <null> <null> true "CREATE DEFAULT INDEX i ON t" "strict serializable" <null> materialize {public}
quickstart quickstart my_app 1 {} success <null> <null> <null> <null> true "CREATE TABLE t(f int)" "strict serializable" <null> materialize {public}
quickstart quickstart my_app 1 {} success <null> <null> <null> <null> true "DECLARE c CURSOR FOR VALUES (1), (2)" "strict serializable" <null> materialize {public}
quickstart quickstart my_app 1 {} success <null> <null> <null> <null> true "FETCH c" "strict serializable" <null> materialize {public}
quickstart quickstart my_app 1 {} success <null> <null> <null> <null> true "FETCH c" "strict serializable" <null> materialize {public}
quickstart quickstart my_app 1 {} success <null> <null> <null> <null> true "INSERT INTO t VALUES ('<REDACTED>')" "strict serializable" <null> materialize {public}
quickstart quickstart my_app 1 {} success <null> <null> <null> <null> true "PREPARE p AS values ($1)" "strict serializable" <null> materialize {public}
quickstart quickstart my_app 1 {} success <null> <null> <null> <null> true "SET cluster TO c" "strict serializable" <null> materialize {public}
mz_catalog_server mz_catalog_server my_app 1 {} success <null> 0 0 fast-path true "SHOW OBJECTS" "strict serializable" <null> materialize {public}
mz_catalog_server mz_catalog_server my_app 1 {} success <null> 22 1 constant true "SELECT 'beginning real test!'" "strict serializable" <null> materialize {public}
mz_catalog_server mz_catalog_server my_app 1 {} success <null> 14 1 constant true "SELECT 'serializable'" serializable <null> materialize {public}
mz_catalog_server mz_catalog_server my_app 1 {} success <null> 17 1 standard true "SELECT count(*) > 0 FROM mz_internal.mz_cluster_replica_metrics" "strict serializable" true materialize {public}

> WITH all_stmts AS (SELECT mseh.id, mseh.began_at, mst.sql FROM mz_internal.mz_statement_execution_history mseh JOIN mz_internal.mz_prepared_statement_history mpsh ON mseh.prepared_statement_id = mpsh.id JOIN (SELECT DISTINCT sql, sql_hash, redacted_sql FROM mz_internal.mz_sql_text) mst ON mpsh.sql_hash = mst.sql_hash),
       test_begin AS (SELECT began_at FROM all_stmts WHERE sql = 'SELECT ''beginning real test!''' ORDER BY began_at DESC LIMIT 1)
  SELECT sql, event_type FROM test_begin, mz_internal.mz_statement_lifecycle_history mslh
  JOIN all_stmts ON mslh.statement_id = all_stmts.id
  WHERE all_stmts.began_at >= test_begin.began_at AND all_stmts.sql NOT LIKE '%sduiahsdfuoiahsdf%'
BEGIN execution-began
BEGIN execution-finished
COMMIT execution-began
COMMIT execution-finished
"CREATE CLUSTER c REPLICAS (r1 (SIZE 'scale=1,workers=1'))" execution-began
"CREATE CLUSTER c REPLICAS (r1 (SIZE 'scale=1,workers=1'))" execution-finished
"CREATE DEFAULT INDEX i ON t" execution-began
"CREATE DEFAULT INDEX i ON t" execution-finished
"CREATE TABLE t(f int)" execution-began
"CREATE TABLE t(f int)" execution-finished
"DECLARE c CURSOR FOR VALUES (1), (2)" execution-began
"DECLARE c CURSOR FOR VALUES (1), (2)" execution-finished
"DROP CLUSTER c" execution-began
"DROP CLUSTER c" execution-finished
"EXECUTE p ('<REDACTED>')" compute-dependencies-finished
"EXECUTE p ('<REDACTED>')" execution-began
"EXECUTE p ('<REDACTED>')" execution-finished
"EXECUTE p ('<REDACTED>')" optimization-finished
"EXECUTE p ('<REDACTED>')" storage-dependencies-finished
"FETCH c" compute-dependencies-finished
"FETCH c" execution-began
"FETCH c" execution-began
"FETCH c" execution-began
"FETCH c" execution-finished
"FETCH c" execution-finished
"FETCH c" execution-finished
"FETCH c" optimization-finished
"FETCH c" storage-dependencies-finished
"INSERT INTO t VALUES ('<REDACTED>')" execution-began
"INSERT INTO t VALUES ('<REDACTED>')" execution-finished
"PREPARE p AS values ($1)" execution-began
"PREPARE p AS values ($1)" execution-finished
"SELECT 'beginning real test!'" compute-dependencies-finished
"SELECT 'beginning real test!'" execution-began
"SELECT 'beginning real test!'" execution-finished
"SELECT 'beginning real test!'" optimization-finished
"SELECT 'beginning real test!'" storage-dependencies-finished
"SELECT 'serializable'" compute-dependencies-finished
"SELECT 'serializable'" execution-began
"SELECT 'serializable'" execution-finished
"SELECT 'serializable'" optimization-finished
"SELECT 'serializable'" storage-dependencies-finished
"SELECT * FROM t" compute-dependencies-finished
"SELECT * FROM t" execution-began
"SELECT * FROM t" execution-finished
"SELECT * FROM t" optimization-finished
"SELECT * FROM t" storage-dependencies-finished
"SELECT count(*) > 0 FROM mz_internal.mz_cluster_replica_metrics" compute-dependencies-finished
"SELECT count(*) > 0 FROM mz_internal.mz_cluster_replica_metrics" execution-began
"SELECT count(*) > 0 FROM mz_internal.mz_cluster_replica_metrics" execution-finished
"SELECT count(*) > 0 FROM mz_internal.mz_cluster_replica_metrics" optimization-finished
"SELECT count(*) FROM t" compute-dependencies-finished
"SELECT count(*) FROM t" compute-dependencies-finished
"SELECT count(*) FROM t" execution-began
"SELECT count(*) FROM t" execution-began
"SELECT count(*) FROM t" execution-finished
"SELECT count(*) FROM t" execution-finished
"SELECT count(*) FROM t" optimization-finished
"SELECT count(*) FROM t" optimization-finished
"SELECT count(*) FROM t" storage-dependencies-finished
"SELECT count(*) FROM t" storage-dependencies-finished
"SELECT f/0 FROM t" compute-dependencies-finished
"SELECT f/0 FROM t" execution-began
"SELECT f/0 FROM t" execution-finished
"SELECT f/0 FROM t" optimization-finished
"SELECT f/0 FROM t" storage-dependencies-finished
"SET cluster TO c" execution-began
"SET cluster TO c" execution-finished
"SET transaction_isolation TO serializable" execution-began
"SET transaction_isolation TO serializable" execution-finished
"SELECT 1/0" compute-dependencies-finished
"SELECT 1/0" execution-began
"SELECT 1/0" execution-finished
"SELECT 1/0" optimization-finished
"SELECT 1/0" storage-dependencies-finished
"SHOW OBJECTS" compute-dependencies-finished
"SHOW OBJECTS" execution-began
"SHOW OBJECTS" execution-finished
"SHOW OBJECTS" optimization-finished
"SHOW OBJECTS" storage-dependencies-finished

# Test that everything in a transaction has the same transaction ID

> BEGIN --hello

> SELECT 'transaction statement 1'
"transaction statement 1"

> SELECT 'transaction statement 2'
"transaction statement 2"

> ROLLBACK

> WITH begin_tid AS
  (SELECT transaction_id FROM mz_internal.mz_statement_execution_history mseh, mz_internal.mz_prepared_statement_history mpsh, mz_internal.mz_sql_text mst
   WHERE mseh.prepared_statement_id = mpsh.id
   AND mpsh.sql_hash = mst.sql_hash
   AND mst.sql = 'BEGIN --hello')
  SELECT sql FROM mz_internal.mz_statement_execution_history mseh, mz_internal.mz_prepared_statement_history mpsh, begin_tid, mz_internal.mz_sql_text mst
  WHERE mseh.prepared_statement_id = mpsh.id
  AND mseh.transaction_id = begin_tid.transaction_id
  AND mpsh.sql_hash = mst.sql_hash
  AND mseh.application_name = 'my_app'
"BEGIN --hello"
"SELECT 'transaction statement 1'"
"SELECT 'transaction statement 2'"
ROLLBACK

# Test that SECRETs have their values redacted.

> CREATE SECRET my_super_secret AS '123';

> SELECT redacted_sql FROM mz_internal.mz_recent_activity_log_redacted WHERE statement_type = 'create_secret';
"CREATE SECRET my_super_secret AS '<REDACTED>'"

> SELECT sql FROM mz_internal.mz_recent_activity_log WHERE statement_type = 'create_secret';
"CREATE SECRET my_super_secret AS '<REDACTED>'"

> ALTER SECRET my_super_secret AS '456';

> SELECT redacted_sql FROM mz_internal.mz_recent_activity_log_redacted WHERE statement_type = 'alter_secret';
"ALTER SECRET my_super_secret AS '<REDACTED>'"

> SELECT sql FROM mz_internal.mz_recent_activity_log WHERE statement_type = 'alter_secret';
"ALTER SECRET my_super_secret AS '<REDACTED>'"

# Test that UPDATE redacts values.

> RESET cluster

> CREATE TABLE redacted_test (a TEXT);

> INSERT INTO redacted_test VALUES ('should be redacted');

> SELECT sql FROM mz_internal.mz_recent_activity_log WHERE statement_type = 'insert' AND sql like '%redacted_test%';
"INSERT INTO redacted_test VALUES ('<REDACTED>')"

> UPDATE redacted_test SET a = 'also redacted' WHERE a = 'should be redacted';

> SELECT sql FROM mz_internal.mz_recent_activity_log WHERE statement_type = 'update';
"UPDATE redacted_test SET a = '<REDACTED>' WHERE a = '<REDACTED>'"

# Test that the number of rows returned in query history respects OFFSET and LIMIT.

> CREATE TABLE t_offset_limit (a int);

> INSERT INTO t_offset_limit SELECT generate_series(1, 5);

# Make sure the table has its data already, otherwise Testdrive might execute it multiple times
> SELECT COUNT(*) FROM t_offset_limit
5

> SELECT * FROM t_offset_limit ORDER BY a DESC OFFSET 4;
1

> SELECT * FROM t_offset_limit ORDER BY a DESC LIMIT 2;
5
4

> SELECT * FROM t_offset_limit ORDER BY a DESC OFFSET 100;

> SELECT * FROM t_offset_limit ORDER BY a DESC LIMIT 0;

> SELECT result_size, rows_returned FROM mz_internal.mz_recent_activity_log WHERE sql = 'SELECT * FROM t_offset_limit ORDER BY a DESC OFFSET 4';
90 1

> SELECT result_size, rows_returned FROM mz_internal.mz_recent_activity_log WHERE sql = 'SELECT * FROM t_offset_limit ORDER BY a DESC LIMIT 2';
54 2

> SELECT result_size, rows_returned FROM mz_internal.mz_recent_activity_log WHERE sql = 'SELECT * FROM t_offset_limit ORDER BY a DESC OFFSET 100';
90 0

> SELECT result_size, rows_returned FROM mz_internal.mz_recent_activity_log WHERE sql = 'SELECT * FROM t_offset_limit ORDER BY a DESC LIMIT 0';
0 0

> SELECT mseh.began_at, mslh.occurred_at
  FROM mz_internal.mz_statement_execution_history mseh JOIN mz_internal.mz_statement_lifecycle_history mslh ON (mseh.id = mslh.statement_id)
  WHERE mslh.event_type = 'execution-began' AND mseh.began_at != mslh.occurred_at;

# Check that all statements are marked as finished.
> SELECT count(*)
  FROM mz_internal.mz_recent_activity_log
  WHERE (finished_at IS NULL OR finished_status IS NULL) AND sql NOT LIKE '%__FILTER-OUT-THIS-QUERY__%';
0
