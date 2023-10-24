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

# Verify that we can get application names
# that are set after session initialization
> SET application_name TO my_app

# Later, we need to ignore everything
# that happened before the real test started. Sleep here,
# so we can be sure that their `began_at` is different.
> SELECT mz_internal.mz_sleep(1)
<null>

# Make it so we can query the tables later
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_rbac_checks = false

# Now the real test begins

# This will be executed on `mz_introspection`, due to auto-routing of "simple" queries.
> SELECT 'beginning real test!'
"beginning real test!"

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
> CREATE CLUSTER c REPLICAS (r1 (size '1'))

> SET cluster TO c

> SELECT count(*) FROM t
1

> DROP CLUSTER c

# This will be executed on mz_introspection, due to auto-routing of introspection queries.
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
# (4) `FETCH` has the somewhat confusing behavior of reporting the `rows_returned`
#     of the *total* result set in the first `FETCH`, and null thereafter.
#
# All of these (except (3), which is just how the postgres library TD uses works) are working as designed.
# However, we recognize that they are confusing, so we will work on making them
# less confusing as follow-ups.
#
# When we do so, we will need to change this test to encode the improved behavior.

> WITH all_stmts AS (SELECT * FROM mz_internal.mz_statement_execution_history mseh RIGHT JOIN mz_internal.mz_prepared_statement_history mpsh ON mseh.prepared_statement_id = mpsh.id),
       test_begin AS (SELECT began_at FROM all_stmts WHERE sql = 'SELECT ''beginning real test!''' ORDER BY began_at DESC LIMIT 1)
  SELECT c.name, all_stmts.cluster_name, all_stmts.application_name, all_stmts.sample_rate, all_stmts.params, all_stmts.finished_status, all_stmts.error_message, all_stmts.rows_returned, all_stmts.execution_strategy, all_stmts.name LIKE 's%', all_stmts.sql, all_stmts.transaction_isolation
  FROM all_stmts, test_begin LEFT JOIN mz_clusters c ON c.id = all_stmts.cluster_id WHERE all_stmts.began_at >= test_begin.began_at AND all_stmts.sql NOT LIKE '%sduiahsdfuoiahsdf%'
<null> <null> my_app 1 {} success <null> <null> <null> true "SET transaction_isolation TO serializable" "strict serializable"
<null> c my_app 1 {} success <null> 1 standard true "SELECT count(*) FROM t" "strict serializable"
<null> c my_app 1 {} success <null> <null> <null> true "DROP CLUSTER c" "strict serializable"
default default my_app 1 {} error "Evaluation error: division by zero" <null> <null> true "SELECT f/0 FROM t" "strict serializable"
default default my_app 1 {} success <null> 1 constant true "EXECUTE p('hello world')" "strict serializable"
default default my_app 1 {} success <null> 1 fast-path true "SELECT * FROM t" "strict serializable"
default default my_app 1 {} success <null> 1 standard true "SELECT count(*) FROM t" "strict serializable"
default default my_app 1 {} success <null> 2 constant true "FETCH c" "strict serializable"
default default my_app 1 {} success <null> <null> <null> true BEGIN "strict serializable"
default default my_app 1 {} success <null> <null> <null> true COMMIT "strict serializable"
default default my_app 1 {} success <null> <null> <null> true "CREATE CLUSTER c REPLICAS (r1 (size '1'))" "strict serializable"
default default my_app 1 {} success <null> <null> <null> true "CREATE DEFAULT INDEX i ON t" "strict serializable"
default default my_app 1 {} success <null> <null> <null> true "CREATE TABLE t(f int)" "strict serializable"
default default my_app 1 {} success <null> <null> <null> true "DECLARE c CURSOR FOR VALUES (1), (2)" "strict serializable"
default default my_app 1 {} success <null> <null> <null> true "FETCH c" "strict serializable"
default default my_app 1 {} success <null> <null> <null> true "FETCH c" "strict serializable"
default default my_app 1 {} success <null> <null> <null> true "INSERT INTO t VALUES (1)" "strict serializable"
default default my_app 1 {} success <null> <null> <null> true "PREPARE p AS values ($1)" "strict serializable"
default default my_app 1 {} success <null> <null> <null> true "SET cluster TO c" "strict serializable"
mz_introspection mz_introspection my_app 1 {} success <null> 1 constant true "SELECT 'beginning real test!'" "strict serializable"
mz_introspection mz_introspection my_app 1 {} success <null> 1 constant true "SELECT 'serializable'" serializable
mz_introspection mz_introspection my_app 1 {} success <null> 1 standard true "SELECT count(*) > 0 FROM mz_internal.mz_cluster_replica_metrics" "strict serializable"

# Test that everything in a transaction has the same transaction ID

> BEGIN --hello

> SELECT 'transaction statement 1'
"transaction statement 1"

> SELECT 'transaction statement 2'
"transaction statement 2"

> ROLLBACK

> WITH begin_tid AS
  (SELECT transaction_id FROM mz_internal.mz_statement_execution_history mseh, mz_internal.mz_prepared_statement_history mpsh
   WHERE mseh.prepared_statement_id = mpsh.id
   AND mpsh.sql = 'BEGIN --hello')
  SELECT sql FROM mz_internal.mz_statement_execution_history mseh, mz_internal.mz_prepared_statement_history mpsh, begin_tid
  WHERE mseh.prepared_statement_id = mpsh.id
  AND mseh.transaction_id = begin_tid.transaction_id
  AND mseh.application_name = 'my_app'
"BEGIN --hello"
"SELECT 'transaction statement 1'"
"SELECT 'transaction statement 2'"
ROLLBACK
