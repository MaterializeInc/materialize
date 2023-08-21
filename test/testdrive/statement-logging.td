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

# Later, we need to ignore everything
# that happened before the real test started. Sleep here,
# so we can be sure that their `began_at` is different.
> SELECT mz_internal.mz_sleep(1)
<null>

# Now the real test begins

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
  SELECT all_stmts.sample_rate, all_stmts.params, all_stmts.finished_status, all_stmts.error_message, all_stmts.rows_returned, all_stmts.execution_strategy, all_stmts.name LIKE 's%', all_stmts.sql
  FROM all_stmts, test_begin WHERE all_stmts.began_at >= test_begin.began_at AND all_stmts.sql NOT LIKE '%sduiahsdfuoiahsdf%' --ignore this statement, in case we have to re-run it
  ORDER BY all_stmts.sql, all_statements.rows_returned
1 {} error "Evaluation error: division by zero" <null> <null> true "SELECT f/0 FROM t"
1 {} success <null> 1 constant true "EXECUTE p('hello world')"
1 {} success <null> 1 constant true "SELECT 'beginning real test!'"
1 {} success <null> 1 fast-path true "SELECT * FROM t"
1 {} success <null> 1 standard true "SELECT count(*) FROM t"
1 {} success <null> 2 constant true "FETCH c"
1 {} success <null> <null> <null> true BEGIN
1 {} success <null> <null> <null> true COMMIT
1 {} success <null> <null> <null> true "CREATE DEFAULT INDEX i ON t"
1 {} success <null> <null> <null> true "CREATE TABLE t(f int)"
1 {} success <null> <null> <null> true "DECLARE c CURSOR FOR VALUES (1), (2)"
1 {} success <null> <null> <null> true "FETCH c"
1 {} success <null> <null> <null> true "FETCH c"
1 {} success <null> <null> <null> true "INSERT INTO t VALUES (1)"
1 {} success <null> <null> <null> true "PREPARE p AS values ($1)"
