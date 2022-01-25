# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Test the detection of the case where a scalar subquery returns more than 1 row
# and make sure there are no false negatives and only the expected false positives.
# Queries prefixed with "!" are expected to return an error while those with ">" are expected to succeed
#
# Some of the queries that fail in Materialize will succeed in Postgresql
#
# See https://github.com/MaterializeInc/materialize/issues/5670 for a discussion on the remaining
# differences between the two databases
#

$ set-sql-timeout duration=125ms

> CREATE TABLE empty (f1 INTEGER, f2 INTEGER);

> CREATE TABLE one_row (f1 INTEGER, f2 INTEGER);
> INSERT INTO one_row VALUES (1, 1);

> CREATE TABLE two_rows (f1 INTEGER, f2 INTEGER);
> INSERT INTO two_rows VALUES (1, 1), (2, 1);

! SELECT (SELECT f1 FROM two_rows);
contains:Evaluation error: more than one record produced in subquery

! SELECT (SELECT f1 FROM two_rows) FROM two_rows LIMIT 0;
contains:Evaluation error: more than one record produced in subquery

! SELECT (SELECT TRUE FROM two_rows);
contains:Evaluation error: more than one record produced in subquery

! SELECT (SELECT f1 FROM two_rows) FROM two_rows;
contains:Evaluation error: more than one record produced in subquery

! SELECT (SELECT f1 FROM two_rows) FROM empty;
contains:Evaluation error: more than one record produced in subquery

! SELECT (SELECT f1 FROM one_row UNION ALL SELECT f1 FROM one_row) FROM two_rows;
contains:Evaluation error: more than one record produced in subquery

! SELECT (SELECT f1 FROM empty UNION ALL SELECT f1 FROM two_rows) FROM two_rows;
contains:Evaluation error: more than one record produced in subquery

! SELECT (SELECT DISTINCT f1 FROM two_rows) FROM two_rows;
contains:Evaluation error: more than one record produced in subquery

> SELECT (SELECT DISTINCT f2 FROM two_rows) FROM two_rows;
1
1

> SELECT (SELECT MIN(f1) FROM two_rows) FROM two_rows;
1
1

> SELECT (SELECT MIN(f2) FROM two_rows GROUP BY f1 ORDER BY MIN(f2) LIMIT 1);
1

! SELECT (SELECT f1 FROM two_rows GROUP BY f1);
contains:Evaluation error: more than one record produced in subquery

! SELECT (SELECT MIN(f1) FROM two_rows UNION ALL SELECT MIN(f2) FROM two_rows) FROM two_rows;
contains:Evaluation error: more than one record produced in subquery

> SELECT (SELECT MIN(f1) FROM two_rows UNION DISTINCT SELECT MIN(f2) FROM two_rows) FROM two_rows;
1
1

! SELECT MIN((SELECT f1 FROM two_rows)) FROM two_rows;
contains:Evaluation error: more than one record produced in subquery

! SELECT * FROM two_rows WHERE f1 > (SELECT f1 FROM two_rows);
contains:Evaluation error: more than one record produced in subquery

! SELECT * FROM two_rows WHERE f1 > (SELECT f1 FROM one_row) AND f1 > (SELECT f1 FROM two_rows);
contains:Evaluation error: more than one record produced in subquery

! SELECT * FROM two_rows WHERE f1 > (SELECT f1 FROM one_row) OR f1 > (SELECT f1 FROM two_rows);
contains:Evaluation error: more than one record produced in subquery

! SELECT * FROM two_rows WHERE (SELECT f1 FROM two_rows) > (SELECT f1 FROM one_row);
contains:Evaluation error: more than one record produced in subquery

! SELECT * FROM two_rows AS o WHERE (SELECT f1 FROM two_rows AS i WHERE o.f2 = i.f2) = 1 AND f1 = 2;
contains:Evaluation error: more than one record produced in subquery

! SELECT * FROM two_rows JOIN two_rows AS r ON (SELECT TRUE FROM two_rows);
contains:Evaluation error: more than one record produced in subquery

! SELECT * FROM two_rows LEFT JOIN two_rows AS r ON (SELECT f1 = 1 FROM two_rows);
contains:Evaluation error: more than one record produced in subquery

! SELECT f1, COUNT(*) as C FROM two_rows GROUP BY f1 HAVING COUNT(*) > (SELECT f1 FROM two_rows);
contains:Evaluation error: more than one record produced in subquery

! SELECT (SELECT f1 FROM two_rows), COUNT(*) FROM two_rows GROUP BY (SELECT f1 FROM two_rows);
contains:Evaluation error: more than one record produced in subquery

! SELECT COUNT(*) FROM two_rows GROUP BY (SELECT f1 FROM two_rows);
contains:Evaluation error: more than one record produced in subquery

! SELECT * FROM two_rows ORDER BY (SELECT f1 FROM two_rows);
contains:Evaluation error: more than one record produced in subquery

! INSERT INTO two_rows VALUES ((SELECT 1 UNION ALL SELECT 2));
contains:more than one record produced in subquery

! SELECT (SELECT f1 from two_rows) + 1 FROM two_rows;
contains:Evaluation error: more than one record produced in subquery

> SELECT (SELECT f1 FROM two_rows LIMIT 0) IS NULL;
true

> SELECT (SELECT f1 FROM two_rows LIMIT 0) IS NULL FROM two_rows;
true
true

> SELECT (SELECT f1 FROM two_rows ORDER BY 1 LIMIT 1);
1

> SELECT (SELECT f1 FROM two_rows ORDER BY 1 LIMIT 1) FROM two_rows;
1
1

> SELECT * FROM two_rows AS a1 WHERE 1 = ( SELECT 1 FROM two_rows ORDER BY 1 LIMIT 1 )
1 1
2 1

> SELECT (SELECT f1 FROM two_rows EXCEPT SELECT f1 FROM one_row) FROM two_rows;
2
2

> SELECT f1 FROM two_rows WHERE f1 = (SELECT f1 FROM two_rows EXCEPT SELECT f1 FROM one_row);
2

# Two columns returned by subquery in scalar context

! SELECT (SELECT * FROM one_row) FROM one_row;
contains:Expected subselect to return 1 column, got 2 columns

! SELECT * FROM one_row WHERE f1 = ( SELECT * FROM one_row );
contains:WHERE clause error: Expected subselect to return 1 column, got 2 columns
