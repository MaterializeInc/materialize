# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE TABLE data (id text, a bigint, b bigint)
> INSERT INTO data VALUES ('valid1', 2, 1), ('valid2', 17, 5)

> CREATE MATERIALIZED VIEW multiply AS SELECT id, a * b AS product FROM data

> CREATE MATERIALIZED VIEW divide AS SELECT id, a / b AS quotient FROM data

> CREATE MATERIALIZED VIEW both AS
  SELECT * FROM multiply NATURAL JOIN divide

> SELECT * FROM both
valid1  2   2
valid2  85  3

> INSERT INTO data VALUES ('bad1', 7, 0)

> SELECT * FROM multiply
valid1  2
valid2  85
bad1    0

! SELECT * FROM divide
contains:division by zero

! SELECT * FROM both
contains:division by zero

> DELETE FROM data WHERE id = 'bad1'

> SELECT * FROM both
valid1  2   2
valid2  85  3
