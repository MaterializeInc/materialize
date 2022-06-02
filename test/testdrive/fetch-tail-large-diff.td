# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that TAIL/FETCH can handle a case where the diff column has a very large value
#

$ set-regex match=\d{13} replacement=<TIMESTAMP>

> CREATE TABLE ten (f1 INTEGER);

> INSERT INTO ten VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);

> CREATE MATERIALIZED VIEW v1 AS SELECT NULL FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6, ten AS a7

> SELECT COUNT(*) FROM v1;
10000000

> CREATE MATERIALIZED VIEW v2 AS SELECT a1.f1 FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6, ten AS a7

> SELECT COUNT(*) FROM v2;
10000000

> BEGIN

> DECLARE c CURSOR FOR TAIL v1;

> FETCH 1 c;
<TIMESTAMP> 10000000 <null>

> FETCH 1 c WITH (timeout = '1s');

> COMMIT

> BEGIN

> DECLARE c CURSOR FOR TAIL v2;

> FETCH 10 c;
<TIMESTAMP> 1000000 1
<TIMESTAMP> 1000000 2
<TIMESTAMP> 1000000 3
<TIMESTAMP> 1000000 4
<TIMESTAMP> 1000000 5
<TIMESTAMP> 1000000 6
<TIMESTAMP> 1000000 7
<TIMESTAMP> 1000000 8
<TIMESTAMP> 1000000 9
<TIMESTAMP> 1000000 10
