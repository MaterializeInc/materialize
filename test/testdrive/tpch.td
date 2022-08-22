# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test the cardinality of TPCH tables. For tables that have a random
# cardinality, don't depend on exact count that could change if the rng
# implementation changes, just check that it's within the spec range.

! CREATE SOURCE gen FROM LOAD GENERATOR TPCH (SCALE FACTOR 0)
contains: multi-output sources require a FOR TABLES (..) or FOR ALL TABLES statement

! CREATE SOURCE gen FROM LOAD GENERATOR TPCH (SCALE FACTOR 9223372036854775807) FOR ALL TABLES
contains: unsupported scale factor 9223372036854776000

! CREATE SOURCE gen FROM LOAD GENERATOR TPCH (SCALE FACTOR -1) FOR ALL TABLES
contains: unsupported scale factor -1

> CREATE SOURCE gen FROM LOAD GENERATOR TPCH (SCALE FACTOR .001) FOR ALL TABLES

$ set-from-sql var=source-size
SELECT size FROM mz_sources WHERE name = 'gen'

> SHOW SOURCES
name         type       size
--------------------------------
 customer  subsource       <null>
 gen       load-generator  ${source-size}
 lineitem  subsource       <null>
 nation    subsource       <null>
 orders    subsource       <null>
 part      subsource       <null>
 partsupp  subsource       <null>
 region    subsource       <null>
 supplier  subsource       <null>

# SF * 150,000
> SELECT count(*) FROM customer
150

# For each row in the ORDERS table, a random number of rows within [1 .. 7] in the LINEITEM table
> SELECT count(*) >= 1500 AND count(*) <= 1500 * 7 FROM lineitem
true

# 25 rows in the NATION table
> SELECT count(*) FROM nation
25

# For each row in the CUSTOMER table, ten rows in the ORDERS table
> SELECT count(*) FROM orders
1500

# SF * 200,000
> SELECT count(*) FROM part
200

# For each row in the PART table, four rows in PartSupp table
> SELECT count(*) FROM partsupp
800

# 5 rows in the REGION table
> SELECT count(*) FROM region
5

# SF * 10,000
> SELECT count(*) FROM supplier
10
