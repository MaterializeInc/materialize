# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test DROP ... CASCADE statements with multiple items
# From https://github.com/MaterializeInc/materialize/issues/5316
> CREATE TABLE t1 (f1 INTEGER)
> CREATE TABLE t2 (f2 INTEGER)
> CREATE MATERIALIZED VIEW v1 AS SELECT * FROM t1, t2
> DROP TABLE t1, t2 CASCADE
