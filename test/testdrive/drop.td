# Copyright Materialize, Inc. and contributors. All rights reserved.
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

# Test CREATE OR REPLACE statement that attempts to depend
# on the object that is being replaced
# https://github.com/MaterializeInc/materialize/issues/5272
> CREATE VIEW v2 AS SELECT 1
! CREATE OR REPLACE VIEW v2 AS SELECT * FROM v2
contains:cannot replace view materialize.public.v2: depended upon by new materialize.public.v2 definition
! CREATE OR REPLACE MATERIALIZED VIEW v2 AS SELECT * FROM v2
contains:cannot replace materialized view materialize.public.v2: depended upon by new materialize.public.v2 definition
