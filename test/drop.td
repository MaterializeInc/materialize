# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

> CREATE TABLE t

> DROP TABLE t

! PEEK t
dataflow t does not exist

> CREATE MATERIALIZED VIEW v AS SELECT 42

! DROP SOURCE v
v is not of type SOURCE

! DROP SOURCE IF EXISTS v
v is not of type SOURCE

> DROP VIEW v
