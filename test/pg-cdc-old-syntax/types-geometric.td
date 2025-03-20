# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test the geometric data types
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

# Insert data pre-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t1 (point_col POINT, line_col LINE, lseg_col LSEG);
ALTER TABLE t1 REPLICA IDENTITY FULL;

INSERT INTO t1 VALUES ('(1,1)', '{1,2,3}', '(1,1),(2,2)');

CREATE PUBLICATION mz_source FOR ALL TABLES;

! CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source') FOR ALL TABLES;
contains:referenced tables use unsupported types
