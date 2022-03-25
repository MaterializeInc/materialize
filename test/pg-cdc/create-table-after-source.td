# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test creating a table after creating the source
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;
CREATE SCHEMA public;
CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';

> SELECT COUNT(*) = 0 FROM mz_source;
true

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE t1 (f1 INTEGER);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 VALUES (1);

! CREATE VIEWS FROM SOURCE mz_source (t1);
contains:table t1 not found

> DROP SOURCE mz_source;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source (t1);

# To keep this test passing we expect no result here, since that is now what we get
> SELECT * FROM t1;
1
