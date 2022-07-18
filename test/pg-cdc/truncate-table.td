# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test TRUNCATE -- source will error out
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg FOR POSTGRES
  HOST postgres,
  DATABASE postgres,
  USER postgres,
  PASSWORD SECRET pgpass

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t1 (f1 INTEGER);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

> CREATE VIEWS FROM SOURCE mz_source;

> SELECT * FROM t1;
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
TRUNCATE TABLE t1;

# Note: truncating the upstream table will not corrupt the source in Materialize
# for Postgres version 10. For all versions > 10, users will see the following error
# "source table got truncated."
#
# See the Postgres docs for more information:
# https://www.postgresql.org/docs/10/logical-replication-restrictions.html
> SELECT * FROM t1;
1
