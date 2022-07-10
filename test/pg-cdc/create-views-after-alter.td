# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test creating a materialized view after altering the upstream source
# This is expected to fail
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

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

> SELECT COUNT(*) > 0 FROM mz_source;
true

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE t1 ADD COLUMN f2 varchar(1);
INSERT INTO t1 VALUES (2, 'a');

# Its debatable if this should succeed when we technically know by here that the source has errored
# but for now this is how it behaves since it still will not serve incorrect results
> CREATE VIEWS FROM SOURCE mz_source;

! SELECT * FROM t1
contains:altered

#
# Test creating a materialized view after altering an irrelevant table in the upstream source
# This is expected to succeed
#

> DROP SOURCE mz_source CASCADE;

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;
CREATE SCHEMA public;

CREATE TABLE t1 (f1 INTEGER);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE MATERIALIZED SOURCE mz_source
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

> SELECT COUNT(*) FROM mz_source;
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE t2 (f1 INTEGER);
ALTER TABLE t2 REPLICA IDENTITY FULL;
INSERT INTO t2 VALUES (2);

> SELECT COUNT(*) FROM mz_source;
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE t2 ADD COLUMN f2 varchar(1);
INSERT INTO t2 VALUES (3, 'c');

> SELECT COUNT(*) FROM mz_source;
1

> CREATE VIEWS FROM SOURCE mz_source;

> SELECT * FROM t1;
1
