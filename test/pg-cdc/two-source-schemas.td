# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that identically-named tables in two source schemas can be
# successfully disambiguated and replicated
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP PUBLICATION IF EXISTS mz_source;

DROP SCHEMA IF EXISTS schema1 CASCADE;
CREATE SCHEMA schema1;

CREATE TABLE schema1.t1 (f1 INTEGER);
ALTER TABLE schema1.t1 REPLICA IDENTITY FULL;
INSERT INTO schema1.t1 VALUES (1);

DROP SCHEMA IF EXISTS schema2 CASCADE;
CREATE SCHEMA schema2;

CREATE TABLE schema2.t1 (f1 INTEGER);
ALTER TABLE schema2.t1 REPLICA IDENTITY FULL;
INSERT INTO schema2.t1 VALUES (2);

CREATE PUBLICATION mz_source FOR ALL TABLES;

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO schema1.t1 SELECT * FROM schema1.t1;
INSERT INTO schema2.t1 SELECT * FROM schema2.t1;

> BEGIN
> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

> COMMIT
> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE schema1.t1);
! CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE schema2.t1);
contains:catalog item 't1' already exists

> DROP TABLE t1;

! CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);
contains:reference t1 is ambiguous, consider specifying an additional layer of qualification

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE schema1.t1);

> SELECT * FROM t1;
1
1

> DROP SOURCE mz_source CASCADE;

> BEGIN
> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
> CREATE TABLE t1_1 FROM SOURCE mz_source (REFERENCE schema1.t1);
> CREATE TABLE t1_2 FROM SOURCE mz_source (REFERENCE schema2.t1);
> COMMIT

> SELECT * FROM t1_1;
1
1

> SELECT * FROM t1_2;
2
2

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP SCHEMA schema1 CASCADE;
DROP SCHEMA schema2 CASCADE;

> DROP SOURCE mz_source CASCADE;
