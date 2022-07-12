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
> CREATE CONNECTION pg FOR POSTGRES
  HOST postgres,
  DATABASE postgres,
  USER postgres,
  PASSWORD SECRET pgpass

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

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source';

> SELECT COUNT(*) > 0 FROM mz_source;
true

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO schema1.t1 SELECT * FROM schema1.t1;
INSERT INTO schema2.t1 SELECT * FROM schema2.t1;

! CREATE VIEWS FROM SOURCE mz_source;
contains:unknown schema 'schema1'

! CREATE VIEWS FROM SOURCE mz_source (t1);
contains:table t1 is ambiguous, consider specifying the schema

> DROP SCHEMA IF EXISTS schema1;

> CREATE SCHEMA schema1;

> CREATE VIEWS FROM SOURCE mz_source (schema1.t1);

> SELECT * FROM schema1.t1;
1
1

> DROP VIEW schema1.t1;

> DROP SCHEMA IF EXISTS schema2;

> CREATE SCHEMA schema2;

> CREATE VIEWS FROM SOURCE mz_source;

> SELECT * FROM schema1.t1;
1
1

> SELECT * FROM schema2.t1;
2
2

> CREATE VIEWS FROM SOURCE mz_source (schema1.t1 AS t1_1, schema2.t1 AS t1_2);

> SELECT * FROM t1_1;
1
1

> SELECT * FROM t1_2;
2
2

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP SCHEMA schema1 CASCADE;
DROP SCHEMA schema2 CASCADE;

> DROP SCHEMA schema1 CASCADE;

> DROP SCHEMA schema2 CASCADE;
