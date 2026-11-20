# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test the INET and CIDR types
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

CREATE TABLE t1 (i INET, c CIDR, ia INET[]);
ALTER TABLE t1 REPLICA IDENTITY FULL;

INSERT INTO t1 VALUES ('192.168.1.226/24', '192.168.1.0/24', '{10.0.0.1,::1}');
INSERT INTO t1 VALUES ('::ffff:1.2.3.4', '10:23::/32', NULL);
INSERT INTO t1 VALUES (NULL, NULL, '{}');

ANALYZE;

CREATE PUBLICATION mz_source FOR ALL TABLES;

# With the flag off the columns are unsupported, and TEXT COLUMNS still works.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_network_types = false

> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

! CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);
contains:referenced tables use unsupported types

> CREATE TABLE t1_text FROM SOURCE mz_source (REFERENCE t1) WITH (TEXT COLUMNS (i, c, ia));

> SELECT i, c FROM t1_text WHERE i IS NOT NULL ORDER BY i;
192.168.1.226/24 192.168.1.0/24
::ffff:1.2.3.4 10:23::/32

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_network_types = true

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);

> SELECT COUNT(*) FROM t1;
3

# Insert the same data post-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 SELECT * FROM t1;

> SELECT pg_typeof(i), pg_typeof(c), pg_typeof(ia) FROM t1 LIMIT 1;
inet cidr inet[]

> SELECT i, c, ia, i << c FROM t1;
192.168.1.226/24 192.168.1.0/24 {10.0.0.1,::1} true
192.168.1.226/24 192.168.1.0/24 {10.0.0.1,::1} true
::ffff:1.2.3.4 10:23::/32 <null> false
::ffff:1.2.3.4 10:23::/32 <null> false
<null> <null> {} <null>
<null> <null> {} <null>
