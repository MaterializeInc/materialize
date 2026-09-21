# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Add a table to a source whose replication slot is parked before an upstream
# schema change. Replaying the slot hands the new table a Relation message that
# describes a column layout it never captured.
#

> CREATE CLUSTER storage SIZE '${arg.default-replica-size}', REPLICATION FACTOR 1

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE t (a int);
ALTER TABLE t REPLICA IDENTITY FULL;
INSERT INTO t VALUES (1);

ANALYZE;

> CREATE SOURCE mz_source
  IN CLUSTER storage
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')

> CREATE TABLE t_one_column FROM SOURCE mz_source (REFERENCE public.t)

> SELECT * FROM t_one_column
1

# Removing the replicas stops the LSN feedback, so the slot's
# confirmed_flush_lsn stays where it is and everything written upstream from
# here on has to be replayed once a replica comes back.
> ALTER CLUSTER storage SET (REPLICATION FACTOR 0)

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t VALUES (2);
ALTER TABLE t ADD COLUMN b int;
INSERT INTO t VALUES (3, 3);

# This table captures the two-column schema, but the replay starts at an LSN
# where `t` still had one column.
> CREATE TABLE t_two_columns FROM SOURCE mz_source (REFERENCE public.t)

> ALTER CLUSTER storage SET (REPLICATION FACTOR 1)

> SELECT * FROM t_one_column
1
2
3

> SELECT * FROM t_two_columns
1 <null>
2 <null>
3 3

# Both tables keep up with the upstream once the replay is done.
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t VALUES (4, 4);

> SELECT * FROM t_one_column
1
2
3
4

> SELECT * FROM t_two_columns
1 <null>
2 <null>
3 3
4 4

> DROP SOURCE mz_source CASCADE
