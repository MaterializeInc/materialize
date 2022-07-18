# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that a publication WITH (publish = 'XXX') option is handled
# correctly.
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

CREATE TABLE t1 (f1 INTEGER PRIMARY KEY);
ALTER TABLE t1 REPLICA IDENTITY FULL;

CREATE PUBLICATION mz_source_insert FOR TABLE t1 WITH ( publish = 'insert') ;
CREATE PUBLICATION mz_source_update FOR TABLE t1 WITH ( publish = 'update') ;
CREATE PUBLICATION mz_source_delete FOR TABLE t1 WITH ( publish = 'delete') ;

> CREATE SOURCE mz_source_insert
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source_insert';

> CREATE SOURCE mz_source_update
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source_update';

> CREATE SOURCE mz_source_delete
  FROM POSTGRES CONNECTION pg
  PUBLICATION 'mz_source_delete';

# Make sure that the above sources are fully instantiated so that the DML statements below
# are sent as actual replication events post-snapshot.

> SELECT COUNT(*) FROM mz_source_delete;
0

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 VALUES (1);
UPDATE t1 SET f1 = 2 WHERE f1 = 1;
DELETE FROM t1;

> CREATE VIEWS FROM SOURCE mz_source_insert (t1 AS t1_insert);

> CREATE VIEWS FROM SOURCE mz_source_update (t1 AS t1_update);

> CREATE VIEWS FROM SOURCE mz_source_delete (t1 AS t1_delete);

> SELECT * FROM t1_insert;
1

! SELECT * FROM t1_update;
contains:Invalid data in source, saw retractions (1) for row that does not exist

! SELECT * FROM t1_delete;
contains:Invalid data in source, saw retractions (1) for row that does not exist
