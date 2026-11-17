# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP PUBLICATION IF EXISTS pgsink_names_publication;
DROP SCHEMA IF EXISTS pgsink_names CASCADE;
DROP SCHEMA IF EXISTS materialize CASCADE;
CREATE SCHEMA pgsink_names;
CREATE PUBLICATION pgsink_names_publication FOR ALL TABLES;

> CREATE SECRET pgsink_pass AS 'postgres'
> CREATE CONNECTION pgsink_conn TO POSTGRES
  (HOST postgres, DATABASE postgres, USER postgres, PASSWORD SECRET pgsink_pass)
> CREATE TABLE pgsink_input (n int)
> INSERT INTO pgsink_input VALUES (1), (1), (2)
> CREATE SINK pgsink_names FROM pgsink_input
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA 'pgsink_names', TABLE 'target')

> SELECT s.status, s.error, f.write_frontier > 0
  FROM mz_internal.mz_sink_statuses s JOIN mz_internal.mz_frontiers f ON f.object_id = s.id
  WHERE s.name = 'pgsink_names'
running <null> true

> CREATE SOURCE pgsink_readback_source FROM POSTGRES CONNECTION pgsink_conn
  (PUBLICATION 'pgsink_names_publication')
> CREATE TABLE pgsink_readback FROM SOURCE pgsink_readback_source (REFERENCE pgsink_names.target)
> SELECT * FROM pgsink_readback
1
1
2

> DELETE FROM pgsink_input WHERE n = 2
> UPDATE pgsink_input SET n = 3 WHERE n = 1
> SELECT * FROM pgsink_readback
3
3
