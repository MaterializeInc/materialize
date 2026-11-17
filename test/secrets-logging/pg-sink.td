# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP PUBLICATION IF EXISTS pgsink_logging_publication;
DROP SCHEMA IF EXISTS pgsink_logging CASCADE;
DROP SCHEMA IF EXISTS materialize CASCADE;
DO $$ BEGIN CREATE ROLE pgsink_logging; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
ALTER ROLE pgsink_logging LOGIN PASSWORD 'pgsink-private-password';
GRANT CREATE ON DATABASE postgres TO pgsink_logging;
CREATE SCHEMA pgsink_logging AUTHORIZATION pgsink_logging;

> CREATE SECRET pgsink_pass AS 'pgsink-private-password'
> CREATE CONNECTION pgsink_conn TO POSTGRES
  (HOST postgres, DATABASE postgres, USER pgsink_logging, PASSWORD SECRET pgsink_pass)
> CREATE TABLE pgsink_input (a int NOT NULL, b text)
> INSERT INTO pgsink_input VALUES (1, 'pgsink-private-payload')
> CREATE SINK pgsink_logging_sink FROM pgsink_input
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA 'pgsink_logging', TABLE 'target')

> SELECT write_frontier > 0 FROM mz_internal.mz_frontiers
  WHERE object_id = (SELECT id FROM mz_sinks WHERE name = 'pgsink_logging_sink')
true

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE PUBLICATION pgsink_logging_publication FOR TABLE pgsink_logging.target;

> CREATE SECRET pgsink_admin_pass AS 'postgres'
> CREATE CONNECTION pgsink_admin TO POSTGRES
  (HOST postgres, DATABASE postgres, USER postgres, PASSWORD SECRET pgsink_admin_pass)
> CREATE SOURCE pgsink_readback_source FROM POSTGRES CONNECTION pgsink_admin
  (PUBLICATION 'pgsink_logging_publication')
> CREATE TABLE pgsink_readback FROM SOURCE pgsink_readback_source (REFERENCE pgsink_logging.target)
> SELECT * FROM pgsink_readback
1 pgsink-private-payload

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE pgsink_logging.target ADD CONSTRAINT reject_large_values CHECK (a < 10);

> INSERT INTO pgsink_input VALUES (10, 'pgsink-private-rejected-payload')
> SELECT status, error LIKE '%check constraint%' FROM mz_internal.mz_sink_statuses
  WHERE name = 'pgsink_logging_sink'
stalled true
> ALTER SECRET pgsink_pass AS 'pgsink-private-wrong-password'
> CREATE SINK pgsink_auth_sink FROM pgsink_input
  INTO POSTGRES CONNECTION pgsink_conn (SCHEMA 'pgsink_logging', TABLE 'auth_target')
> SELECT status, error LIKE '%password authentication failed%' FROM mz_internal.mz_sink_statuses
  WHERE name = 'pgsink_auth_sink'
stalled true
> SELECT count(*) FROM mz_internal.mz_sink_status_history
  WHERE sink_id IN (SELECT id FROM mz_sinks WHERE name IN ('pgsink_logging_sink', 'pgsink_auth_sink'))
    AND (error LIKE '%pgsink-private-%' OR details::text LIKE '%pgsink-private-%')
0

> DROP SINK pgsink_auth_sink
> DROP SINK pgsink_logging_sink
> DROP SOURCE pgsink_readback_source CASCADE
