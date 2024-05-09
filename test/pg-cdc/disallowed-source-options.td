# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# IMPORTANT: The Postgres server has a custom pg_hba.conf that only
# accepts connections from specific users. You will have to update
# pg_hba.conf if you modify the existing user names or add new ones.

> CREATE SECRET pgpass AS 'postgres'

> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> CREATE CLUSTER cdc_cluster SIZE '${arg.default-replica-size}'

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE table_a (a int);
INSERT INTO table_a VALUES (1);
ALTER TABLE table_a REPLICA IDENTITY FULL;

! CREATE SOURCE pg_source
  IN CLUSTER cdc_cluster
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  INCLUDE KEY AS renamed_id
  FOR ALL TABLES;
contains:Kafka sources are the only source type that can provide KEY/VALUE formats

! CREATE SOURCE pg_source
  IN CLUSTER cdc_cluster
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  ENVELOPE DEBEZIUM
  FOR ALL TABLES;
contains:ENVELOPE other than NONE with PostgreSQL source is not supported

! CREATE SOURCE pg_source
  IN CLUSTER cdc_cluster
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FORMAT TEXT
  FOR ALL TABLES;
contains:FORMAT with PostgreSQL source is not supported

! CREATE SOURCE pg_source
  IN CLUSTER cdc_cluster
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  INCLUDE HEADERS
  FOR ALL TABLES;
contains:INCLUDE metadata with PostgreSQL source is not supported
