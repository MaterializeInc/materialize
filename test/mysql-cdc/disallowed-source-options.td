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

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_create_source_denylist_with_options = true

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

> CREATE CLUSTER cdc_cluster SIZE '${arg.default-replica-size}'

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE table_a (a INTEGER);
INSERT INTO table_a VALUES (1);

! CREATE SOURCE mysql_source
  IN CLUSTER cdc_cluster
  FROM MYSQL CONNECTION mysql_conn
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  INCLUDE KEY AS renamed_id
  FOR ALL TABLES;
contains:Kafka sources are the only source type that can provide KEY/VALUE formats

! CREATE SOURCE mysql_source
  IN CLUSTER cdc_cluster
  FROM MYSQL CONNECTION mysql_conn
  ENVELOPE DEBEZIUM
  FOR ALL TABLES;
contains:ENVELOPE other than NONE with MySQL source is not supported

! CREATE SOURCE mysql_source
  IN CLUSTER cdc_cluster
  FROM MYSQL CONNECTION mysql_conn
  FORMAT TEXT
  FOR ALL TABLES;
contains:FORMAT with MySQL source is not supported

! CREATE SOURCE mysql_source
  IN CLUSTER cdc_cluster
  FROM MYSQL CONNECTION mysql_conn
  INCLUDE HEADERS
  FOR ALL TABLES;
contains:INCLUDE metadata with MySQL source is not supported
