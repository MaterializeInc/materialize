# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE table_mysql (a int);

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    PORT 3306,
    USER root,
    PASSWORD SECRET mysqlpass
  )

> BEGIN
> CREATE SOURCE mysql_source
  FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE table_mysql FROM SOURCE mysql_source (REFERENCE public.table_mysql);
> COMMIT

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE table_pg (a int);
ALTER TABLE table_pg REPLICA IDENTITY FULL;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SECRET pgpass AS 'postgres'

> CREATE CONNECTION pg_conn TO POSTGRES (
    HOST postgres,
    PORT 5432,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> BEGIN
> CREATE SOURCE pg_source
  FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'mz_source');
> CREATE TABLE table_pg FROM SOURCE pg_source (REFERENCE table_pg);
> COMMIT

$ kafka-create-topic topic=input

$ kafka-ingest topic=input format=bytes repeat=1
0

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER 'kafka:9092', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE input_kafka
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-input-${testdrive.seed}')

> CREATE TABLE input_kafka_tbl (a) FROM SOURCE input_kafka (REFERENCE "testdrive-input-${testdrive.seed}")
  FORMAT CSV WITH 1 COLUMNS
> COMMIT

> CREATE MATERIALIZED VIEW sum AS
  SELECT sum(count)
  FROM (
      SELECT count(*) FROM table_mysql
      UNION ALL SELECT count(*) FROM table_pg
      UNION ALL SELECT count(*) FROM input_kafka_tbl
  ) AS x;

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET allow_real_time_recency = true
