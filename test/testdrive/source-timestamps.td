# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests to verify that source timestamps are rounded to the timestamp interval.

> CREATE CLUSTER test SIZE '1'

$ kafka-create-topic topic=test

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)
> CREATE SOURCE kafka_src
  IN CLUSTER test
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-test-${testdrive.seed}')

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP PUBLICATION IF EXISTS mz_source
CREATE TABLE t (x int);
ALTER TABLE t REPLICA IDENTITY FULL;
CREATE PUBLICATION mz_source FOR ALL TABLES

> CREATE SECRET pg_pass AS 'postgres'
> CREATE CONNECTION pg_conn TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pg_pass
  )
> CREATE SOURCE pg_src
  IN CLUSTER test
  FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'mz_source')

$ mysql-connect name=mysql url=mysql://root@mysql password=p@ssw0rd

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE t (x int)

> CREATE SECRET mysql_pass AS 'p@ssw0rd';
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysql_pass
  )
> CREATE SOURCE mysql_src
  IN CLUSTER test
  FROM MYSQL CONNECTION mysql_conn;

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET kafka_default_metadata_fetch_interval = '1s'
ALTER SYSTEM SET pg_offset_known_interval = '1s'
ALTER SYSTEM SET mysql_offset_known_interval = '1s'

> SELECT name, write_frontier::text::uint8 % 1000 = 1
  FROM mz_internal.mz_frontiers
  JOIN mz_sources ON object_id = id
  WHERE id LIKE 'u%'
kafka_src          true
kafka_src_progress true
pg_src             true
pg_src_progress    true
mysql_src          true
mysql_src_progress true

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET kafka_default_metadata_fetch_interval = 1234
ALTER SYSTEM SET pg_offset_known_interval = 1234
ALTER SYSTEM SET mysql_offset_known_interval = 1234

> SELECT name, write_frontier::text::uint8 % 1234 = 1
  FROM mz_internal.mz_frontiers
  JOIN mz_sources ON object_id = id
  WHERE id LIKE 'u%'
kafka_src          true
kafka_src_progress true
pg_src             true
pg_src_progress    true
mysql_src          true
mysql_src_progress true

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET kafka_default_metadata_fetch_interval = 500
ALTER SYSTEM SET pg_offset_known_interval = 500
ALTER SYSTEM SET mysql_offset_known_interval = 500

> SELECT name, write_frontier::text::uint8 % 500 = 1
  FROM mz_internal.mz_frontiers
  JOIN mz_sources ON object_id = id
  WHERE id LIKE 'u%'
kafka_src          true
kafka_src_progress true
pg_src             true
pg_src_progress    true
mysql_src          true
mysql_src_progress true
