# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Configure the Postgres side
#
# we could use postgres:postgres as our Debezium user, however our
# documentation suggests the creation of a dedicated user, so
# we do the same in the test.
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE USER debezium WITH SUPERUSER PASSWORD 'debezium';
GRANT ALL PRIVILEGES ON DATABASE "postgres" to debezium;
GRANT ALL PRIVILEGES ON SCHEMA "public" to debezium;

#
# Create a table and insert some data
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE t1 (f1 INTEGER);
INSERT INTO t1 VALUES (123);

#
# Configure the Debezium side
#

$ shell-execute
curl -H 'Content-Type: application/json' debezium:8083/connectors --data '{
  "name": "psql-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.password": "debezium",
    "database.dbname" : "postgres",
    "database.server.name": "postgres",
    "plugin.name": "pgoutput",
    "slot.name" : "tester",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "database.history.kafka.topic": "schema-changes.history"
  }
}'

#
# Configure the Materialize side
#

> CREATE MATERIALIZED SOURCE t1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.t1'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;

> SELECT * FROM t1;
123

#
# Perform DDL on a table being replicated
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE t1 ADD COLUMN f2 INTEGER;
INSERT INTO t1 VALUES (234, 345);

#
# The new column is not visible on the Materialize side
#

> SELECT * FROM t1
123
234
