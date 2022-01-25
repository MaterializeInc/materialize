# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test hstore.handling.mode = MAP
# Not currently supported
#

$ http-request method=PUT url=http://debezium:8083/connectors/psql-connector/config content-type=application/json
{
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
  "database.history.kafka.topic": "schema-changes.history",
  "truncate.handling.mode": "include",
  "provide.transaction.metadata": "true",
  "hstore.handling.mode": "map"
}

# PUT requests do not take effect immediately, we need to sleep

> SELECT mz_internal.mz_sleep(10)
<null>

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE TABLE hstore_map (f1 hstore);
ALTER TABLE hstore_map REPLICA IDENTITY FULL;
INSERT INTO hstore_map VALUES (NULL), ('a=>1'::hstore);

$ schema-registry-wait-schema schema=postgres.public.hstore_map-value

! CREATE MATERIALIZED SOURCE hstore_map
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.hstore_map'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;
contains:Unsupported type in schema

#
# Restore default
#

$ http-request method=PUT url=http://debezium:8083/connectors/psql-connector/config content-type=application/json
{
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
  "database.history.kafka.topic": "schema-changes.history",
  "truncate.handling.mode": "include",
  "provide.transaction.metadata": "true",
  "hstore.handling.mode": "json"
}

> SELECT mz_internal.mz_sleep(10)
<null>
