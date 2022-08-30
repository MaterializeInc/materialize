# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Start with decimal.handling.mode PRECISE
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE decimal_handling_mode_precise (f1 DECIMAL(10,3) PRIMARY KEY);
INSERT INTO decimal_handling_mode_precise VALUES (1234567.890);

$ schema-registry-wait-schema schema=postgres.public.decimal_handling_mode_precise-value

> CREATE CONNECTION IF NOT EXISTS csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
  URL '${testdrive.schema-registry-url}';

> CREATE SOURCE decimal_handling_mode_precise
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.decimal_handling_mode_precise'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

> SELECT f1, pg_typeof(f1) FROM decimal_handling_mode_precise
1234567.89 numeric

#
# Set decimal.handling.mode to DOUBLE
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
  "decimal.handling.mode": "double"
}

# PUT requests do not take effect immediately, we need to sleep

> SELECT mz_internal.mz_sleep(10)
<null>

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE decimal_handling_mode_double (f1 DECIMAL(10,3) PRIMARY KEY);
INSERT INTO decimal_handling_mode_double VALUES (2234567.890);

$ schema-registry-wait-schema schema=postgres.public.decimal_handling_mode_double-value

> CREATE SOURCE decimal_handling_mode_double
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.decimal_handling_mode_double'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

> SELECT f1, pg_typeof(f1) FROM decimal_handling_mode_double;
2234567.89 "double precision"

#
# Set decimal.handling.mode to STRING
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
  "decimal.handling.mode": "string"
}

> SELECT mz_internal.mz_sleep(10)
<null>

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE decimal_handling_mode_string (f1 DECIMAL(10,3) PRIMARY KEY);
INSERT INTO decimal_handling_mode_string VALUES (3234567.890);

$ schema-registry-wait-schema schema=postgres.public.decimal_handling_mode_string-value

> CREATE SOURCE decimal_handling_mode_string
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.decimal_handling_mode_string'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM;

> SELECT f1, pg_typeof(f1) FROM decimal_handling_mode_string;
3234567.890 text

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
  "decimal.handling.mode": "precise"
}

> SELECT mz_internal.mz_sleep(10)
<null>
