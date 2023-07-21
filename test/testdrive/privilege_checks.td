# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test privilege checks of creating sinks. All other tests are implemented in SQLogicTests

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_connection_validation_syntax = true

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_rbac_checks TO true;
ALTER SYSTEM SET enable_ld_rbac_checks TO true;
CREATE CONNECTION kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}');
CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${testdrive.schema-registry-url}');
CREATE TABLE t (a INT);
REVOKE USAGE ON SCHEMA materialize.public FROM PUBLIC;
REVOKE USAGE ON DATABASE materialize FROM PUBLIC;
REVOKE USAGE ON CLUSTER default FROM PUBLIC;
REVOKE ALL PRIVILEGES ON SCHEMA materialize.public FROM materialize;
REVOKE ALL PRIVILEGES ON DATABASE materialize FROM materialize;
REVOKE ALL PRIVILEGES ON CLUSTER default FROM materialize;
REVOKE ALL PRIVILEGES ON SYSTEM FROM materialize;

# CREATE SINK

## WITH SIZE

! CREATE SINK s FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
  WITH (SIZE '1')
contains:permission denied for CONNECTION "materialize.public.kafka_conn"

$ postgres-execute connection=mz_system
GRANT USAGE ON CONNECTION kafka_conn TO materialize;

! CREATE SINK s FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
  WITH (SIZE '1')
contains:permission denied for CONNECTION "materialize.public.csr_conn"

$ postgres-execute connection=mz_system
GRANT USAGE ON CONNECTION csr_conn TO materialize;

! CREATE SINK s FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
  WITH (SIZE '1')
contains:permission denied for SCHEMA "materialize.public"

$ postgres-execute connection=mz_system
GRANT CREATE ON SCHEMA materialize.public TO materialize;

! CREATE SINK s FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
  WITH (SIZE '1')
contains:permission denied for SCHEMA "materialize.public"

$ postgres-execute connection=mz_system
GRANT USAGE ON SCHEMA materialize.public TO materialize;

! CREATE SINK s FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
  WITH (SIZE '1')
contains:permission denied for TABLE "materialize.public.t"

$ postgres-execute connection=mz_system
GRANT SELECT ON TABLE t TO materialize;

! CREATE SINK s FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
  WITH (SIZE '1')
contains:permission denied for SYSTEM

$ postgres-execute connection=mz_system
GRANT CREATECLUSTER ON SYSTEM TO materialize;

> CREATE SINK s FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
  WITH (SIZE '1')

$ postgres-execute connection=mz_system
REVOKE CREATE ON SCHEMA materialize.public FROM materialize;

! CREATE SINK s1 FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
  WITH (SIZE '1')
contains:permission denied for SCHEMA "materialize.public"

$ postgres-execute connection=mz_system
GRANT CREATE ON SCHEMA materialize.public TO materialize;

## IN CLUSTER

$ postgres-execute connection=mz_system
CREATE CLUSTER sink_cluster REPLICAS (r1 (SIZE '1'));

! CREATE SINK s1 IN CLUSTER sink_cluster FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
contains:permission denied for CLUSTER "sink_cluster"

$ postgres-execute connection=mz_system
GRANT CREATE ON CLUSTER sink_cluster TO materialize;

> CREATE SINK s1 IN CLUSTER sink_cluster FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

$ postgres-execute connection=mz_system
REVOKE CREATE, USAGE ON SCHEMA materialize.public FROM materialize;
REVOKE USAGE ON CONNECTION kafka_conn, csr_conn FROM materialize;
REVOKE SELECT ON TABLE t FROM materialize;
REVOKE CREATECLUSTER ON SYSTEM FROM materialize;
REVOKE CREATE ON CLUSTER sink_cluster FROM materialize;
DROP SINK s;
DROP SINK s1;
DROP TABLE t;
DROP CLUSTER sink_cluster;

# CREATE CONNECTION

$ postgres-execute connection=mz_system
CREATE SECRET confluent_username AS 'materialize';
CREATE SECRET confluent_password AS 'password';

! CREATE CONNECTION conn TO KAFKA (
      BROKER '${testdrive.kafka-addr}',
      SASL MECHANISMS = 'PLAIN',
      SASL USERNAME = SECRET confluent_username,
      SASL PASSWORD = SECRET confluent_password
  ) WITH (VALIDATE = false);
contains:permission denied for SECRET "materialize.public.confluent_username"

$ postgres-execute connection=mz_system
GRANT USAGE ON SECRET confluent_username TO materialize;

! CREATE CONNECTION conn TO KAFKA (
      BROKER '${testdrive.kafka-addr}',
      SASL MECHANISMS = 'PLAIN',
      SASL USERNAME = SECRET confluent_username,
      SASL PASSWORD = SECRET confluent_password
  ) WITH (VALIDATE = false);
contains:permission denied for SECRET "materialize.public.confluent_password"

$ postgres-execute connection=mz_system
GRANT USAGE ON SECRET confluent_password TO materialize;

! CREATE CONNECTION conn TO KAFKA (
      BROKER '${testdrive.kafka-addr}',
      SASL MECHANISMS = 'PLAIN',
      SASL USERNAME = SECRET confluent_username,
      SASL PASSWORD = SECRET confluent_password
  ) WITH (VALIDATE = false);
contains:permission denied for SCHEMA "materialize.public"

$ postgres-execute connection=mz_system
GRANT CREATE ON SCHEMA materialize.public TO materialize;

> CREATE CONNECTION conn TO KAFKA (
      BROKER '${testdrive.kafka-addr}',
      SASL MECHANISMS = 'PLAIN',
      SASL USERNAME = SECRET confluent_username,
      SASL PASSWORD = SECRET confluent_password
  ) WITH (VALIDATE = false);

$ postgres-execute connection=mz_system
REVOKE CREATE ON SCHEMA materialize.public FROM materialize;
REVOKE USAGE ON SECRET confluent_username, confluent_password FROM materialize;
DROP CONNECTION conn;
DROP SECRET confluent_username;
DROP SECRET confluent_password;

## CREATE SOURCE

$ kafka-create-topic topic=rbac partitions=1

$ set int-schema={"type": "record", "name": "schema_int", "fields": [ {"name": "f1", "type": "int"} ] }

$ kafka-ingest format=avro topic=rbac schema=${int-schema} timestamp=1
{"f1": 123}

## WITH SIZE

! CREATE SOURCE s
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-rbac-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
  WITH (SIZE '1')
contains:permission denied for CONNECTION "materialize.public.kafka_conn"

$ postgres-execute connection=mz_system
GRANT USAGE ON CONNECTION kafka_conn TO materialize;

! CREATE SOURCE s
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-rbac-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
  WITH (SIZE '1')
contains:permission denied for CONNECTION "materialize.public.csr_conn"

$ postgres-execute connection=mz_system
GRANT USAGE ON CONNECTION csr_conn TO materialize;

! CREATE SOURCE s
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-rbac-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
  WITH (SIZE '1')
contains:permission denied for SCHEMA "materialize.public"

$ postgres-execute connection=mz_system
GRANT CREATE ON SCHEMA materialize.public TO materialize;

! CREATE SOURCE s
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-rbac-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
  WITH (SIZE '1')
contains:permission denied for SYSTEM

$ postgres-execute connection=mz_system
GRANT CREATECLUSTER ON SYSTEM TO materialize;

> CREATE SOURCE s
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-rbac-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
  WITH (SIZE '1')

## IN CLUSTER

$ postgres-execute connection=mz_system
CREATE CLUSTER source_cluster REPLICAS (r1 (SIZE '1'));

! CREATE SOURCE s1 IN CLUSTER source_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-rbac-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
contains:permission denied for CLUSTER "source_cluster"

$ postgres-execute connection=mz_system
GRANT CREATE ON CLUSTER source_cluster TO materialize;

> CREATE SOURCE s1 IN CLUSTER source_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-rbac-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

$ postgres-execute connection=mz_system
REVOKE CREATE ON SCHEMA materialize.public FROM materialize;
REVOKE USAGE ON CONNECTION kafka_conn, csr_conn FROM materialize;
REVOKE CREATECLUSTER ON SYSTEM FROM materialize;
REVOKE CREATE ON CLUSTER source_cluster FROM materialize;
DROP SOURCE s;
DROP SOURCE s1;
DROP CLUSTER source_cluster;

$ postgres-execute connection=mz_system
DROP CONNECTION csr_conn;
DROP CONNECTION kafka_conn;
ALTER SYSTEM SET enable_rbac_checks TO false;
ALTER SYSTEM SET enable_ld_rbac_checks TO false;
