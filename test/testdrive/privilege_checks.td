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
REVOKE SELECT ON TABLE t FROM materialize;
REVOKE CREATECLUSTER ON SYSTEM FROM materialize;
REVOKE CREATE ON CLUSTER sink_cluster FROM materialize;
DROP SINK s;
DROP SINK s1;
DROP TABLE t;
DROP CONNECTION csr_conn;
DROP CONNECTION kafka_conn;
DROP CLUSTER sink_cluster;
ALTER SYSTEM SET enable_rbac_checks TO false;
ALTER SYSTEM SET enable_ld_rbac_checks TO false;
