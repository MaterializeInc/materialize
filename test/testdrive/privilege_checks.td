# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test privilege checks of creating sinks. All other tests are implemented in SQLogicTests

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_rbac_checks TO true;
ALTER SYSTEM SET enable_ld_rbac_checks TO true;
CREATE CONNECTION kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}');
CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${testdrive.schema-registry-url}');
CREATE TABLE t (a INT);
ALTER ROLE materialize CREATECLUSTER;
REVOKE USAGE ON SCHEMA materialize.public FROM PUBLIC;
REVOKE USAGE ON DATABASE materialize FROM PUBLIC;
REVOKE USAGE ON CLUSTER default FROM PUBLIC;
REVOKE CREATE ON SCHEMA materialize.public FROM materialize;
REVOKE CREATE ON DATABASE materialize FROM materialize;
REVOKE CREATE ON CLUSTER default FROM materialize;

! CREATE SINK s FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
contains:permission denied for SCHEMA materialize.public

$ postgres-execute connection=mz_system
GRANT CREATE ON SCHEMA materialize.public TO materialize;

! CREATE SINK s FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
contains:permission denied for SCHEMA materialize.public

$ postgres-execute connection=mz_system
GRANT USAGE ON SCHEMA materialize.public TO materialize;

! CREATE SINK s FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
contains:permission denied for TABLE materialize.public.t

$ postgres-execute connection=mz_system
GRANT SELECT ON TABLE t TO materialize;

! CREATE SINK s FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
contains:permission denied for CLUSTER default

$ postgres-execute connection=mz_system
GRANT CREATE ON CLUSTER default TO materialize;

> CREATE SINK s FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

$ postgres-execute connection=mz_system
REVOKE CREATE ON schema materialize.public FROM materialize;

! CREATE SINK s2 FROM t
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'output-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM
contains:permission denied for SCHEMA materialize.public

$ postgres-execute connection=mz_system
REVOKE CREATE, USAGE ON SCHEMA materialize.public FROM materialize;
REVOKE SELECT ON TABLE t FROM materialize;
REVOKE CREATE ON CLUSTER default FROM materialize;
DROP SINK s;
DROP TABLE t;
DROP CONNECTION csr_conn;
DROP CONNECTION kafka_conn;
ALTER SYSTEM SET enable_rbac_checks TO false;
ALTER SYSTEM SET enable_ld_rbac_checks TO false;