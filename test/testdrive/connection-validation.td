# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ skip-consistency-checks reason="workflow uses SSH keys which we currently can't check"

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_connection_validation_syntax = true;
ALTER SYSTEM SET enable_rbac_checks = true;

> CREATE CONNECTION kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

> VALIDATE CONNECTION kafka_conn

> DROP CONNECTION kafka_conn

# Explicitly enable validation

> CREATE CONNECTION kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT) WITH (VALIDATE = true)

> VALIDATE CONNECTION kafka_conn

> DROP CONNECTION kafka_conn

# WITH (VALIDATE) variant

> CREATE CONNECTION kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT) WITH (VALIDATE)

> VALIDATE CONNECTION kafka_conn

> DROP CONNECTION kafka_conn

> CREATE CONNECTION invalid_tunnel TO SSH TUNNEL (HOST 'invalid', USER 'invalid', PORT 22)

! CREATE CONNECTION invalid_kafka_conn TO KAFKA (BROKERS ('${testdrive.kafka-addr}' USING SSH TUNNEL invalid_tunnel), SECURITY PROTOCOL PLAINTEXT)
contains:failed to lookup address information

# Create the connection without validation and validate later
> CREATE CONNECTION invalid_kafka_conn TO KAFKA (BROKERS ('${testdrive.kafka-addr}' USING SSH TUNNEL invalid_tunnel), SECURITY PROTOCOL PLAINTEXT) WITH (VALIDATE = false)

! VALIDATE CONNECTION invalid_kafka_conn
contains:failed to lookup address information

# Altering options authorizes the resulting definition, while validating an
# unchanged connection delegates access to its dependencies.

$ postgres-execute connection=mz_system
CREATE ROLE connection_validator_${testdrive.seed} LOGIN PASSWORD 'validatorpass';
CREATE SECRET alter_connection_password AS 'password';
CREATE CONNECTION secret_csr_conn TO CONFLUENT SCHEMA REGISTRY (URL '${testdrive.schema-registry-url}', USERNAME 'user', PASSWORD SECRET alter_connection_password) WITH (VALIDATE = true);
ALTER CONNECTION secret_csr_conn OWNER TO materialize;
GRANT USAGE ON SCHEMA materialize.public TO materialize, connection_validator_${testdrive.seed};
GRANT USAGE ON CONNECTION secret_csr_conn TO connection_validator_${testdrive.seed};

! ALTER CONNECTION secret_csr_conn SET (URL '${testdrive.schema-registry-url}') WITH (VALIDATE = true)
contains:permission denied for SECRET "materialize.public.alter_connection_password"

$ postgres-connect name=connection_validator url=postgres://connection_validator_${testdrive.seed}:validatorpass@${testdrive.materialize-sql-addr}

$ postgres-execute connection=connection_validator
VALIDATE CONNECTION secret_csr_conn
