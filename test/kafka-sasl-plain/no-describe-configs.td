# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_kafka_config_denylist_options = true

#
# Test that sinks can be created even in the absence of the DescribeConfigs permission
#

> CREATE MATERIALIZED VIEW no_describe_configs_view AS SELECT 1;

> CREATE SECRET no_describe_configs_password AS 'password'

> CREATE CONNECTION no_describe_configs_kafka_conn TO KAFKA (
    BROKER 'kafka:9092',
    SASL MECHANISMS = 'PLAIN',
    SASL USERNAME = 'no_describe_config',
    SASL PASSWORD = SECRET no_describe_configs_password,
    SSL CERTIFICATE AUTHORITY = '${arg.ca}'
  );

> CREATE CONNECTION no_describe_configs_csr_conn
  FOR CONFLUENT SCHEMA REGISTRY
    URL '${testdrive.schema-registry-url}',
    SSL CERTIFICATE AUTHORITY = '${arg.ca}'


$ kafka-create-topic topic=no-describe-configs1

> CREATE SINK no_describe_configs_sink1
  FROM no_describe_configs_view
  INTO KAFKA CONNECTION no_describe_configs_kafka_conn (TOPIC 'testdrive-no-describe-configs1-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION no_describe_configs_csr_conn
  ENVELOPE DEBEZIUM;

$ kafka-create-topic topic=no-describe-configs2

> CREATE SINK no_describe_configs_sink2
  FROM no_describe_configs_view
  INTO KAFKA CONNECTION no_describe_configs_kafka_conn (REPLICATION FACTOR = -1 , TOPIC 'testdrive-no-describe-configs2-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION no_describe_configs_csr_conn
  ENVELOPE DEBEZIUM;

$ kafka-create-topic topic=no-describe-configs3

> CREATE SINK no_describe_configs_sink3
  FROM no_describe_configs_view
  INTO KAFKA CONNECTION no_describe_configs_kafka_conn (PARTITION COUNT = -1 , TOPIC 'testdrive-no-describe-configs3-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION no_describe_configs_csr_conn
  ENVELOPE DEBEZIUM;
