# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that Kafka sources with no format are disallowed.

> CREATE CONNECTION kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}');

! CREATE SOURCE s
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
contains:Source format must be specified

> CREATE CONNECTION no_topic TO KAFKA (BROKER '');

! CREATE SOURCE s
  FROM KAFKA CONNECTION no_topic
contains:KAFKA CONNECTION without TOPIC

# Verify that incorrect/unreachable brokers throw up an error on source creation.

> CREATE CONNECTION fawlty_kafka_conn
  TO KAFKA (BROKER 'non-existent-broker:9092');

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

! CREATE SOURCE fawlty_broker_source
  FROM KAFKA CONNECTION fawlty_kafka_conn (TOPIC 'testdrive-fawlty-broker-source-${testdrive.seed}')
contains:Meta data fetch error: BrokerTransportFailure (Local: Broker transport failure)

> CREATE CONNECTION IF NOT EXISTS fawlty_csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://non-existent-csr:8081'
  );

! CREATE SOURCE fawlty_csr_source
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-fawlty-csr-source-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION fawlty_csr_conn
contains: failed to fetch schema subject

# Check that for all tables clause is rejected
! CREATE SOURCE bad_definition1
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'any_topic')
  FORMAT BYTES FOR ALL TABLES WITH (SIZE = '1');
contains: FOR ALL TABLES is only valid for multi-output sources

# Check that for tables() clause is rejected
! CREATE SOURCE bad_definition2
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'any_topic')
  FORMAT BYTES FOR TABLES (t1);
contains: FOR TABLES (t1) is only valid for multi-output sources
