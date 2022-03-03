# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": [
      {
        "name": "before",
        "type": [
          {
            "name": "row",
            "type": "record",
            "fields": [
              {"name": "a", "type": "long"}
            ]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} publish=true timestamp=1
{"before": null, "after": {"row": {"a": 1}}}

> CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      security_protocol = 'SSL',
      ssl_key_location = '/share/secrets/materialized.key',
      ssl_certificate_location = '/share/secrets/materialized.crt',
      ssl_ca_location = '/share/secrets/ca.crt',
      ssl_key_password = 'mzmzmz'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_location = '/share/secrets/materialized.key',
      ssl_certificate_location = '/share/secrets/materialized.crt',
      ssl_ca_location = '/share/secrets/ca.crt',
      username = "materialize",
      password = "sekurity"
  )
  ENVELOPE DEBEZIUM

! CREATE SINK env_pw_snk FROM data
  INTO KAFKA BROKER 'kafka' TOPIC 'snk'
  WITH (
      security_protocol = 'SSL',
      ssl_key_location = '/share/secrets/materialized.key',
      ssl_certificate_location = '/share/secrets/materialized.crt',
      ssl_ca_location = '/share/secrets/ca.crt',
      ssl_key_password = 'mzmzmz',
      ssl_key_password_env = 'SSL_KEY_PASSWORD'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_location = '/share/secrets/materialized.key',
      ssl_certificate_location = '/share/secrets/materialized.crt',
      ssl_ca_location = '/share/secrets/ca.crt',
      username = "materialize",
      password = "sekurity"
  )
contains:Invalid WITH options: cannot specify both ssl_key_password and ssl_key_password_env options at the same time

! CREATE SINK env_pw_snk FROM data
  INTO KAFKA BROKER 'kafka' TOPIC 'snk'
  WITH (
      security_protocol = 'SSL',
      ssl_key_location = '/share/secrets/materialized.key',
      ssl_certificate_location = '/share/secrets/materialized.crt',
      ssl_ca_location = '/share/secrets/ca.crt',
      ssl_key_password_env = 'SSL_KEY_PASSWORD',
      ssl_key_password = 'mzmzmz'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_location = '/share/secrets/materialized.key',
      ssl_certificate_location = '/share/secrets/materialized.crt',
      ssl_ca_location = '/share/secrets/ca.crt',
      username = "materialize",
      password = "sekurity"
  )
contains:Invalid WITH options: cannot specify both ssl_key_password and ssl_key_password_env options at the same time

! CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      security_protocol = 'SASL_SSL',
      sasl_mechanisms = 'PLAIN',
      sasl_username = 'materialize',
      ssl_key_password_env = 'DNE',
      ssl_ca_location = '/share/secrets/ca.crt'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      username = "materialize",
      password = "sekurity"
  )
  ENVELOPE DEBEZIUM
contains:Invalid WITH option ssl_key_password_env='DNE': environment variable not found
