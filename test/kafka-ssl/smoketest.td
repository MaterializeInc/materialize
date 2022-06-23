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

> CREATE SECRET ssl_key_kafka AS '${arg.materialized-kafka-key}'
> CREATE SECRET ssl_key_password_kafka AS 'mzmzmz'
> CREATE SECRET ssl_key_csr AS '${arg.materialized-schema-registry-key}'
> CREATE SECRET password_csr AS 'sekurity'

> CREATE TABLE not_a_secret (a int);

! CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      ssl_key_pem = SECRET not_a_secret
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM
contains: materialize.public.not_a_secret is not a secret

> CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      security_protocol = 'SSL',
      ssl_key_pem = SECRET ssl_key_kafka,
      ssl_certificate_pem = '${arg.materialized-kafka-crt}',
      ssl_ca_pem = '${arg.ca-crt}',
      ssl_key_password = SECRET ssl_key_password_kafka
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_pem = SECRET ssl_key_csr,
      ssl_certificate_pem = '${arg.materialized-schema-registry-crt}',
      ssl_ca_pem = '${arg.ca-crt}',
      username = "materialize",
      password = SECRET password_csr
  )
  ENVELOPE DEBEZIUM

> SELECT * FROM data
a
---
1

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=2
{"before": null, "after": {"row": {"a": 2}}}

> SELECT * FROM data
a
---
1
2

> CREATE SINK snk FROM data
  INTO KAFKA BROKER 'kafka' TOPIC 'snk'
  WITH (
      security_protocol = 'SSL',
      ssl_key_pem = SECRET ssl_key_kafka,
      ssl_certificate_pem = '${arg.materialized-kafka-crt}',
      ssl_ca_pem = '${arg.ca-crt}',
      ssl_key_password = SECRET ssl_key_password_kafka
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_pem = SECRET ssl_key_csr,
      ssl_certificate_pem = '${arg.materialized-schema-registry-crt}',
      ssl_ca_pem = '${arg.ca-crt}',
      username = "materialize",
      password = SECRET password_csr
  )

$ kafka-verify format=avro sink=materialize.public.snk sort-messages=true
{"before": null, "after": {"row":{"a": 1}}}
{"before": null, "after": {"row":{"a": 2}}}

# Check environment variables; defined in mzcompose.yml

> CREATE SINK env_pw_snk FROM data
  INTO KAFKA BROKER 'kafka' TOPIC 'snk'
  WITH (
      security_protocol = 'SSL',
      ssl_key_pem = SECRET ssl_key_kafka,
      ssl_certificate_pem = '${arg.materialized-kafka-crt}',
      ssl_ca_pem = '${arg.ca-crt}',
      ssl_key_password = SECRET ssl_key_password_kafka
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_pem = SECRET ssl_key_csr,
      ssl_certificate_pem = '${arg.materialized-schema-registry-crt}',
      ssl_ca_pem = '${arg.ca-crt}',
      username = "materialize",
      password = SECRET password_csr
  )
  ENVELOPE DEBEZIUM

$ kafka-verify format=avro sink=materialize.public.env_pw_snk sort-messages=true
{"before": null, "after": {"row":{"a": 1}}}
{"before": null, "after": {"row":{"a": 2}}}

# not basic_auth
! CREATE SINK no_basic_auth FROM data
  INTO KAFKA BROKER 'kafka' TOPIC 'snk'
  WITH (
      security_protocol = 'SSL',
      ssl_key_pem = SECRET ssl_key_kafka,
      ssl_certificate_pem = '${arg.materialized-kafka-crt}',
      ssl_ca_pem = '${arg.ca-crt}',
      ssl_key_password = SECRET ssl_key_password_kafka
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_pem = SECRET ssl_key_csr,
      ssl_certificate_pem = '${arg.materialized-schema-registry-crt}',
      ssl_ca_pem = '${arg.ca-crt}'
  )
  ENVELOPE DEBEZIUM
contains:error publishing kafka schemas for sink: unable to publish value schema to registry in kafka sink: server error 401: Unauthorized

# Ensure that we get an ssl error if we forget to set certs
! CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      security_protocol = 'SSL',
      ssl_key_pem = SECRET ssl_key_kafka,
      ssl_certificate_pem = '${arg.materialized-kafka-crt}',
      ssl_ca_pem = '${arg.ca-crt}',
      ssl_key_password = SECRET ssl_key_password_kafka
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM
contains:self signed certificate in certificate chain

# Ensure that connectors work with SSL basic_auth
> CREATE CONNECTION kafka_ssl
  FOR KAFKA BROKER 'kafka:9092'
  WITH (
      security_protocol = 'SSL',
      ssl_key_pem = SECRET ssl_key_kafka,
      ssl_certificate_pem = '${arg.materialized-kafka-crt}',
      ssl_ca_pem = '${arg.ca-crt}',
      ssl_key_password = SECRET ssl_key_password_kafka
  )

> CREATE MATERIALIZED SOURCE connector_source
  FROM KAFKA CONNECTION kafka_ssl
  TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_pem = SECRET ssl_key_csr,
      ssl_certificate_pem = '${arg.materialized-schema-registry-crt}',
      ssl_ca_pem = '${arg.ca-crt}',
      username = "materialize",
      password = SECRET password_csr
  )
  ENVELOPE DEBEZIUM


> SELECT * FROM connector_source
a
---
1
2
