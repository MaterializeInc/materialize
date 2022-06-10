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
      ssl_key_location = '/share/secrets/materialized-kafka.key',
      ssl_certificate_location = '/share/secrets/materialized-kafka.crt',
      ssl_ca_location = '/share/secrets/ca.crt',
      ssl_key_password = 'mzmzmz'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_location = '/share/secrets/materialized-schema-registry.key',
      ssl_certificate_location = '/share/secrets/materialized-schema-registry.crt',
      ssl_ca_location = '/share/secrets/ca.crt',
      username = "materialize",
      password = "sekurity"
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

> CREATE SECRET ssl_key_location_kafka AS '/share/secrets/materialized-kafka.key'

> CREATE SECRET ssl_certificate_location_kafka AS '/share/secrets/materialized-kafka.crt'

> CREATE SECRET ssl_key_password_kafka AS 'mzmzmz'

> CREATE SECRET ssl_key_location_csr AS '/share/secrets/materialized-schema-registry.key'

> CREATE SECRET ssl_certificate_location_csr  AS '/share/secrets/materialized-schema-registry.crt'

> CREATE SECRET username_csr AS 'materialize'

> CREATE SECRET password_csr AS 'sekurity'

> CREATE SECRET ssl_ca_location AS '/share/secrets/ca.crt'

> CREATE MATERIALIZED SOURCE data_secret
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      security_protocol = 'SSL',
      ssl_key_location = SECRET ssl_key_location_kafka,
      ssl_certificate_location = SECRET ssl_certificate_location_kafka,
      ssl_ca_location = SECRET ssl_ca_location,
      ssl_key_password = SECRET ssl_key_password_kafka
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_location = SECRET ssl_key_location_csr,
      ssl_certificate_location = SECRET ssl_certificate_location_csr,
      ssl_ca_location = SECRET ssl_ca_location,
      username = SECRET username_csr,
      password = SECRET password_csr
  )
  ENVELOPE DEBEZIUM

> SELECT * FROM data_secret
a
---
1
2

# Ensure that our test infra correctly sets up certs by failing when CSR is not
# specifically configured
! CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      security_protocol = 'SSL',
      ssl_key_location = '/share/secrets/materialized-kafka.key',
      ssl_certificate_location = '/share/secrets/materialized-kafka.crt',
      ssl_ca_location = '/share/secrets/ca.crt',
      ssl_key_password = 'mzmzmz',
      username = "materialize",
      password = "sekurity"
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_location = '/share/secrets/materialized-kafka.key',
      ssl_certificate_location = '/share/secrets/materialized-kafka.crt',
      ssl_ca_location = '/share/secrets/ca.crt'
  )
  ENVELOPE DEBEZIUM
contains:sslv3 alert certificate unknown

# Ensure that our test infra correctly sets up certs by failing when CSR is not
# specifically configured, AND we are specifically setting certs
! CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      security_protocol = 'SSL',
      ssl_key_location = '/share/secrets/materialized-kafka.key',
      ssl_certificate_location = '/share/secrets/materialized-kafka.crt',
      ssl_ca_location = '/share/secrets/ca.crt',
      ssl_key_password = 'mzmzmz',
      username = "materialize",
      password = "sekurity"
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_location = '/share/secrets/materialized-kafka.key',
      ssl_certificate_location = '/share/secrets/materialized-kafka.crt',
      ssl_ca_location = '/share/secrets/ca.crt'
  )
  ENVELOPE DEBEZIUM
contains:sslv3 alert certificate unknown


# test sinks with multiple things to
> CREATE SINK snk FROM data
  INTO KAFKA BROKER 'kafka' TOPIC 'snk'
  WITH (
      security_protocol = 'SSL',
      ssl_key_location = '/share/secrets/materialized-kafka.key',
      ssl_certificate_location = '/share/secrets/materialized-kafka.crt',
      ssl_ca_location = '/share/secrets/ca.crt',
      ssl_key_password = 'mzmzmz'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_location = '/share/secrets/materialized-schema-registry.key',
      ssl_certificate_location = '/share/secrets/materialized-schema-registry.crt',
      ssl_ca_location = '/share/secrets/ca.crt',
      username = "materialize",
      password = "sekurity"
  )

$ kafka-verify format=avro sink=materialize.public.snk sort-messages=true
{"before": null, "after": {"row":{"a": 1}}}
{"before": null, "after": {"row":{"a": 2}}}

# and wrong cert for sink
! CREATE SINK snk_bad FROM data
  INTO KAFKA BROKER 'kafka' TOPIC 'snk'
  WITH (
      security_protocol = 'SSL',
      ssl_key_location = '/share/secrets/materialized-kafka.key',
      ssl_certificate_location = '/share/secrets/materialized-kafka.crt',
      ssl_ca_location = '/share/secrets/ca.crt',
      ssl_key_password = 'mzmzmz'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_location = '/share/secrets/materialized-kafka.key',
      ssl_certificate_location = '/share/secrets/materialized-kafka.crt',
      ssl_ca_location = '/share/secrets/ca.crt'
  )
contains:sslv3 alert certificate unknown
