# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "a", "type": "long"}
    ]
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} publish=true timestamp=1
{"a": 1}

> CREATE SECRET ssl_key_kafka AS '${arg.materialized-kafka-key}'
> CREATE SECRET ssl_key_csr AS '${arg.materialized-schema-registry-key}'
> CREATE SECRET password_csr AS 'sekurity'

> CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      security_protocol = 'SSL',
      ssl_key_pem = SECRET ssl_key_kafka,
      ssl_certificate_pem = '${arg.materialized-kafka-crt}',
      ssl_ca_pem = '${arg.ca-crt}'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_pem = SECRET ssl_key_csr,
      ssl_certificate_pem = '${arg.materialized-schema-registry-crt}',
      ssl_ca_pem = '${arg.ca-crt}',
      username = "materialize",
      password = SECRET password_csr
  )

> SELECT * FROM data
a
---
1

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=2
{"a": 2}

> SELECT * FROM data
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
      ssl_key_pem = SECRET ssl_key_kafka,
      ssl_certificate_pem = '${arg.materialized-kafka-crt}',
      ssl_ca_pem = '${arg.ca-crt}',
      username = "materialize",
      password = SECRET password_csr
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_pem = SECRET ssl_key_kafka,
      ssl_certificate_pem = '${arg.materialized-kafka-crt}',
      ssl_ca_pem = '${arg.ca-crt}'
  )
contains:sslv3 alert certificate unknown

# Ensure that our test infra correctly sets up certs by failing when CSR is not
# specifically configured, AND we are specifically setting certs
! CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER 'kafka:9092' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      security_protocol = 'SSL',
      ssl_key_pem = SECRET ssl_key_kafka,
      ssl_certificate_pem = '${arg.materialized-kafka-crt}',
      ssl_ca_pem = '${arg.ca-crt}',
      username = "materialize",
      password = SECRET password_csr
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_pem = SECRET ssl_key_kafka,
      ssl_certificate_pem = '${arg.materialized-kafka-crt}',
      ssl_ca_pem = '${arg.ca-crt}'
  )
contains:sslv3 alert certificate unknown


# test sinks with multiple things to
> CREATE SINK snk FROM data
  INTO KAFKA BROKER 'kafka' TOPIC 'snk'
  WITH (
      security_protocol = 'SSL',
      ssl_key_pem = SECRET ssl_key_kafka,
      ssl_certificate_pem = '${arg.materialized-kafka-crt}',
      ssl_ca_pem = '${arg.ca-crt}'
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

# and wrong cert for sink
! CREATE SINK snk_bad FROM data
  INTO KAFKA BROKER 'kafka' TOPIC 'snk'
  WITH (
      security_protocol = 'SSL',
      ssl_key_pem = SECRET ssl_key_kafka,
      ssl_certificate_pem = '${arg.materialized-kafka-crt}',
      ssl_ca_pem = '${arg.ca-crt}'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_key_pem = SECRET ssl_key_kafka,
      ssl_certificate_pem = '${arg.materialized-kafka-crt}',
      ssl_ca_pem = '${arg.ca-crt}'
  )
contains:sslv3 alert certificate unknown
