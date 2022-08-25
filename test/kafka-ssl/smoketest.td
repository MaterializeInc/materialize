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

# Ensure connections track their dependencies
> CREATE CONNECTION kafka_ssl
  FOR KAFKA
    BROKER 'kafka:9092',
    SSL KEY = SECRET ssl_key_kafka,
    SSL CERTIFICATE = '${arg.materialized-kafka-crt}',
    SSL CERTIFICATE AUTHORITY = '${arg.ca-crt}';

> CREATE CONNECTION csr_ssl
  FOR CONFLUENT SCHEMA REGISTRY
    URL '${testdrive.schema-registry-url}',
    SSL KEY = SECRET ssl_key_csr,
    SSL CERTIFICATE = '${arg.materialized-schema-registry-crt}',
    SSL CERTIFICATE AUTHORITY = '${arg.ca-crt}',
    USERNAME = 'materialize',
    PASSWORD = SECRET password_csr;

! DROP SECRET ssl_key_kafka;
contains: cannot drop

# Ensure non-secrets cannot be used as secrets
> CREATE TABLE not_a_secret (a int);

! CREATE CONNECTION conn
  FOR KAFKA
    SSL KEY = SECRET not_a_secret;
contains: materialize.public.not_a_secret is not a secret

> CREATE SOURCE data
  FROM KAFKA CONNECTION kafka_ssl TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_ssl;

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

> CREATE SINK snk FROM data
  INTO KAFKA CONNECTION kafka_ssl TOPIC 'snk'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_ssl

$ kafka-verify format=avro sink=materialize.public.snk sort-messages=true
{"before": null, "after": {"row":{"a": 1}}}
{"before": null, "after": {"row":{"a": 2}}}

> CREATE CONNECTION no_basic_auth_conn
  FOR CONFLUENT SCHEMA REGISTRY
    URL '${testdrive.schema-registry-url}',
    SSL KEY = SECRET ssl_key_csr,
    SSL CERTIFICATE = '${arg.materialized-schema-registry-crt}',
    SSL CERTIFICATE AUTHORITY = '${arg.ca-crt}';

# not basic_auth
! CREATE SINK no_basic_auth FROM data
  INTO KAFKA CONNECTION kafka_ssl TOPIC 'snk'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION no_basic_auth_conn
contains:error publishing kafka schemas for sink: unable to publish value schema to registry in kafka sink: server error 401: Unauthorized

> CREATE CONNECTION csr_without_ssl
  FOR CONFLUENT SCHEMA REGISTRY
    URL '${testdrive.schema-registry-url}',
    USERNAME = 'materialize',
    PASSWORD = SECRET password_csr;

# Ensure that we get an ssl error if we forget to set certs
! CREATE SOURCE data
  FROM KAFKA CONNECTION kafka_ssl TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_without_ssl
contains:self signed certificate in certificate chain

# missing config
! CREATE CONNECTION m
  FOR KAFKA BROKER 'kafka:9092',
  SSL CERTIFICATE '${arg.materialized-kafka-crt}',
  SSL CERTIFICATE AUTHORITY = '${arg.ca-crt}';
contains:invalid CONNECTION: under-specified security configuration

> CREATE SOURCE kafka_csr_connector_source
  FROM KAFKA CONNECTION kafka_ssl
    TOPIC 'testdrive-data-${testdrive.seed}'
    FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_ssl


> SELECT * FROM kafka_csr_connector_source
a
---
1
2

# Old with_options do not work
! CREATE SOURCE IF NOT EXISTS duplicate_option_specified
  FROM KAFKA CONNECTION kafka_ssl
  TOPIC 'testdrive-data-${testdrive.seed}'
  LEGACYWITH (
    security_protocol = 'ssl'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_ssl
contains:unexpected parameters for CREATE SOURCE: security_protocol

# Ensure that connectors do not require the certificate authority

> CREATE CONNECTION kafka_sasl_no_ca
  FOR KAFKA
    BROKER 'kafka:9092',
    SSL KEY = SECRET ssl_key_kafka,
    SSL CERTIFICATE = '${arg.materialized-kafka-crt}';

> CREATE CONNECTION csr_ssl_no_ca
  FOR CONFLUENT SCHEMA REGISTRY
    URL '${testdrive.schema-registry-url}',
    SSL KEY = SECRET ssl_key_csr,
    SSL CERTIFICATE = '${arg.materialized-schema-registry-crt}',
    USERNAME = 'materialize',
    PASSWORD = SECRET password_csr;

# This ensures that the error is not that the CA was required, but simply that
# not providing it prohibits connecting.
! CREATE SOURCE kafka_ssl_no_ca
  FROM KAFKA CONNECTION kafka_sasl_no_ca
    TOPIC 'testdrive-data-${testdrive.seed}'
    FORMAT AVRO
  USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_ssl_no_ca
contains: broker certificate could not be verified
