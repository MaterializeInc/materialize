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

> CREATE SECRET sasl_password AS 'sekurity'

> CREATE SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      security_protocol = 'SASL_SSL',
      sasl_mechanisms = 'PLAIN',
      sasl_username = 'materialize',
      sasl_password = SECRET sasl_password,
      ssl_ca_pem = '${arg.ca}'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_ca_pem = '${arg.ca}'
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

> CREATE SINK data_snk
  FROM data
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      security_protocol = 'SASL_SSL',
      sasl_mechanisms = 'PLAIN',
      sasl_username = 'materialize',
      sasl_password = SECRET sasl_password,
      ssl_ca_pem = '${arg.ca}'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_ca_pem = '${arg.ca}'
  )

$ kafka-verify format=avro sink=materialize.public.data_snk sort-messages=true
{"before": null, "after": {"row": {"a": 1}}}
{"before": null, "after": {"row": {"a": 2}}}

# Check environment variables; defined in mzcompose.py

> CREATE SOURCE env_pw_data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      security_protocol = 'SASL_SSL',
      sasl_mechanisms = 'PLAIN',
      sasl_username = 'materialize',
      sasl_password = SECRET sasl_password,
      ssl_ca_pem = '${arg.ca}'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_ca_pem = '${arg.ca}'
  )

> SELECT * FROM env_pw_data
a
---
1
2


# Ensure that connectors work with SSL basic_auth
> CREATE CONNECTION kafka_sasl
  FOR KAFKA
    BROKER 'kafka:9092',
    SASL MECHANISMS = 'PLAIN',
    SASL USERNAME = 'materialize',
    SASL PASSWORD = SECRET sasl_password,
    SSL CERTIFICATE AUTHORITY = '${arg.ca}';

> CREATE SOURCE connector_source
  FROM KAFKA CONNECTION kafka_sasl
  TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_ca_pem = '${arg.ca}'
  )


> SELECT * FROM connector_source
a
---
1
2

! CREATE SOURCE connector_source
  FROM KAFKA CONNECTION kafka_sasl
  TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
    security_protocol = 'sasl_ssl'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_ca_pem = '${arg.ca}'
  )
contains:cannot set option security.protocol for SOURCE using CONNECTION materialize.public.kafka_sasl


# Ensure that connectors do not require the certificate authority
> CREATE CONNECTION kafka_sasl_no_ca
  FOR KAFKA
    BROKER 'kafka:9092',
    SASL MECHANISMS = 'PLAIN',
    SASL USERNAME = 'materialize',
    SASL PASSWORD = SECRET sasl_password;

# This ensures that the error is not that the CA was required, but simply that
# not providing it prohibits connecting.
! CREATE SOURCE connector_source
  FROM KAFKA CONNECTION kafka_sasl_no_ca
  TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_ca_pem = '${arg.ca}'
  )
contains: broker certificate could not be verified
