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

> CREATE SECRET sasl_password AS 'sekurity'

> CREATE MATERIALIZED SOURCE data
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

> CREATE MATERIALIZED SOURCE env_pw_data
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
  ENVELOPE DEBEZIUM

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

> CREATE MATERIALIZED SOURCE connector_source
  FROM KAFKA CONNECTION kafka_sasl
  TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_ca_pem = '${arg.ca}'
  )
  ENVELOPE DEBEZIUM


> SELECT * FROM connector_source
a
---
1
2

! CREATE MATERIALIZED SOURCE connector_source
  FROM KAFKA CONNECTION kafka_sasl
  TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
    security_protocol = 'sasl_ssl'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_ca_pem = '${arg.ca}'
  )
  ENVELOPE DEBEZIUM
contains:cannot set option security.protocol for SOURCE using CONNECTION materialize.public.kafka_sasl
