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
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      security_protocol = 'SASL_SSL',
      sasl_mechanisms = 'PLAIN',
      sasl_username = 'materialize',
      sasl_password = 'sekurity',
      ssl_ca_location = '/share/secrets/ca.crt'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_ca_location = '/share/secrets/ca.crt'
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
      sasl_password = 'sekurity',
      ssl_ca_location = '/share/secrets/ca.crt'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_ca_location = '/share/secrets/ca.crt'
  )

$ kafka-verify format=avro sink=materialize.public.data_snk sort-messages=true
{"before": null, "after": {"row": {"a": 1}}}
{"before": null, "after": {"row": {"a": 2}}}

# Check environment variables; defined in mzcompose.yml

> CREATE MATERIALIZED SOURCE env_pw_data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      security_protocol = 'SASL_SSL',
      sasl_mechanisms = 'PLAIN',
      sasl_username = 'materialize',
      sasl_password = 'sekurity',
      ssl_ca_location = '/share/secrets/ca.crt'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_ca_location = '/share/secrets/ca.crt'
  )
  ENVELOPE DEBEZIUM

> SELECT * FROM env_pw_data
a
---
1
2

#TODO(nharring-adjacent): Uncomment once this test runs with experimental enabled
## Ensure connectors work with SASL_PLAIN auth
#> CREATE CONNECTOR sasl_connector
#  FOR KAFKA BROKER '${testdrive.kafka-addr}'
#  WITH (
#      security_protocol = 'SASL_SSL',
#      sasl_mechanisms = 'PLAIN',
#      sasl_username = 'materialize',
#      sasl_password = 'sekurity',
#      ssl_ca_location = '/share/secrets/ca.crt'
#  )
#
#> CREATE MATERIALIZED SOURCE connector_data
#  FROM KAFKA CONNECTOR sasl_connector
#  TOPIC 'testdrive-data-${testdrive.seed}'
#  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
#  WITH (
#      ssl_ca_location = '/share/secrets/ca.crt'
#  )
#  ENVELOPE DEBEZIUM
#
#> SELECT * FROM connector_data
#a
#---
#1
#2
#
#> CREATE MATERIALIZED SOURCE connector_env_pw_data
#  FROM KAFKA CONNECTOR sasl_env_pw_connector
#  TOPIC 'testdrive-data-${testdrive.seed}'
#  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
#  WITH (
#      ssl_ca_location = '/share/secrets/ca.crt'
#  )
#  ENVELOPE DEBEZIUM
#
#> SELECT * from connector_env_pw_data
#a
#---
#1
#2
