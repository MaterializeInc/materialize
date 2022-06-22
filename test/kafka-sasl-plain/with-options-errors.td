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

! CREATE MATERIALIZED SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
      security_protocol = 'SASL_SSL',
      sasl_mechanisms = 'PLAIN',
      sasl_username = 'materialize',
      sasl_password_env = 'SASL_PASSWORD',
      ssl_ca_location = '/share/secrets/ca.crt'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH (
      ssl_ca_location = '/share/secrets/ca.crt'
  )
  ENVELOPE DEBEZIUM
contains:sasl.password must be set
