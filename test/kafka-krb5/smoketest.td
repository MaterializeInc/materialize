# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This peforms a smoke test ensuring that our Kerberos auth implementation
# continues to work.

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
            "fields": [{"name": "a", "type": "int"}]
          },
          "null"
        ]
      },
      { "name": "after", "type": ["row", "null"] }
    ]
  }

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1
{"before": null, "after": {"a": 102}}
{"before": null, "after": {"a": 201}}

> CREATE SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  WITH (
    security_protocol = 'sasl_plaintext',
    sasl_kerberos_keytab = '/share/secrets/materialized.key',
    sasl_kerberos_service_name = 'kafka',
    sasl_kerberos_principal = 'materialized@CI.MATERIALIZE.IO'
  )
  FORMAT AVRO USING SCHEMA '${schema}' ENVELOPE DEBEZIUM

> CREATE MATERIALIZED VIEW data_view as SELECT * from data

> SELECT * FROM data_view
102
201
