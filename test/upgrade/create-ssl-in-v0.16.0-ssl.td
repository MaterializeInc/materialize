# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# this can't be run if the version is > 19, as v20 is when the behavior changed, requiring the migration
# this test is checking
$ skip-if
select split_part(ltrim('${arg.upgrade-from-version}', 'v'), '.', 1)::int = 0 and split_part(ltrim('${arg.upgrade-from-version}', 'v'), '.', 2)::int > 19;

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
  ENVELOPE DEBEZIUM
