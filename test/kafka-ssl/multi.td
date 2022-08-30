# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-regex match=\d{13} replacement=<TIMESTAMP>

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

> CREATE CONNECTION kafka_conn
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

> CREATE SOURCE data
  FROM KAFKA CONNECTION kafka_conn TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_ssl

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

# test sinks with multiple things to
> CREATE SINK snk FROM data
  INTO KAFKA CONNECTION kafka_conn TOPIC 'snk'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_ssl

$ kafka-verify format=avro sink=materialize.public.snk sort-messages=true
{"before": null, "after": {"row":{"a": 1}}, "transaction": {"id": "<TIMESTAMP>"}}
{"before": null, "after": {"row":{"a": 2}}, "transaction": {"id": "<TIMESTAMP>"}}

# todo: re-enable this as part of #13453
# Ensure that our test infra correctly sets up certs by failing when CSR is not
# specifically configured, AND we are specifically setting certs
# > CREATE CONNECTION unknown_cert
#   FOR CONFLUENT SCHEMA REGISTRY
#     URL '${testdrive.schema-registry-url}',
#     SSL KEY = SECRET ssl_key_kafka,
#     SSL CERTIFICATE = '${arg.materialized-schema-registry-crt}',
#     SSL CERTIFICATE AUTHORITY = '${arg.ca-crt}',
#     USERNAME = 'materialize',
#     PASSWORD = SECRET password_csr;
