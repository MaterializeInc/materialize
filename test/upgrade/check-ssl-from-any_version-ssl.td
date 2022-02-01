# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT * FROM data
a
---
1

# ensure we actually have the new option
> SHOW CREATE SOURCE data
"materialize.public.data" "CREATE SOURCE \"materialize\".\"public\".\"data\" FROM KAFKA BROKER 'kafka:9092' TOPIC 'testdrive-data-${testdrive.seed}' WITH (\"security_protocol\" = 'SSL', \"ssl_key_location\" = '/share/secrets/materialized.key', \"ssl_certificate_location\" = '/share/secrets/materialized.crt', \"ssl_ca_location\" = '/share/secrets/ca.crt', \"ssl_key_password\" = 'mzmzmz') FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'https://schema-registry:8081/' SEED VALUE SCHEMA '{\"type\":\"record\",\"name\":\"envelope\",\"fields\":[{\"name\":\"before\",\"type\":[{\"type\":\"record\",\"name\":\"row\",\"fields\":[{\"name\":\"a\",\"type\":\"long\"}]},\"null\"]},{\"name\":\"after\",\"type\":[\"row\",\"null\"]}]}' WITH (\"ssl_follow_kafka\" = true) ENVELOPE DEBEZIUM"
