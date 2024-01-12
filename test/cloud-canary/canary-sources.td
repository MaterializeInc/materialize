# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The free tier of Confluent Cloud does not alow for programatic
# topic creation
# $ kafka-create-topic topic=bytes

$ kafka-ingest format=bytes key-terminator=: key-format=bytes topic=bytes repeat=100
abc:abc

> DROP SOURCE IF EXISTS bytes CASCADE;
> DROP CONNECTION IF EXISTS kafka_conn;
> DROP SECRET IF EXISTS confluent_username
> DROP SECRET IF EXISTS confluent_password

> CREATE SECRET confluent_username AS '${arg.confluent-api-key}';

> CREATE SECRET confluent_password AS '${arg.confluent-api-secret}';

> CREATE CONNECTION kafka_conn TO KAFKA (
  BROKER '${testdrive.kafka-addr}',
  SASL MECHANISMS = 'PLAIN',
  SASL USERNAME = SECRET confluent_username,
  SASL PASSWORD = SECRET confluent_password
  );

> CREATE SOURCE bytes
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-bytes-${testdrive.seed}')
  FORMAT BYTES
  ENVELOPE NONE
  WITH (SIZE '3xsmall');

> CREATE MATERIALIZED VIEW bytes_view AS SELECT COUNT(*) AS cnt FROM bytes;

> CREATE DEFAULT INDEX ON bytes_view;

> SELECT cnt > 0 from bytes_view
true
