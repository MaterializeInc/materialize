# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test AWS PRIVATELINKS (plural) with Confluent Cloud PrivateLink.
# Verifies pattern-based AZ routing for dynamic broker discovery.
#
# The underlying AWS PRIVATELINK connection (confluent_privatelink_conn) is
# created by ConfluentPrivateLink in mzcompose.py before this file runs.

$ kafka-create-topic topic=bytes-privatelinks replication-factor=3

$ kafka-ingest format=bytes key-terminator=: key-format=bytes topic=bytes-privatelinks repeat=100
abc:abc

> DROP SOURCE IF EXISTS confluent_privatelinks_bytes CASCADE;
> DROP CONNECTION IF EXISTS confluent_privatelinks_conn CASCADE;
> DROP SECRET IF EXISTS confluent_privatelinks_password CASCADE;

> CREATE SECRET confluent_privatelinks_password AS '${arg.confluent-pl-api-secret}';

# AWS PRIVATELINKS (plural) with pattern-based AZ routing.
# The exact-match pattern provides the bootstrap broker address.
# Confluent brokers advertise AZ-specific addresses (e.g. *.use1-az1.glb.confluent.cloud)
# that the wildcard patterns match to route through the correct AZ endpoint.
> CREATE CONNECTION confluent_privatelinks_conn TO KAFKA (
  AWS PRIVATELINKS (
      '${arg.confluent-pl-bootstrap}' TO confluent_privatelink_conn,
      '*.use1-az1.*' TO confluent_privatelink_conn (AVAILABILITY ZONE = 'use1-az1'),
      '*.use1-az4.*' TO confluent_privatelink_conn (AVAILABILITY ZONE = 'use1-az4'),
      '*.use1-az6.*' TO confluent_privatelink_conn (AVAILABILITY ZONE = 'use1-az6')
  ),
  SECURITY PROTOCOL = 'SASL_SSL',
  SASL MECHANISMS = 'PLAIN',
  SASL USERNAME = '${arg.confluent-pl-api-key}',
  SASL PASSWORD = SECRET confluent_privatelinks_password
  );

> BEGIN
> CREATE SOURCE confluent_privatelinks_bytes
  IN CLUSTER canary_sources
  FROM KAFKA CONNECTION confluent_privatelinks_conn (TOPIC 'testdrive-bytes-privatelinks-${testdrive.seed}');

> CREATE TABLE confluent_privatelinks_bytes_tbl FROM SOURCE confluent_privatelinks_bytes (REFERENCE "testdrive-bytes-privatelinks-${testdrive.seed}")
  FORMAT BYTES
  ENVELOPE NONE;
> COMMIT

> CREATE MATERIALIZED VIEW confluent_privatelinks_bytes_view AS SELECT COUNT(*) AS cnt FROM confluent_privatelinks_bytes_tbl;

> CREATE DEFAULT INDEX ON confluent_privatelinks_bytes_view;

> SELECT cnt from confluent_privatelinks_bytes_view
100
