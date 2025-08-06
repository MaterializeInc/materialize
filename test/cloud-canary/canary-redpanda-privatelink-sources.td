# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# TODO(def-) Reenable when Redpanda Cloud is working with Testdrive again
$ skip-if
SELECT true

$ kafka-create-topic topic=bytes-privatelink replication-factor=3

$ kafka-ingest format=bytes key-terminator=: key-format=bytes topic=bytes-privatelink repeat=100
abc:abc

> DROP SOURCE IF EXISTS redpanda_privatelink_bytes CASCADE;
> DROP CONNECTION IF EXISTS redpanda_privatelink_conn CASCADE;
> DROP SECRET IF EXISTS redpanda_privatelink_password CASCADE;

> CREATE SECRET redpanda_privatelink_password AS '${arg.redpanda-password}';

> VALIDATE CONNECTION privatelink_conn;

# When run immediately: ERROR: Meta data fetch error: BrokerTransportFailure
$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration=120s

> CREATE CONNECTION redpanda_privatelink_conn TO KAFKA (
  AWS PRIVATELINK privatelink_conn (PORT 30292),
  SASL MECHANISMS = 'SCRAM-SHA-512',
  SASL USERNAME = '${arg.redpanda-username}',
  SASL PASSWORD = SECRET redpanda_privatelink_password
  );

> CREATE SOURCE redpanda_privatelink_bytes
  IN CLUSTER canary_sources
  FROM KAFKA CONNECTION redpanda_privatelink_conn (TOPIC 'testdrive-bytes-privatelink-${testdrive.seed}');

> CREATE TABLE redpanda_privatelink_bytes_tbl FROM SOURCE redpanda_privatelink_bytes (REFERENCE "testdrive-bytes-privatelink-${testdrive.seed}")
  FORMAT BYTES
  ENVELOPE NONE;

> CREATE MATERIALIZED VIEW redpanda_privatelink_bytes_view AS SELECT COUNT(*) AS cnt FROM redpanda_privatelink_bytes_tbl;

> CREATE DEFAULT INDEX ON redpanda_privatelink_bytes_view;

> SELECT cnt from redpanda_privatelink_bytes_view
100
