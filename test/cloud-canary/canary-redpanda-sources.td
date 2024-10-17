# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=bytes replication-factor=3

$ kafka-ingest format=bytes key-terminator=: key-format=bytes topic=bytes repeat=100
abc:abc

> DROP SOURCE IF EXISTS redpanda_bytes CASCADE;
> DROP CONNECTION IF EXISTS redpanda_conn CASCADE;
> DROP SECRET IF EXISTS redpanda_password CASCADE;

> CREATE SECRET redpanda_password AS '${arg.redpanda-password}';

> CREATE CONNECTION redpanda_conn TO KAFKA (
  BROKER '${testdrive.kafka-addr}',
  SASL MECHANISMS = 'SCRAM-SHA-512',
  SASL USERNAME = '${arg.redpanda-username}',
  SASL PASSWORD = SECRET redpanda_password
  );

> CREATE SOURCE redpanda_bytes
  IN CLUSTER canary_sources
  FROM KAFKA CONNECTION redpanda_conn (TOPIC 'testdrive-bytes-${testdrive.seed}');

> CREATE TABLE redpanda_bytes_tbl FROM SOURCE redpanda_bytes (REFERENCE "testdrive-bytes-${testdrive.seed}")
  FORMAT BYTES
  ENVELOPE NONE;

> CREATE MATERIALIZED VIEW redpanda_bytes_view AS SELECT COUNT(*) AS cnt FROM redpanda_bytes_tbl;

> CREATE DEFAULT INDEX ON redpanda_bytes_view;

> SELECT cnt from redpanda_bytes_view
100
