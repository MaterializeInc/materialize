# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Ensure that the source becomes healthy again once the SSH tunnel is restarted.
# We specifically make sure that new data written to the Kafka topic is visible
# in the source, as that is the true measure of health, vs what is reported in
# the mz_source_statuses relation.

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 VALUES (3);

> SELECT f1 FROM t1 ORDER BY f1 ASC;
1
1
2
3


> SELECT name, type, error
  FROM mz_internal.mz_source_statuses
  WHERE
      name
      IN (
      SELECT name FROM mz_tables WHERE source_id IS NOT NULL
      );
t1 table <null>
