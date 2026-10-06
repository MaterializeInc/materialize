# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# An upstream transaction that has written a row is open while the source is created, so the
# replication slot creation blocks on it until the configured timeout.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET pg_source_replication_slot_creation_timeout = '2s'

> CREATE SECRET pgpass AS 'postgres'

> CREATE CONNECTION pg_conn TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> CREATE CLUSTER slot_timeout_cluster SIZE '${arg.default-replica-size}'

> BEGIN
> CREATE SOURCE mz_source
  IN CLUSTER slot_timeout_cluster
  FROM POSTGRES CONNECTION pg_conn (PUBLICATION 'mz_source');

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);
> COMMIT

# The source alternates between `stalled` and `starting` while the transaction is open, so check
# the recorded stalls instead of the current status.
> SELECT DISTINCT error ~~ 'postgres: creating replication slot % timed out after 2s. Slot creation waits for in-progress upstream transactions to end'
  FROM mz_internal.mz_source_status_history
  JOIN mz_sources ON id = source_id
  WHERE name = 'mz_source' AND status = 'stalled'
true
