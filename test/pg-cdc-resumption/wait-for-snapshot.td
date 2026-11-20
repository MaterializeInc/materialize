# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that the initial snapshot is complete and that the initial data has
# arrived at Materialize. This ensures that any further disruptions will happen
# during the replication and not during the initial snapshot
#

> SELECT COUNT(*) FROM t1;
1000000

# Tables commit snapshots independently, so wait for all of them, not just t1
> SELECT count(*) FROM mz_tables t
  WHERE t.source_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM mz_internal.mz_source_statistics s
    WHERE s.id = t.id AND s.snapshot_committed
  )
0
