# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

> CREATE SOURCE counter
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR COUNTER;

> SELECT
    s.name,
    -- Counters
    u.messages_received > 0,
    u.bytes_received > 0,
    u.updates_staged > 0,
    u.updates_committed > 0,
    -- Resetting Gauges
    u.records_indexed = 0,
    u.bytes_indexed = 0,
    u.rehydration_latency IS NOT NULL,
    u.snapshot_records_known IS NULL,
    u.snapshot_records_staged IS NULL,
    u.snapshot_committed,
    -- Gauges
    u.offset_known > 0,
    u.offset_committed > 0
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('counter')
counter true true true true true true true true true true true true

> DROP SOURCE counter CASCADE
