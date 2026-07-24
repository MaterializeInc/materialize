# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# two_runtime_query_dataflow scenario: the acceptance test for the interactive
# slow path (N1-N3). Unlike two_runtime_index.spec, which peeks a maintenance
# index directly (the fast path), this creates a genuine INTERACTIVE QUERY
# DATAFLOW: a `count(*)` reduce that imports a maintenance index and is itself
# exported with `transient`, so the multiplexer routes its `CreateDataflow` to
# the interactive runtime (see `mz_compute_client::multiplex`).
#
# The maintenance index is created but deliberately left unscheduled before the
# query dataflow is submitted, so nothing has been published to the shared
# arrangement-sharing registry yet: the interactive runtime's `CreateDataflow`
# handler finds the import missing and defers the build (N3) instead of
# building against a nonexistent arrangement. The query's own `schedule` is
# then sent immediately, before the maintenance index is scheduled, mirroring
# what the real compute controller always does for a transient collection
# (`Instance::maybe_schedule_collection`: scheduled right away, without waiting
# for its inputs). Only afterward is the maintenance index scheduled, which
# renders and publishes it; that publication (never a bare poll) wakes the
# deferred dataflow, which builds and runs. The result peek then returns the
# correct reduced rows, proving the defer -> build -> resolve path completed off
# the maintenance worker.
create-instance
----
ok

update-configuration
----
ok

initialization-complete
----
ok

write-rows shard=r ts=0
  1 alpha
  2 beta
  3 gamma
----
wrote 3

# The maintenance index over shard `r`. Registering it lets `import index=2001`
# below reference it; its dataflow is intentionally left unscheduled, so no
# arrangement is published yet.
create-dataflow name=maint-index as-of=0
  import source=1000 shard=r upper=1
  build id=2000
    Project (#0, #1)
      Get u1000
  export kind=index index=2001 on=2000 key=[0]
----
ok

# A one-column schema for the count reduce's output (a single bigint).
define-schema name=count_out
  count bigint
----
ok

# The interactive query dataflow: `count(*)` over the (still unpublished)
# maintenance index, exported `transient` so it is routed to the interactive
# runtime instead of maintenance.
create-dataflow name=interactive-count as-of=0
  import index=2001
  build id=3000
    Reduce aggregates=[count(*)]
      Get u2000
  export index=4000 on=3000 key=[0] transient
----
ok

# Scheduled before the maintenance index: the build is still deferred at this
# point (its import is unpublished), so this `Schedule` races ahead of it,
# exactly as the real controller's immediate-schedule-of-transient behavior
# would.
schedule id=4000
----
ok

# Scheduling the maintenance index renders and publishes it, waking the deferred
# interactive query dataflow via the publication notification.
schedule id=2001
----
ok

# The interactive runtime built and ran the reduce once notified of the
# publication, off the maintenance worker; the result is the correct count.
peek id=4000 schema=count_out ts=0
----
3
