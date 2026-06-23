# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# CLU-131 regression test: a read-only materialized-view sink must not retain a
# full-MV-size correction buffer.
#
# The scenario reproduces the 0dt configuration in which the bug appears: the output
# shard `mv` already holds the MV at spread timestamps (the old generation wrote it
# over time), and a read-only sink recomputes the same rows from a single-timestamp
# source. The recomputed `desired` collapses to t=0 while the `persist` read-back
# replays at the original spread timestamps, so the two halves are the same rows at
# different times and cancel only when a consolidation pass re-times them together.
#
# A read-only sink mints no batches, so before the fix its only consolidation was the
# one-shot `consolidate_at_since` = `consolidate_before(since + 1)`, which covers only
# the band at or below the `since` when it happened to fire. The forward region (the
# spread persist beyond t=0) survived uncancelled: the sink held ~one full copy of the
# MV for the whole read-only window. The exact residual was timing-dependent (it
# hinged on the `since` value at the instant the one-shot fired), so before the fix
# this assertion failed intermittently with a value in the hundreds of thousands.
#
# The fix re-arms the forced consolidation in read-only mode so it keeps sweeping
# `consolidate_before` forward as the frontiers advance, mirroring the read-write
# path. The buffer is therefore consolidated down to a few thousand records,
# deterministically. The defaults exercise the affected path
# (`enable_compute_sync_mv_sink` and `enable_compute_correction_v2` both on).
create-instance
----
ok

initialization-complete
----
ok

# The old generation's accumulated output: the MV at spread timestamps 0..9.
write-spread shard=mv count=100000 n-ts=10
----
wrote 100000

# The read-only sink's source: the same rows at a single timestamp, so the recomputed
# `desired` snapshot collapses to t=0 while `persist` (mv) replays at 0..9.
write-single-ts shard=src ts=0 count=100000
----
wrote 100000

# A read-only MV sink recomputing `desired` from `src` and reading `mv` back. It is
# never given allow-writes, so it stays read-only and mints no batches. `as_of=0`
# holds `since` low, the configuration that exposed the bug.
create-dataflow name=reader as-of=0
  import source=1100 shard=src upper=1
  build id=2100
    Get u1100
  export kind=materialized-view sink=2101 on=2100 shard=mv
----
ok

schedule id=2101
----
ok

# The read-only correction buffer stays small: the re-armed forced consolidation
# sweeps the spread persist away instead of leaving ~one full MV uncancelled. Live
# occupancy is the difference of the two correction counters. `metrics` polls until it
# settles. (Before the fix this returned a full-MV-size value, intermittently.)
metrics mz_persist_sink_correction_insertions_total minus=mz_persist_sink_correction_deletions_total max=15000 timeout-secs=120
----
value <= 15000
