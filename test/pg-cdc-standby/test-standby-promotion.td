# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The standby was promoted to a primary via pg_promote() (see promote_standby()
# in mzcompose.py), as an operator would during a failover. Promotion flips
# pg_is_in_recovery() to false and switches the WAL timeline.
#
# Logical decoding cannot safely continue against a node that is no longer the
# standby the source was created against. The source detects this proactively
# while the replication stream is live: a periodic background check (the schema
# validator, see spawn_schema_validator() in replication.rs) re-reads
# pg_is_in_recovery(), notices the changed physical-replica status, and stalls
# the source with a definite, non-retryable error -- no restart required.
#
# The startup check in raw_stream() catches the same condition across a restart,
# and runs before the timeline-ID check on purpose so a promotion deterministically
# surfaces as this specific error rather than the more generic timeline mismatch.

! SELECT count(*) FROM t
contains: upstream physical replica status changed

# The stall is permanent; an operator's only recourse is to recreate the source
# against the promoted (now primary) node. Drop it here so we leave a clean slate
# -- otherwise the post-workflow sanity restart fails on the stalled source.
> DROP SOURCE mz_source CASCADE
