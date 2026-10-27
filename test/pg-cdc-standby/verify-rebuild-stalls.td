# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The standby was rebuilt from a fresh base backup of a *later* version of the
# primary (see rebuild_standby() in mzcompose.py). A base backup never copies
# replication slots, so the logical slot Materialize created on the standby is
# gone. On resume the source recreates the slot at the rebuilt standby's current
# LSN, which is now far ahead of the LSN Materialize has durably committed. The
# slot can no longer serve the data Materialize still needs, so the source
# stalls and never recovers.
#
# This is the same failure surfaced by test/pg-cdc/dropped-slot-errors.td, but
# here the LSN gap is large and deterministic rather than marginal.

> SELECT status, error ~~ 'postgres: slot overcompacted. Requested LSN % but only LSNs % are available%'
  FROM mz_internal.mz_source_statuses
  WHERE name = 't'
stalled true

# The rows inserted on the primary just before the rebuild never reach
# Materialize -- the source is stuck at the pre-rebuild contents.
#
# Read under SERIALIZABLE: the default STRICT SERIALIZABLE isolation anchors the
# read timestamp to the global timeline oracle (real time), then waits for the
# source's write frontier to catch up -- which never happens once it stalls, so
# the query would hang until timeout. SERIALIZABLE reads the freshest timestamp
# the stalled source can actually serve, surfacing the frozen contents instead.
> SET TRANSACTION_ISOLATION = 'SERIALIZABLE'

> SELECT count(*) FROM t
1250
