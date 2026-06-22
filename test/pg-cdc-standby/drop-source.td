# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Dropping the source should clean up the logical replication slot Materialize
# created on the standby. The slot drop happens in a best-effort background task
# (see drop_replication_slots_and_secrets in the coordinator), so it is *not*
# synchronous with this statement returning -- the mzcompose workflow polls the
# standby for the slot's removal afterwards.

> DROP SOURCE mz_source CASCADE
