# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Runs after the setup and add-tables phases of pg-snapshot-freshness. The held
# snapshot of `t_paused` keeps the slot's confirmed_flush_lsn behind the second
# row of `t_before`, which `t_before` has already committed. The schema
# validation interval is long, so the change below goes unnoticed until a
# restart replays that row with a Relation message for `t_before`.

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE t_before ALTER COLUMN f1 TYPE bigint;
