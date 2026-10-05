#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Creates an empty table with lineitem's schema. A union with it keeps the
# input frontier at the table's (advancing) frontier, since the static TPC-H
# source has an empty frontier and would ship future updates immediately.
set -uo pipefail
USR="psql -h localhost -p 6875 -U materialize materialize -qAtX"
COLS=$($USR -c "SELECT string_agg(c.name || ' ' || c.type, ', ' ORDER BY c.position) FROM mz_columns c JOIN mz_tables t ON t.id = c.id WHERE t.name = 'lineitem'" 2>/dev/null)
echo "columns: $COLS"
$USR -c "DROP TABLE IF EXISTS li_empty CASCADE"
$USR -c "CREATE TABLE li_empty ($COLS)"
$USR -c "SELECT o.name, f.write_frontier FROM mz_internal.mz_frontiers f JOIN mz_objects o ON o.id = f.object_id WHERE o.name = 'li_empty'" 2>/dev/null
