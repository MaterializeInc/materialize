#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euo pipefail

# Basic health check — verifies materialized is responding to SQL.
# This is a minimal placeholder; the antithesis-workload skill will add
# real test commands with property assertions.

PGHOST="${PGHOST:-materialized}"
PGPORT="${PGPORT:-6875}"
PGUSER="${PGUSER:-materialize}"

result=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -tAc "SELECT 1" 2>/dev/null)
if [ "$result" = "1" ]; then
    echo "Health check passed"
    exit 0
else
    echo "Health check failed: $result"
    exit 1
fi
