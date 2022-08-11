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

COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING=true cockroach start-single-node --insecure --background

cockroach sql --insecure -e "CREATE SCHEMA IF NOT EXISTS consensus"
cockroach sql --insecure -e "CREATE SCHEMA IF NOT EXISTS storage"
cockroach sql --insecure -e "CREATE SCHEMA IF NOT EXISTS adapter"

exec environmentd \
    --sql-listen-addr=0.0.0.0:6875 \
    --http-listen-addr=0.0.0.0:6876 \
    --internal-sql-listen-addr=0.0.0.0:6877 \
    --internal-http-listen-addr=0.0.0.0:6878 \
    "--persist-consensus-url=postgresql://root@$(hostname):26257?options=--search_path=consensus" \
    "--storage-stash-url=postgresql://root@$(hostname):26257?options=--search_path=storage" \
    "--adapter-stash-url=postgresql://root@$(hostname):26257?options=--search_path=adapter" \
    "$@"
