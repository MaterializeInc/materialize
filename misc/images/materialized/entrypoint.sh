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

pg_ctlcluster 14 materialize start

psql -Atc "CREATE SCHEMA IF NOT EXISTS consensus"
psql -Atc "CREATE SCHEMA IF NOT EXISTS storage"
psql -Atc "CREATE SCHEMA IF NOT EXISTS adapter"

exec environmentd \
    --sql-listen-addr=0.0.0.0:6875 \
    --http-listen-addr=0.0.0.0:6876 \
    --internal-sql-listen-addr=0.0.0.0:6877 \
    --internal-http-listen-addr=0.0.0.0:6878 \
    "--persist-consensus-url=postgresql://materialize@$(hostname):5432?options=--search_path=consensus" \
    "--storage-stash-url=postgresql://materialize@$(hostname):5432?options=--search_path=storage" \
    "--adapter-stash-url=postgresql://materialize@$(hostname):5432?options=--search_path=adapter" \
    "$@"
