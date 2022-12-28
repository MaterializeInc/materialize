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

COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING=true cockroach start-single-node \
    --insecure \
    --background

cockroach sql --insecure -e "CREATE SCHEMA IF NOT EXISTS consensus"
cockroach sql --insecure -e "CREATE SCHEMA IF NOT EXISTS storage"
cockroach sql --insecure -e "CREATE SCHEMA IF NOT EXISTS adapter"

if [[ ! -f /mzdata/environment-id ]]; then
  echo "docker-container-$(cat /proc/sys/kernel/random/uuid)-0" > /mzdata/environment-id
fi

# We pass default arguments as environment variables, and only if those
# environment variables do not already exist, to allow users to override these
# arguments when running the container via either environment variables or
# command-line arguments.
export MZ_ENVIRONMENT_ID=${MZ_ENVIRONMENT_ID:-$(</mzdata/environment-id)}
export MZ_SQL_LISTEN_ADDR=${MZ_SQL_LISTEN_ADDR:-0.0.0.0:6875}
export MZ_HTTP_LISTEN_ADDR=${MZ_HTTP_LISTEN_ADDR:-0.0.0.0:6876}
export MZ_INTERNAL_SQL_LISTEN_ADDR=${MZ_INTERNAL_SQL_LISTEN_ADDR:-0.0.0.0:6877}
export MZ_INTERNAL_HTTP_LISTEN_ADDR=${MZ_INTERNAL_HTTP_LISTEN_ADDR:-0.0.0.0:6878}
export MZ_PERSIST_CONSENSUS_URL=${MZ_PERSIST_CONSENSUS_URL:-postgresql://root@$(hostname):26257?options=--search_path=consensus}
export MZ_PERSIST_BLOB_URL=${MZ_PERSIST_BLOB_URL:-file:///mzdata/persist/blob}
export MZ_STORAGE_STASH_URL=${MZ_STORAGE_STASH_URL:-postgresql://root@$(hostname):26257?options=--search_path=storage}
export MZ_ADAPTER_STASH_URL=${MZ_ADAPTER_STASH_URL:-postgresql://root@$(hostname):26257?options=--search_path=adapter}
export MZ_ORCHESTRATOR=${MZ_ORCHESTRATOR:-process}
export MZ_ORCHESTRATOR_PROCESS_SECRETS_DIRECTORY=${MZ_ORCHESTRATOR_PROCESS_SECRETS_DIRECTORY:-/mzdata/secrets}

exec environmentd "$@"
