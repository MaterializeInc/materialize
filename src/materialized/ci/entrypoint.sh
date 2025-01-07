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

cat <<EOF >/dev/stderr
Thank you for trying Materialize! We are interested in any and all feedback you
have, which may be able to improve both our software and your queries!

Please reach out at:

  Web: https://materialize.com
  GitHub discussions: https://github.com/MaterializeInc/materialize/discussions
  Slack: https://www.materialize.com/s/chat
  Email: support@materialize.com
  Twitter: @MaterializeInc

********************************* WARNING ********************************

You *should not* run production deployments using this Docker image.

This Docker image is *not* supported by Materialize.

This Docker image does *not* support version upgrades.

The performance characteristics of this Docker image are *not*
representative of the performance characteristics of our hosted offering.
This image bundles several services into the same container, while in our
hosted offering we run these services scaled across many machines.

********************************* WARNING ********************************
EOF

# Start PostgreSQL, unless suppressed.
if [ -z "${MZ_NO_BUILTIN_POSTGRES:-}" ]; then
  pg_ctlcluster 16 main start
  psql -U root -c "CREATE SCHEMA IF NOT EXISTS consensus"
  psql -U root -c "CREATE SCHEMA IF NOT EXISTS storage"
  psql -U root -c "CREATE SCHEMA IF NOT EXISTS adapter"
  psql -U root -c "CREATE SCHEMA IF NOT EXISTS tsoracle"
fi

# Start nginx to serve the console.
nginx

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
export MZ_BALANCER_SQL_LISTEN_ADDR=${MZ_BALANCER_SQL_LISTEN_ADDR:-0.0.0.0:6880}
export MZ_BALANCER_HTTP_LISTEN_ADDR=${MZ_BALANCER_HTTP_LISTEN_ADDR:-0.0.0.0:6881}
# Ideally we'd want to use the local path, but this parameter is passed through to clusterd
#export MZ_PERSIST_CONSENSUS_URL=${MZ_PERSIST_CONSENSUS_URL:-postgresql://root@%2Fvar%2Frun%2Fpostgresql:26257/?options=--search_path=consensus}
export MZ_PERSIST_CONSENSUS_URL=${MZ_PERSIST_CONSENSUS_URL:-postgresql://root@$(hostname):26257/?options=--search_path=consensus}
export MZ_PERSIST_BLOB_URL=${MZ_PERSIST_BLOB_URL:-file:///mzdata/persist/blob}
export MZ_ADAPTER_STASH_URL=${MZ_ADAPTER_STASH_URL:-postgresql://root@%2Fvar%2Frun%2Fpostgresql:26257/?options=--search_path=adapter}
export MZ_TIMESTAMP_ORACLE_URL=${MZ_TIMESTAMP_ORACLE_URL:-postgresql://root@%2Fvar%2Frun%2Fpostgresql:26257/?options=--search_path=tsoracle}
export MZ_ORCHESTRATOR=${MZ_ORCHESTRATOR:-process}
export MZ_ORCHESTRATOR_PROCESS_SECRETS_DIRECTORY=${MZ_ORCHESTRATOR_PROCESS_SECRETS_DIRECTORY:-/mzdata/secrets}
export MZ_ORCHESTRATOR_PROCESS_SCRATCH_DIRECTORY=${MZ_ORCHESTRATOR_PROCESS_SCRATCH_DIRECTORY:-/scratch}
export MZ_BOOTSTRAP_ROLE=${MZ_BOOTSTRAP_ROLE:-materialize}

# Supported replica sizes.
export MZ_CLUSTER_REPLICA_SIZES=${MZ_CLUSTER_REPLICA_SIZES:-$(cat <<EOF
{
  "25cc": {
    "cpu_exclusive": false,
    "cpu_limit": 0.5,
    "credits_per_hour": "0.25",
    "disk_limit": "7762MiB",
    "memory_limit": "3881MiB",
    "scale": 1,
    "workers": 1
  },
  "50cc": {
    "cpu_exclusive": true,
    "cpu_limit": 1,
    "credits_per_hour": "0.5",
    "disk_limit": "15525MiB",
    "memory_limit": "7762MiB",
    "scale": 1,
    "workers": 1
  },
  "100cc": {
    "cpu_exclusive": true,
    "cpu_limit": 2,
    "credits_per_hour": "1",
    "disk_limit": "31050MiB",
    "memory_limit": "15525MiB",
    "scale": 1,
    "workers": 2
  },
  "200cc": {
    "cpu_exclusive": true,
    "cpu_limit": 4,
    "credits_per_hour": "2",
    "disk_limit": "62100MiB",
    "memory_limit": "31050MiB",
    "scale": 1,
    "workers": 4
  },
  "300cc": {
    "cpu_exclusive": true,
    "cpu_limit": 6,
    "credits_per_hour": "3",
    "disk_limit": "93150MiB",
    "memory_limit": "46575MiB",
    "scale": 1,
    "workers": 6
  },
  "400cc": {
    "cpu_exclusive": true,
    "cpu_limit": 8,
    "credits_per_hour": "4",
    "disk_limit": "124201MiB",
    "memory_limit": "62100MiB",
    "scale": 1,
    "workers": 8
  },
  "600cc": {
    "cpu_exclusive": true,
    "cpu_limit": 12,
    "credits_per_hour": "6",
    "disk_limit": "186301MiB",
    "memory_limit": "93150MiB",
    "scale": 1,
    "workers": 12
  },
  "800cc": {
    "cpu_exclusive": true,
    "cpu_limit": 16,
    "credits_per_hour": "8",
    "disk_limit": "248402MiB",
    "memory_limit": "124201MiB",
    "scale": 1,
    "workers": 16
  },
  "1200cc": {
    "cpu_exclusive": true,
    "cpu_limit": 24,
    "credits_per_hour": "12",
    "disk_limit": "372603MiB",
    "memory_limit": "186301MiB",
    "scale": 1,
    "workers": 24
  },
  "1600cc": {
    "cpu_exclusive": true,
    "cpu_limit": 31,
    "credits_per_hour": "16",
    "disk_limit": "481280MiB",
    "memory_limit": "240640MiB",
    "scale": 1,
    "workers": 31
  },
  "3200cc": {
    "cpu_exclusive": true,
    "cpu_limit": 62,
    "credits_per_hour": "32",
    "disk_limit": "962560MiB",
    "memory_limit": "481280MiB",
    "scale": 1,
    "workers": 62
  }
}
EOF
)}

export MZ_BOOTSTRAP_DEFAULT_CLUSTER_REPLICA_SIZE="${MZ_BOOTSTRAP_DEFAULT_CLUSTER_REPLICA_SIZE:-25cc}"
export MZ_BOOTSTRAP_BUILTIN_SYSTEM_CLUSTER_REPLICA_SIZE="${MZ_BOOTSTRAP_BUILTIN_SYSTEM_CLUSTER_REPLICA_SIZE:-${MZ_BOOTSTRAP_DEFAULT_CLUSTER_REPLICA_SIZE}}"
export MZ_BOOTSTRAP_BUILTIN_PROBE_CLUSTER_REPLICA_SIZE="${MZ_BOOTSTRAP_BUILTIN_PROBE_CLUSTER_REPLICA_SIZE:-${MZ_BOOTSTRAP_DEFAULT_CLUSTER_REPLICA_SIZE}}"
export MZ_BOOTSTRAP_BUILTIN_SUPPORT_CLUSTER_REPLICA_SIZE="${MZ_BOOTSTRAP_BUILTIN_SUPPORT_CLUSTER_REPLICA_SIZE:-${MZ_BOOTSTRAP_DEFAULT_CLUSTER_REPLICA_SIZE}}"
export MZ_BOOTSTRAP_BUILTIN_CATALOG_SERVER_CLUSTER_REPLICA_SIZE="${MZ_BOOTSTRAP_BUILTIN_CATALOG_SERVER_CLUSTER_REPLICA_SIZE:-${MZ_BOOTSTRAP_DEFAULT_CLUSTER_REPLICA_SIZE}}"
export MZ_BOOTSTRAP_BUILTIN_ANALYTICS_CLUSTER_REPLICA_SIZE="${MZ_BOOTSTRAP_BUILTIN_ANALYTICS_CLUSTER_REPLICA_SIZE:-${MZ_BOOTSTRAP_DEFAULT_CLUSTER_REPLICA_SIZE}}"

export MZ_SYSTEM_PARAMETER_DEFAULT="${MZ_SYSTEM_PARAMETER_DEFAULT:-allowed_cluster_replica_sizes=\"25cc\",\"50cc\",\"100cc\",\"200cc\",\"300cc\",\"400cc\",\"600cc\",\"800cc\",\"1200cc\",\"1600cc\",\"3200cc\";enable_rbac_checks=false}"


if [ -z "${MZ_NO_TELEMETRY:-}" ]; then
    export MZ_SEGMENT_API_KEY=${MZ_SEGMENT_API_KEY:-hMWi3sZ17KFMjn2sPWo9UJGpOQqiba4A}
    export MZ_SEGMENT_CLIENT_SIDE=${MZ_SEGMENT_CLIENT_SIDE:-true}
fi

if [ -n "${MZ_RESTART_ON_FAILURE:-}" ]; then
    for ((i = 0; i < ${MZ_RESTART_LIMIT:-9999999999}; i++)); do
        # Run `environmentd` inside of an `if` to avoid tripping `set -e`
        # behavior.
        if environmentd "$@"; then
            code=$?
        else
            code=$?
        fi

        if [[ $code != 0 ]]; then
            echo "environmentd exited (code: $code); restarting in 5s..." >&2
            sleep 5
        else
            echo "environmentd exited gracefully; sleeping forever" >&2
            sleep infinity
        fi
    done
    echo "environmentd exited; giving up after $i tries" >&2
    exit "$code"
else
    exec environmentd "$@"
fi
