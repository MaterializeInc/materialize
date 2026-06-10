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

# We pass default arguments as environment variables, and only if those
# environment variables do not already exist, to allow users to override these
# arguments when running the container via either environment variables or
# command-line arguments.
export CLUSTERD_STORAGE_CONTROLLER_LISTEN_ADDR=${CLUSTERD_STORAGE_CONTROLLER_LISTEN_ADDR:-0.0.0.0:2100}
export CLUSTERD_COMPUTE_CONTROLLER_LISTEN_ADDR=${CLUSTERD_COMPUTE_CONTROLLER_LISTEN_ADDR:-0.0.0.0:2101}
export CLUSTERD_INTERNAL_HTTP_LISTEN_ADDR=${CLUSTERD_INTERNAL_HTTP_LISTEN_ADDR:-0.0.0.0:6878}
export CLUSTERD_SECRETS_READER=${CLUSTERD_SECRETS_READER:-local-file}
export CLUSTERD_SECRETS_READER_LOCAL_FILE_DIR=${CLUSTERD_SECRETS_READER_LOCAL_DIR:-/mzdata/secrets}

if [[ "${KUBERNETES_SERVICE_HOST:-}" ]]; then
    # Pass the host's FQDN as the host to be used for GRPC request validation
    # only when running in Kubernetes. In other contexts (like when running
    # locally, or in Docker), this is likely not desirable.
    export CLUSTERD_GRPC_HOST=${CLUSTERD_GRPC_HOST:-$(hostname --fqdn)}

    # When running in Kubernetes, pass the StatefulSet replica's ordinal index
    # as the process index.
    export CLUSTERD_PROCESS=${CLUSTERD_PROCESS:-${HOSTNAME##*-}}
fi

if [ -z "${MZ_EAT_MY_DATA:-}" ]; then
    unset LD_PRELOAD
else
    export LD_PRELOAD=libeatmydata.so
fi

exec clusterd "$@"
