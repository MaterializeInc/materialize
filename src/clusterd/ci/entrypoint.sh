#!/bin/sh

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -eu

# Defaults passed as environment variables, set only when unset so users can
# override them via env vars or command-line flags.
export CLUSTERD_STORAGE_CONTROLLER_LISTEN_ADDR="${CLUSTERD_STORAGE_CONTROLLER_LISTEN_ADDR:-0.0.0.0:2100}"
export CLUSTERD_COMPUTE_CONTROLLER_LISTEN_ADDR="${CLUSTERD_COMPUTE_CONTROLLER_LISTEN_ADDR:-0.0.0.0:2101}"
export CLUSTERD_INTERNAL_HTTP_LISTEN_ADDR="${CLUSTERD_INTERNAL_HTTP_LISTEN_ADDR:-0.0.0.0:6878}"
export CLUSTERD_SECRETS_READER="${CLUSTERD_SECRETS_READER:-local-file}"
export CLUSTERD_SECRETS_READER_LOCAL_FILE_DIR="${CLUSTERD_SECRETS_READER_LOCAL_DIR:-/mzdata/secrets}"

# In Kubernetes, advertise the pod FQDN for the CTP peer check and derive the
# process ordinal from the StatefulSet pod name (e.g. `cluster-0` -> `0`). Uses
# `$(hostname)` rather than `$HOSTNAME`, which POSIX sh does not define.
if [ "${KUBERNETES_SERVICE_HOST:-}" ]; then
    export CLUSTERD_GRPC_HOST="${CLUSTERD_GRPC_HOST:-$(hostname -f)}"
    if [ -z "${CLUSTERD_PROCESS:-}" ]; then
        _mz_pod=$(hostname)
        CLUSTERD_PROCESS="${_mz_pod##*-}"
    fi
    export CLUSTERD_PROCESS
fi

if [ -z "${MZ_EAT_MY_DATA:-}" ]; then
    unset LD_PRELOAD
else
    export LD_PRELOAD=libeatmydata.so
fi

exec clusterd "$@"
