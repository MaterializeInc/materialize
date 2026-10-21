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

if [ -z "${MZ_EAT_MY_DATA:-}" ]; then
    unset LD_PRELOAD
else
    export LD_PRELOAD=libeatmydata.so
fi

# Exit code of mz_ore::restart_in_place!, which environmentd uses only for
# halts that are safe to repeat inside this container. Restarting here avoids
# Kubernetes crash backoff. Every other failure, including mz_ore::halt!
# (166), exits the container and leaves the restart to Kubernetes.
restart_in_place=168

while true; do
    if environmentd "$@"; then
        echo "environmentd exited gracefully; sleeping forever" >&2
        exec sleep infinity
    else
        status=$?
        if [ "$status" -ne "$restart_in_place" ]; then
            exit "$status"
        fi
        echo "environmentd requested a restart in place; restarting" >&2
    fi
done
