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

# tini -g forwards shutdown signals to both this shell and environmentd.
# Exit even if environmentd handles the signal by halting or exiting cleanly.
trap 'exit 143' TERM
trap 'exit 130' INT

while true; do
    if environmentd "$@"; then
        echo "environmentd exited gracefully; sleeping forever" >&2
        exec sleep infinity
    else
        status=$?
        if [ "$status" -ne 166 ]; then
            exit "$status"
        fi
        # mz_ore::halt! requests a restart without Kubernetes crash backoff.
        echo "environmentd halted; restarting" >&2
    fi
done
