#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This script wraps the standard FoundationDB entrypoint to add
# automatic database initialization after the server starts.

set -e

CLUSTER_FILE="${FDB_CLUSTER_FILE:-/var/fdb/fdb.cluster}"

# Function to initialize the database
initialize_database() {
    echo "Checking FoundationDB status..."

    # If status minimal succeeds, database is already configured
    if fdbcli -C "$CLUSTER_FILE" --exec "status minimal" --timeout 5 2>/dev/null; then
        echo "Database already configured"
        return 0
    fi

    # Configure the database
    echo "Configuring new single ssd database..."
    fdbcli -C "$CLUSTER_FILE" --exec "configure new single ssd" --timeout 30
    echo "Database configured successfully"
}

# Run initialization in background after a delay to let fdbserver start
(
    sleep 1
    initialize_database
) &

# Execute the original entrypoint
exec /var/fdb/scripts/fdb.bash "$@"
