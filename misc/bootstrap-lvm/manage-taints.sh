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

# Node name is provided via downward API
NODE_NAME=${NODE_NAME:-$(hostname)}
TAINT_KEY="disk-unconfigured"

echo "Starting taint management for node: $NODE_NAME"
echo "Action: $1"

# Check if necessary environment variables and files exist
if [ -z "$KUBERNETES_SERVICE_HOST" ] || [ -z "$KUBERNETES_SERVICE_PORT" ]; then
    echo "Error: Kubernetes service environment variables not found"
    exit 1
fi

if [ ! -f "/var/run/secrets/kubernetes.io/serviceaccount/token" ]; then
    echo "Error: Service account token not found"
    exit 1
fi

# Remove the taint from the node
remove_taint() {
    echo "Removing taint $TAINT_KEY from node $NODE_NAME"

    kubectl --server="https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT}" \
            --token="$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" \
            --certificate-authority="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt" \
            taint nodes "$NODE_NAME" "$TAINT_KEY-" || {
                echo "Warning: Failed to remove taint, but continuing anyway"
            }
}

# Main execution
ACTION=${1:-"remove"}

case "$ACTION" in
    remove)
        remove_taint
        ;;
    *)
        echo "Usage: $0 [remove]"
        exit 1
        ;;
esac

exit 0
