#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -e

# Node name is provided via downward API
NODE_NAME=${NODE_NAME:-$(hostname)}
TAINT_KEY="disk-unconfigured"
TAINT_VALUE="true"
TAINT_EFFECT="NoSchedule"

echo "Starting taint management for node: $NODE_NAME"
echo "Action: $1"

# Check if necessary environment variables and files exist
if [ -z "$KUBERNETES_SERVICE_HOST" ] || [ -z "$KUBERNETES_SERVICE_PORT" ]; then
    echo "Error: Kubernetes service environment variables not found"
    exit 0  # Exit with success to avoid crash loop
fi

if [ ! -f "/var/run/secrets/kubernetes.io/serviceaccount/token" ]; then
    echo "Error: Service account token not found"
    exit 0  # Exit with success to avoid crash loop
fi

# Add the taint to the node
add_taint() {
    echo "Adding taint $TAINT_KEY=$TAINT_VALUE:$TAINT_EFFECT to node $NODE_NAME"

    kubectl --server="https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT}" \
            --token="$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" \
            --certificate-authority="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt" \
            taint nodes "$NODE_NAME" "$TAINT_KEY=$TAINT_VALUE:$TAINT_EFFECT" --overwrite || {
                echo "Warning: Failed to add taint, but continuing anyway"
            }
}

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
ACTION=${1:-"add"}

case "$ACTION" in
    add)
        add_taint
        ;;
    remove)
        remove_taint
        ;;
    *)
        echo "Usage: $0 [add|remove]"
        exit 0  # Exit with success to avoid crash loop
        ;;
esac

exit 0
