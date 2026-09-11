#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Starts a single-node garage and performs the cluster setup a fresh node
# needs before it accepts S3 requests: a layout, the S3 key from
# GARAGE_ACCESS_KEY_ID/GARAGE_SECRET_ACCESS_KEY, and the buckets named in
# GARAGE_BUCKETS. Garage only accepts these over RPC from a running node, so
# they cannot be baked into the image. The ready marker tells the healthcheck
# that setup has completed, and skips it when the container restarts with its
# metadata intact.

set -euo pipefail

: "${GARAGE_ACCESS_KEY_ID:?}"
: "${GARAGE_SECRET_ACCESS_KEY:?}"
: "${GARAGE_BUCKETS:=}"

ready=/var/lib/garage/meta/ready

garage server &
pid=$!
trap 'kill "$pid"' TERM INT

until garage status >/dev/null 2>&1; do
    sleep 0.2
done

if [[ ! -f "$ready" ]]; then
    # `node id` prints `<id>@<rpc addr>`, and `layout assign` wants the id.
    node_id=$(garage node id -q | cut -d@ -f1)
    # The capacity only weights data placement across nodes, so any value
    # works for one node.
    garage layout assign -z dc1 -c 1T "$node_id"
    garage layout apply --version 1
    garage key import --yes -n persist "$GARAGE_ACCESS_KEY_ID" "$GARAGE_SECRET_ACCESS_KEY"
    for bucket in $GARAGE_BUCKETS; do
        garage bucket create "$bucket"
        garage bucket allow --read --write --owner "$bucket" --key persist
    done
    touch "$ready"
fi

wait "$pid"
