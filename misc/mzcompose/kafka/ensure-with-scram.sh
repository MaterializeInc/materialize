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
# Drop-in replacement for cp-kafka's /etc/confluent/docker/ensure that
# additionally bootstraps KRaft SCRAM users at storage-format time.
#
# In Kafka 4.x KRaft, the runtime `kafka-configs --alter --add-config=SCRAM-*`
# path is fragile: altering both SCRAM-SHA-256 and SCRAM-SHA-512 for the same
# user in one request is rejected, and splitting into two requests creates
# records that the broker rejects as "invalid credentials" during SASL auth.
# `kafka-storage format --add-scram ...` at first boot is the supported path.
#
# Configure users by setting KAFKA_INIT_SCRAM_USERS to a semicolon-separated
# list of entries of the form:
#   SCRAM-SHA-256=[name=alice,password=secret];SCRAM-SHA-512=[name=alice,password=secret]

set -euo pipefail

# shellcheck source=/dev/null
. /etc/confluent/docker/bash-config

export KAFKA_DATA_DIRS=${KAFKA_DATA_DIRS:-"/var/lib/kafka/data"}
echo "===> Check if $KAFKA_DATA_DIRS is writable ..."
# cp-kafka 7.x ships a `dub` helper; cp-kafka 8.x renamed it to `ub`.
if command -v ub >/dev/null 2>&1; then
    ub path "$KAFKA_DATA_DIRS" writable
else
    dub path "$KAFKA_DATA_DIRS" writable
fi

echo "===> Using provided cluster id $CLUSTER_ID ..."

format_args=(--cluster-id="$CLUSTER_ID" -c /etc/kafka/kafka.properties)
if [[ -n "${KAFKA_INIT_SCRAM_USERS:-}" ]]; then
    IFS=';' read -ra entries <<<"$KAFKA_INIT_SCRAM_USERS"
    for entry in "${entries[@]}"; do
        format_args+=(--add-scram "$entry")
    done
fi

# Not erroring out if storage is already formatted matches the original
# cp-kafka ensure script. --add-scram only applies on first format.
result=$(kafka-storage format "${format_args[@]}" 2>&1) || \
    echo "$result" | grep -i "already formatted" || \
    { echo "$result" && exit 1; }
echo "$result"
