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
# run-environmentd.sh — start a local environmentd for the Kafka listener demo.
#
# The instance keeps its metadata in its own CockroachDB database and its blobs
# in its own directory, so it neither reuses nor resets the state of other local
# instances. Pass --fresh to start over. Any further arguments are passed to
# bin/environmentd.

set -euo pipefail

cd "$(dirname "$0")/../.."

crdb=${MZDEV_CRDB:-postgres://root@localhost:26257}
db=kafka_listener_demo
blob_dir=${MZ_BLOB_DIR:-$PWD/mzdata/kafka-listener-demo/blob}

if [[ "${1:-}" == --fresh ]]; then
    shift
    echo "Dropping database $db and removing $blob_dir"
    psql "$crdb/defaultdb" -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS $db CASCADE"
    rm -rf "$blob_dir"
    # The process orchestrator's record of the old replicas. environmentd halts at startup if it
    # lists replica IDs that the new catalog has not allocated yet.
    if [[ -f mzdata/environment-id ]]; then
        rm -rf "${TMPDIR:-/tmp}/environmentd-$(cat mzdata/environment-id)"
    fi
fi

psql "$crdb/defaultdb" -v ON_ERROR_STOP=1 -q -c "CREATE DATABASE IF NOT EXISTS $db"
mkdir -p "$blob_dir"

# rocksdb's build script needs libclang. Xcode's command line tools ship one.
if [[ -z "${LIBCLANG_PATH:-}" && -f /Library/Developer/CommandLineTools/usr/lib/libclang.dylib ]]; then
    export LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib
fi

exec bin/environmentd --optimized \
    --postgres "$crdb/$db" \
    --blob "file://$blob_dir" \
    "$@"
