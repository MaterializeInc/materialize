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
# cargo-test-miri.sh — runs subset of unit tests under miri to check for
# undefined behaviour.

set -euo pipefail

# miri artifacts are thoroughly incompatible with normal build artifacts,
# so keep them away from the `target` directory.
export CARGO_TARGET_DIR="$PWD/miri-target"
export MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-strict-provenance"

PARTITION=$((${BUILDKITE_PARALLEL_JOB:-0}+1))
TOTAL=${BUILDKITE_PARALLEL_JOB_COUNT:-1}

# exclude network-based and more complex tests which run out of memory
cargo miri nextest run -j"$(nproc)" --partition "count:$PARTITION/$TOTAL" --no-fail-fast --workspace --exclude 'mz-environmentd*' --exclude 'mz-compute-client*' --exclude 'mz-compute-types*' --exclude 'mz-ssh-util*' --exclude 'mz-rocksdb*' --exclude 'mz-fivetran-destination*'
