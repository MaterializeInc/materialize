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
# Checks that the default allocator is jemalloc on supported platforms, but can
# be disabled using --no-default-features
#
# Example usages:
#
#     $ ci/test/lint-allocator.sh
#
# To rewrite the stored files, pass the --rewrite option.
#
# See the [README](lint-allocator/README.md) for more details.

set -euo pipefail

targets=(
    aarch64-apple-darwin
    x86_64-unknown-linux-gnu
)

rewrite=false
resources="$(dirname "$0")/lint-allocator/"

if [[ "${1:-}" = --rewrite ]]; then
    shift
    rewrite=true
fi

function deps() {
    RUSTFLAGS="--target $target" cargo tree -i -p tikv-jemallocator -f ':{lib}:{f}' --prefix depth --locked "$@" || true
}

for target in "${targets[@]}"; do
    if $rewrite; then
        deps > "$resources/$target-default"
        deps --no-default-features > "$resources/$target-no-default-features"
    else
        diff --unified=0 "$resources/$target-default" <(deps)
        diff --unified=0 "$resources/$target-no-default-features" <(deps --no-default-features)
    fi
done
