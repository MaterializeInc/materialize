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
# Compare dependencies and their flags against a ground truth. This serves against accidentally enabling dependencies
# or feature flags.
#
# Example usages:
#
#     $ ci/test/lint-deps.sh
#
# To rewrite the stored files, pass the --rewrite option.
#
# See the [README](lint-deps/README.md) for more details.

set -euo pipefail

# Explicitly name targets to check dependencies. We support Apple on ARM64 and x86_64 and Linux on x86_64.
targets=(
    aarch64-apple-darwin
    x86_64-apple-darwin
    x86_64-unknown-linux-gnu
)

# List of crates to include in the dependency lint, including an explanation why they're listed.
crates=(
    # Checks that the default allocator is jemalloc on supported platforms, but can
    # be disabled using --no-default-features
    tikv_jemalloc_ctl
    tikv_jemallocator
)

rewrite=false
resources="$(dirname "$0")/lint-deps/"

if [[ "${1:-}" = --rewrite ]]; then
    shift
    rewrite=true
fi

function deps() {
    # We iterate $crates instead of passing all to `cargo tree` because it produces no
    # output if there is no dependency to the specified package.
    for crate in "${crates[@]}"; do
        echo "$crate"
        # Gather data for the current target, reverse lines, find the dependency hierarchy to the root, reverse lines.
        cargo tree --format ':{lib}:{f}' \
            --prefix depth \
            --locked \
            --target "$target" \
            "$@" \
            | tac \
            | awk -F: 'BEGIN {f=-1} /^([0-9]+):'"$crate"'/{print;f=$0-1} f==$1{print;f=f-1}' \
            | tac
    done
}

echo "Linting dependencies -- if the check fails, consult ci/test/lint-deps/README.md"

for target in "${targets[@]}"; do
    if $rewrite; then
        deps > "$resources/$target-default"
        deps --no-default-features > "$resources/$target-no-default-features"
    else
        diff --unified=0 "$resources/$target-default" <(deps)
        diff --unified=0 "$resources/$target-no-default-features" <(deps --no-default-features)
    fi
done
