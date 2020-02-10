#!/usr/bin/env bash

# Copyright Materialize, Inc. All rights reserved.
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
export CARGO_TARGET_DIR=miri-target

# At the moment only repr has tests meant to be run under miri.
pkgs=(
    repr
)

for pkg in "${pkgs[@]}"; do
    (cd src/"$pkg" && cargo miri test -- -- miri)
done
