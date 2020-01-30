#!/usr/bin/env bash

# Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
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
