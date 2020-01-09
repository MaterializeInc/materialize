#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.
#
# cargo-test-miri.sh - runs cargo test under miri to check for undefined behaviour

set -euo pipefail

if [[ ! "${BUILDKITE-}" ]]; then
    rustup component add miri --toolchain nightly
fi

cd "$(dirname "$0")"/../../src/repr
cargo +nightly miri test -- -- miri
