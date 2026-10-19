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
# materialize-sdk.sh -- lints and tests the Materialize SDK, which is its own
# Cargo workspace and so is not covered by the workspace-wide steps.

set -euo pipefail

cd "$(dirname "$0")/../.."

cargo deny --locked --manifest-path misc/materialize-sdk/Cargo.toml \
    check --config deny.toml licenses bans sources
cd misc/materialize-sdk
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked

# The Python package: its binding, the binding's stubs, and the conformance
# vectors through it.
venv="$(mktemp -d)/venv"
python3 -m venv "$venv"
"$venv/bin/pip" install --quiet "maturin>=1.9,<2"
(
    cd python
    export VIRTUAL_ENV="$venv" PATH="$venv/bin:$PATH"
    maturin develop --locked --quiet --extras test
    python -m mypy.stubtest materialize_sdk._core
    mypy --strict materialize_sdk
    python -m pytest --quiet tests
)
