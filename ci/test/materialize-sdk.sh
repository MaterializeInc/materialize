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

cargo deny --locked --manifest-path misc/materialize-sdk/Cargo.toml --config deny.toml \
    check licenses bans sources
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

# The Node package: its WebAssembly binding, type-checked against the
# declarations wasm-bindgen generates, and the conformance vectors through it.
(
    cd node
    npm ci --quiet
    npm run build
    npm test
    # The published package holds the WebAssembly build.
    npm pack --dry-run --json | node -e '
        const files = JSON.parse(require("fs").readFileSync(0))[0].files.map((f) => f.path);
        if (!files.includes("core/materialize_sdk_node_bg.wasm")) {
            console.error("the package leaves out the WebAssembly build");
            process.exit(1);
        }'
)
