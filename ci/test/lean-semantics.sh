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
# lean-semantics.sh — build the Lean 4 semantics model in
# `doc/developer/semantics`. The Lean toolchain version is read from
# `lean-toolchain` and forwarded to the Dockerfile in the same
# directory, so the elan toolchain pin used by local developers and the
# CI image stay in lockstep.
#
# The Docker image is built locally on the agent and reused across CI
# runs by Docker's layer cache. There is no registry push; the image is
# cheap to rebuild from cold (≈ apt + elan + one Lean toolchain).

set -euo pipefail

cd "$(dirname "$0")/../.."

semantics_dir="doc/developer/semantics"
lean_toolchain="$(tr -d '[:space:]' < "$semantics_dir/lean-toolchain")"
image_tag="mz-lean-semantics:latest"

docker build \
    --build-arg "LEAN_TOOLCHAIN=$lean_toolchain" \
    --tag "$image_tag" \
    -f "$semantics_dir/Dockerfile" \
    "$semantics_dir"

# Bind-mount only the Mz library sources over the image's
# placeholders. The image carries the prebuilt Mathlib oleans under
# `/workspace/.lake`; masking the full `/workspace` with a bind mount
# would hide them and force a fresh `lake exe cache get` on every run.
docker run --rm \
    --user "$(id -u):$(id -g)" \
    -v "$PWD/$semantics_dir/Mz:/workspace/Mz" \
    -v "$PWD/$semantics_dir/Mz.lean:/workspace/Mz.lean" \
    -w /workspace \
    "$image_tag" \
    lake build
