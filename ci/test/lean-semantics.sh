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
# lean-semantics.sh: build the Lean 4 compute protocol semantics
# model in `doc/developer/semantics`. The Lean toolchain version is
# read from `lean-toolchain` and forwarded to the Dockerfile in the
# same directory, so the elan toolchain pin used by local developers
# and the CI image stay in lockstep.
#
# The Docker image is built locally on the agent and reused across CI
# runs by Docker's layer cache. There is no registry push. The image
# is cheap to rebuild from cold (no Mathlib dependency, see the
# Dockerfile's comment).

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

docker run --rm \
    --user "$(id -u):$(id -g)" \
    -v "$PWD/$semantics_dir/MzCompute:/workspace/MzCompute" \
    -v "$PWD/$semantics_dir/MzCompute.lean:/workspace/MzCompute.lean" \
    -w /workspace \
    "$image_tag" \
    lake build
