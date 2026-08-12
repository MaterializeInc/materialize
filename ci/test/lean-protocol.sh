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
# lean-protocol.sh: check the Lean 4 model of the two-runtime compute
# command protocol in
# `doc/developer/design/20260720_two_runtime_compute/protocol`.
#
# Reads the Lean version from `lean-toolchain` and forwards it to the
# Dockerfile beside it, so a local elan pin and the CI image stay in
# lockstep. The image is built locally and reused through Docker's layer
# cache; there is no registry push, and a cold build is roughly apt +
# elan + one Lean toolchain. The model needs core Lean only, so unlike
# `lean-semantics.sh` there is no Mathlib cache to fetch.
#
# The library sets `warningAsError`, so an unproved goal fails this
# script rather than passing with a warning. That matters: the model
# exists because a stated-but-unchecked invariant is how the protocol
# bug it now rules out survived in the first place.
#
# Pass a command to run something else in the image, for example:
#   ci/test/lean-protocol.sh lake env lean Protocol/TwoRuntime.lean

set -euo pipefail

cd "$(dirname "$0")/../.."

protocol_dir="doc/developer/design/20260720_two_runtime_compute/protocol"
lean_toolchain="$(tr -d '[:space:]' < "$protocol_dir/lean-toolchain")"
image_tag="mz-protocol-lean:latest"

if [[ $# -eq 0 ]]; then
    set -- lake build
fi

docker build \
    --build-arg "LEAN_TOOLCHAIN=$lean_toolchain" \
    --tag "$image_tag" \
    "$protocol_dir"

# Bind-mount only the library sources over the image's placeholders.
# Masking all of /workspace would hide the resolved `.lake`.
docker run --rm \
    --user "$(id -u):$(id -g)" \
    -v "$PWD/$protocol_dir/Protocol:/workspace/Protocol" \
    -v "$PWD/$protocol_dir/Protocol.lean:/workspace/Protocol.lean" \
    -w /workspace \
    "$image_tag" \
    "$@"
