#!/usr/bin/env bash

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# lean-mir-rewrite.sh — build the `MirRewrite` Lean verification library in a
# reproducible container and fail if `lake build` is not green. The image pins
# the exact Lean toolchain from `src/transform/lean/lean-toolchain`, so CI and
# local developers check the theorems against the same compiler.
#
# "Green" means: compiles with zero errors and only the intended, documented
# `sorry`s (provable-later theorems, one pre-existing infra gap). It does NOT
# mean zero `sorry`s. The permanent-sorry invariant (builtin-applier RHS, none
# until slice 4) is enforced separately below: any `-- PERMANENT SORRY` marker
# in the sources fails the build, since none should exist yet.
#
# The image is built locally and reused via Docker's layer cache; there is no
# registry push. It is Mathlib-free, so a cold build is just apt + elan + one
# toolchain.

set -euo pipefail

cd "$(dirname "$0")/../.."

lean_dir="src/transform/lean"
lean_toolchain="$(tr -d '[:space:]' < "$lean_dir/lean-toolchain")"
image_tag="mz-lean-mir-rewrite:latest"

docker build \
    --build-arg "LEAN_TOOLCHAIN=$lean_toolchain" \
    --tag "$image_tag" \
    -f "$lean_dir/Dockerfile" \
    "$lean_dir"

# Bind-mount the project over /workspace; lake writes artifacts to the mounted
# .lake/ (git-ignored, discarded after the run).
docker run --rm \
    --user "$(id -u):$(id -g)" \
    -v "$PWD/$lean_dir:/workspace" \
    -w /workspace \
    "$image_tag" \
    lake build

# Guard the permanent-sorry invariant: builtin-applier obligations (slices 4-6)
# carry the `-- PERMANENT SORRY` marker and are the only never-provable sorries.
# None should exist before slice 4; a pre-existing-gap sorry uses a distinct
# marker and must not be counted here.
# `grep` exits non-zero when there are no matches, which is the passing case
# here, so `|| true` keeps `set -o pipefail` from aborting the script.
permanent=$(grep -rho "PERMANENT SORRY" "$lean_dir/MirRewrite" | wc -l || true)
if [ "$permanent" -ne 0 ]; then
    echo "error: found $permanent PERMANENT SORRY marker(s); expected 0 before slice 4" >&2
    exit 1
fi
