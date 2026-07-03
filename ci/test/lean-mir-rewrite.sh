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
# mean zero `sorry`s. The permanent-sorry invariant (a rule whose RHS is a
# Rust builtin applier or otherwise depends on opaque Rust metadata, e.g. a
# negation table) is enforced separately below: the count of
# `-- PERMANENT SORRY` markers in the sources must match exactly, so such a
# rule cannot be added (or removed) without a deliberate update to this guard.
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

# Guard the permanent-sorry invariant: builtin-applier and opaque-metadata
# obligations (slices 4-6) carry the `-- PERMANENT SORRY` marker and are the
# only never-provable sorries. A pre-existing-gap sorry uses a distinct marker
# and must not be counted here. `grep` exits non-zero when there are no
# matches. `|| true` keeps `set -o pipefail` from treating that as
# script-fatal, so the count check below reports a proper error (a zero count
# is now a failure, since markers are expected).
expected_permanent=10  # const_fold, if_err_cond, null_prop_binary, err_prop_binary,
                      # null_prop_variadic, err_prop_variadic (builtin RHS)
                      # + not_binary_negate (opaque negate table)
                      # + flatten_coalesce, flatten_greatest, flatten_least (opaque
                      # non-Bool variadic RHS: Coalesce/Greatest/Least return
                      # first-non-null/max/min, outside the two-valued Bool model).
                      # isnull_fold and flatten_and/flatten_or prove outright (no sorry).
permanent=$(grep -rho "PERMANENT SORRY" "$lean_dir/MirRewrite" | wc -l || true)
if [ "$permanent" -ne "$expected_permanent" ]; then
    echo "error: found $permanent PERMANENT SORRY marker(s); expected $expected_permanent" >&2
    exit 1
fi
