# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Lean 4 toolchain + Mathlib-cache image for building the Materialize
# semantics model in this directory. The Lean version is supplied via
# build arg, which `ci/test/lean-semantics.sh` reads from
# `lean-toolchain`, so the elan toolchain pin used by local developers
# and the CI image stay in lockstep.
#
# The image bakes Mathlib's prebuilt oleans (via `lake exe cache get`)
# into `/workspace/.lake` so each CI run does not re-pull ~8000 cache
# files. `lake-manifest.json` pins the exact Mathlib revision; bumping
# it forces a layer rebuild.
#
# Runtime contract: the source files `Mz/` and `Mz.lean` are bind-
# mounted over the image's empty placeholders; everything else under
# `/workspace` (including `.lake/packages/mathlib` and its prebuilt
# `.lake/build/lib/Mathlib/`) is supplied by the image. Lake writes
# the Mz library's own build artifacts to the image's overlay
# filesystem; they are ephemeral and discarded when the container
# exits.

FROM ubuntu:26.04

ARG LEAN_TOOLCHAIN

# Install elan system-wide so the toolchain is usable by any uid that
# the container may be launched with (CI runs with the agent's uid via
# `--user`).
ENV ELAN_HOME=/opt/elan
ENV PATH=$ELAN_HOME/bin:$PATH
# Mathlib's `cache get` writes to `~/.cache`; route HOME to a writable
# location so any uid can run it.
ENV HOME=/tmp

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf \
        https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh \
        | sh -s -- --default-toolchain "$LEAN_TOOLCHAIN" -y --no-modify-path \
    && chmod -R a+rX "$ELAN_HOME" \
    && lean --version

# Trust every repository system-wide. The image bakes Mathlib's git
# checkout as root, but containers run as the agent's uid (`--user`).
# Without this, git's `safe.directory` check rejects the root-owned
# repo, lake falls through to "URL has changed", and re-clones every
# transitive dependency on every run.
RUN git config --system --add safe.directory '*'

WORKDIR /workspace

# Bake Mathlib into the image. The Lake project files are copied first
# so this layer invalidates only when the manifest or lakefile changes,
# not on every Mz/ source edit.
COPY lakefile.toml lean-toolchain lake-manifest.json ./

# Lake needs the library directory to exist before it will resolve;
# the placeholder is overwritten by the bind-mount at runtime.
RUN mkdir -p Mz \
    && touch Mz.lean \
    && lake update \
    && lake exe cache get \
    && chmod -R a+rwX /workspace
