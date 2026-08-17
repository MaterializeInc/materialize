# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Lean 4 toolchain image for checking the two-runtime compute command
# protocol model in this directory. The Lean version arrives as a build
# arg, which `ci/test/lean-protocol.sh` reads from `lean-toolchain`, so
# the elan pin a local developer uses and the one the image carries stay
# in lockstep.
#
# This model depends on core Lean only, not Mathlib, so there is no
# olean cache to bake and no `lake exe cache get` step. Compare
# `doc/developer/semantics/Dockerfile`, which does need both.
#
# Runtime contract: `Protocol/` and `Protocol.lean` are bind-mounted
# over the image's empty placeholders, and everything else under
# `/workspace` comes from the image. Do not bind-mount `/workspace`
# itself, which would mask the resolved `.lake` directory.

FROM ubuntu:26.04

ARG LEAN_TOOLCHAIN

# elan goes in system-wide so the toolchain works under any uid, since
# CI runs the container with the agent's uid via `--user`.
ENV ELAN_HOME=/opt/elan
ENV PATH=$ELAN_HOME/bin:$PATH
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

# The baked project directory is root-owned while containers run as the
# caller's uid. Without this, git's `safe.directory` check makes lake
# treat the checkout as untrusted.
RUN git config --system --add safe.directory '*'

WORKDIR /workspace

COPY lakefile.toml lean-toolchain ./

# Lake resolves the library only if its directory exists. Both
# placeholders are replaced by bind-mounts at runtime.
RUN mkdir -p Protocol \
    && touch Protocol.lean \
    && lake update \
    && chmod -R a+rwX /workspace
