# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Lean 4 toolchain image for building the compute protocol semantics
# model in this directory. The Lean version is supplied via build
# arg, which `ci/test/lean-semantics.sh` reads from `lean-toolchain`,
# so the elan toolchain pin used by local developers and the CI image
# stay in lockstep.
#
# Unlike a Mathlib-backed Lean project, this one has no `[[require]]`
# in `lakefile.toml`: the compute protocol model is finite-state and
# needs nothing beyond core Lean (structural recursion, `List`,
# `Option`, `Bool`). This keeps the image small and the build fast.
# If a later phase needs Mathlib, add a `[[require]]` entry and a
# `lake exe cache get` step here, following the pattern in the (at
# time of writing, unmerged) MaterializeInc/materialize#36614.
#
# Runtime contract: the source under `MzCompute/` and `MzCompute.lean`
# is bind-mounted over the image's empty placeholders. Lake writes
# build artifacts to the image's overlay filesystem. They are
# ephemeral and discarded when the container exits.

FROM ubuntu:26.04

ARG LEAN_TOOLCHAIN

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

RUN git config --system --add safe.directory '*'

WORKDIR /workspace

COPY lakefile.toml lean-toolchain ./

RUN mkdir -p MzCompute \
    && touch MzCompute.lean \
    && lake update \
    && lake build \
    && chmod -R a+rwX /workspace
