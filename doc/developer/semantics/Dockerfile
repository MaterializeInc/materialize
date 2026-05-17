# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# Minimal Lean 4 toolchain image for building the Materialize semantics
# model in this directory. The Lean version is supplied via build arg,
# which `ci/test/lean-semantics.sh` reads from `lean-toolchain`, so the
# elan toolchain pin used by local developers and the CI image stay in
# lockstep.
#
# The image is intentionally small. It installs elan, a single Lean
# toolchain, and the system packages elan needs to download. No Mathlib,
# no editor, no test runners. Adding those is a deliberate future
# decision, not a passive consequence of inheriting a heavier base.

FROM ubuntu:26.04

ARG LEAN_TOOLCHAIN

# Install elan system-wide so the toolchain is usable by any uid that
# the container may be launched with (CI runs with the agent's uid via
# `--user`). Writes go to `/workspace/.lake/`, which is bind-mounted.
ENV ELAN_HOME=/opt/elan
ENV PATH=$ELAN_HOME/bin:$PATH

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

WORKDIR /workspace
