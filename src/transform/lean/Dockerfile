# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.

# Lean 4 toolchain image for building the `MirRewrite` verification library
# (the DSL-generated rewrite-soundness theorems plus their denotational
# semantics). Modeled on doc/developer/semantics/Dockerfile from the
# lean-semantics work, but Mathlib-free: this project depends only on Lean 4
# core, so there is no `lake exe cache get` bake and `lake build` is cheap from
# cold (apt + elan + one toolchain).
#
# The Lean version is supplied via the LEAN_TOOLCHAIN build arg, which
# ci/test/lean-mir-rewrite.sh reads from `lean-toolchain`, so the elan pin used
# by local developers and the CI image stay in lockstep. Diverging the arg from
# the project's `lean-toolchain` produces spurious tactic failures, so they must
# match.
#
# Runtime contract: the project directory (this directory) is bind-mounted over
# /workspace at run time; lake writes its build artifacts to the mounted
# `.lake/` (git-ignored, ephemeral in CI).

FROM ubuntu:26.04

ARG LEAN_TOOLCHAIN

# elan system-wide so any uid (CI runs with the agent's uid via `--user`) can
# use the toolchain. HOME routed to a writable location for the same reason.
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

# Trust every repository so git's safe.directory check does not reject the
# bind-mounted, differently-owned workspace.
RUN git config --system --add safe.directory '*'

WORKDIR /workspace
