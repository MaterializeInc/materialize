# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Lean 4 toolchain for the Lake project in `misc/lean`. The toolchain comes
# from that project's `lean-toolchain`, so a local elan install and this image
# use the same Lean version. The project's dependencies are built into
# `/workspace/.lake` at image build time.

MZFROM debian-base

ARG ELAN_VERSION=v4.2.4

ENV ELAN_HOME=/opt/elan
ENV PATH=$ELAN_HOME/bin:$PATH

RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        jq \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY lean-toolchain /workspace/

# Checking proofs needs the `.olean*`, `.ir` and shared libraries. The static
# libraries, LLVM, `.ilean` files and sources are deleted, which saves 0.7 GB.
# NOTE: without them nothing can link an executable, so `lean_exe`, `lake exe`
# (including Mathlib's `lake exe cache get`) and `precompileModules` fail.
# Keep them if a dependency needs any of these.
RUN case "$(uname -m)" in \
        x86_64) triple=x86_64-unknown-linux-gnu; sha256=42b94d4244e8353142c456ec0e4ca6528fd898a6c604d4059f494e706e431f63 ;; \
        aarch64) triple=aarch64-unknown-linux-gnu; sha256=05febd124d84ebf994b2e7479922a5650b1e950c17ae3bd1ddd776b65bb72bf9 ;; \
        *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;; \
    esac \
    && curl -fsSL -o /tmp/elan.tar.gz \
        "https://github.com/leanprover/elan/releases/download/$ELAN_VERSION/elan-$triple.tar.gz" \
    && echo "$sha256  /tmp/elan.tar.gz" | sha256sum -c - \
    && tar -C /tmp -xzf /tmp/elan.tar.gz \
    && /tmp/elan-init -y --no-modify-path --default-toolchain "$(cat /workspace/lean-toolchain)" \
    && rm /tmp/elan.tar.gz /tmp/elan-init \
    && elan toolchain install "$(cat /workspace/lean-toolchain)" \
    && toolchain="$(echo "$ELAN_HOME"/toolchains/*)" \
    && find "$toolchain" \( -name '*.a' -o -name '*.ilean' \) -delete \
    && rm -rf "$toolchain/src" "$toolchain"/lib/libLLVM.so.* "$toolchain"/lib/libclang-cpp.so.* \
    && chmod -R a+rX "$ELAN_HOME" \
    && lean --version

# Lake refuses dependency checkouts owned by a different uid than the one
# running it, and re-clones them instead.
RUN git config --system --add safe.directory '*'

WORKDIR /workspace

COPY lakefile.toml lake-manifest.json /workspace/

# Builds the dependency closure. Building the placeholder library alone only
# clones the dependencies, because Lake compiles just the modules a target
# imports, so each package's default targets are built by name. NOTE: a
# package that declares no default targets builds nothing here and compiles
# on every run instead. One `lake build` for all packages fails if any of them
# fails. With no dependencies it builds the placeholder.
RUN mkdir Mz \
    && touch Mz.lean \
    && targets="$(jq -r '.packages[] | "@" + .name' lake-manifest.json)" \
    && lake build $targets \
    && rm -rf Mz Mz.lean .lake/build \
    && chmod -R a+rwX /workspace

COPY mz-lake /usr/local/bin/

ENTRYPOINT ["mz-lake"]
CMD ["build"]
