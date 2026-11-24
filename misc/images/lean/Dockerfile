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
# `/mz/misc/lean/.lake` at image build time. The project sits at its
# repository path because `lakefile.lean` finds design doc models relative to
# itself, in `/mz/doc/developer/design`.

MZFROM debian-base

# renovate: datasource=github-release-attachments depName=leanprover/elan
ARG ELAN_AMD64=v4.4.0@0e8250aa2be86c7af929ade68db617af9edc8b9ccf6eaf5ece228250d0d032e2
# renovate: datasource=github-release-attachments depName=leanprover/elan
ARG ELAN_ARM64=v4.4.0@45cde3006f82f98804c94cf2ca4949d117060a5f3cc8afa62f523759947ee21f

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

COPY lean-toolchain /mz/misc/lean/

# NOTE: the toolchain is kept whole although checking proofs needs only part of
# it. Batteries, a dependency of Aesop and Mathlib, has the `runLinter`
# executable among its default targets. Linking it needs the toolchain's static
# libraries and LLVM, so removing them breaks the dependency build below.
RUN ELAN_VERSION=${ELAN_AMD64%@*} \
    && case "$(uname -m)" in \
        x86_64) triple=x86_64-unknown-linux-gnu; sha256=${ELAN_AMD64#*@} ;; \
        aarch64) triple=aarch64-unknown-linux-gnu; sha256=${ELAN_ARM64#*@} ;; \
        *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;; \
    esac \
    && curl -fsSL -o /tmp/elan.tar.gz \
        "https://github.com/leanprover/elan/releases/download/$ELAN_VERSION/elan-$triple.tar.gz" \
    && echo "$sha256  /tmp/elan.tar.gz" | sha256sum -c - \
    && tar -C /tmp -xzf /tmp/elan.tar.gz \
    && /tmp/elan-init -y --no-modify-path --default-toolchain "$(cat /mz/misc/lean/lean-toolchain)" \
    && rm /tmp/elan.tar.gz /tmp/elan-init \
    && elan toolchain install "$(cat /mz/misc/lean/lean-toolchain)" \
    && chmod -R a+rX "$ELAN_HOME" \
    && lean --version

# Git refuses repositories owned by a different uid than the one running it,
# and Lake then re-clones the baked dependencies.
RUN git config --system --add safe.directory '*'

WORKDIR /mz/misc/lean

COPY lakefile.lean lake-manifest.json /mz/misc/lean/

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
    && chmod -R a+rwX /mz/misc/lean

COPY mz-lake /usr/local/bin/

ENTRYPOINT ["mz-lake"]
CMD ["build"]
