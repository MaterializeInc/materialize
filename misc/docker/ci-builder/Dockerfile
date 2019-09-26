# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

FROM ubuntu:bionic

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    clang \
    cmake \
    curl \
    gcc \
    g++ \
    git \
    gnupg2 \
    jq \
    libclang-dev \
    libssl-dev \
    libz-dev \
    openssh-client \
    pkg-config \
    rsync \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://github.com/benesch/autouseradd/releases/download/1.1.0/autouseradd-1.1.0-amd64.tar.gz \
    | tar xz -C /usr --strip-components 1

RUN curl -fsSL https://shellcheck.storage.googleapis.com/shellcheck-v0.7.0.linux.x86_64.tar.xz > shellcheck.tar.xz \
    && echo '39c501aaca6aae3f3c7fc125b3c3af779ddbe4e67e4ebdc44c2ae5cba76c847f  shellcheck.tar.xz' | sha256sum --check \
    && tar -xJf shellcheck.tar.xz -C /usr/local/bin --strip-components 1 shellcheck-v0.7.0/shellcheck \
    && rm shellcheck.tar.xz

# TODO(benesch): If we're still not using sccache in a few months (say, by
# January 2020), remove it from the image.
COPY --from=materialize/sccache:0.2.8 /usr/local/bin/sccache /usr/local/bin/sccache

COPY ssh_known_hosts /etc/ssh/ssh_known_hosts

COPY rust.asc .

RUN gpg --import rust.asc \
    && rm rust.asc \
    && echo "trusted-key 85AB96E6FA1BE5FE" >> ~/.gnupg/gpg.conf

ENTRYPOINT ["autouseradd", "--user", "materialize"]

WORKDIR /workdir

ARG RUST_DATE
ARG RUST_VERSION

RUN mkdir rust \
    && curl -fsSL https://static.rust-lang.org/dist$RUST_DATE/rust-$RUST_VERSION-x86_64-unknown-linux-gnu.tar.gz > rust.tar.gz \
    && curl -fsSL "https://static.rust-lang.org/dist$RUST_DATE/rust-$RUST_VERSION-x86_64-unknown-linux-gnu.tar.gz.asc" > rust.asc \
    && gpg --verify rust.asc rust.tar.gz \
    && tar -xzf rust.tar.gz -C rust --strip-components=1 \
    && rm -f rust.asc rust.tar.gz \
    && rust/install.sh --components=rustc,cargo,clippy-preview,rustfmt-preview,rust-std-x86_64-unknown-linux-gnu \
    && rm -rf /rust
