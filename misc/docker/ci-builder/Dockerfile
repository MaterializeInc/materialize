# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

FROM ubuntu:bionic

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    clang \
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

COPY --from=materialize/sccache:0.2.8 /usr/local/bin/sccache /usr/local/bin/sccache

COPY ssh_known_hosts /etc/ssh/ssh_known_hosts

COPY rust.asc .

RUN gpg --import rust.asc \
    && rm rust.asc \
    && echo "trusted-key 85AB96E6FA1BE5FE" >> ~/.gnupg/gpg.conf

ENTRYPOINT ["autouseradd", "--user", "materialize"]

WORKDIR /workdir

ARG RUST_VERSION

RUN mkdir rust \
    && curl -fsSL https://static.rust-lang.org/dist/rust-$RUST_VERSION-x86_64-unknown-linux-gnu.tar.gz > rust.tar.gz \
    && curl -fsSL "https://static.rust-lang.org/dist/rust-$RUST_VERSION-x86_64-unknown-linux-gnu.tar.gz.asc" > rust.asc \
    && gpg --verify rust.asc rust.tar.gz \
    && tar -xzf rust.tar.gz -C rust --strip-components=1 \
    && rm -f rust.asc rust.tar.gz \
    && rust/install.sh --components=rustc,cargo,clippy-preview,rustfmt-preview,rust-std-x86_64-unknown-linux-gnu \
    && rm -rf /rust
