# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM ubuntu:xenial

RUN apt-get update && apt-get install -y --no-install-recommends \
    bsdmainutils \
    ca-certificates \
    cmake \
    curl \
    docker.io \
    gcc \
    g++ \
    git \
    gnupg2 \
    jq \
    libz-dev \
    openssh-client \
    pkg-config \
    python3 \
    python3-pip \
    python3-setuptools \
    rsync \
    ruby \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://github.com/benesch/autouseradd/releases/download/1.1.0/autouseradd-1.1.0-amd64.tar.gz \
    | tar xz -C /usr --strip-components 1

RUN curl -fsSL https://shellcheck.storage.googleapis.com/shellcheck-v0.7.0.linux.x86_64.tar.xz > shellcheck.tar.xz \
    && echo '39c501aaca6aae3f3c7fc125b3c3af779ddbe4e67e4ebdc44c2ae5cba76c847f  shellcheck.tar.xz' | sha256sum --check \
    && tar -xJf shellcheck.tar.xz -C /usr/local/bin --strip-components 1 shellcheck-v0.7.0/shellcheck \
    && rm shellcheck.tar.xz

COPY ssh_known_hosts /etc/ssh/ssh_known_hosts

COPY rust.asc .

RUN gpg --import rust.asc \
    && rm rust.asc \
    && echo "trusted-key 85AB96E6FA1BE5FE" >> ~/.gnupg/gpg.conf

ENTRYPOINT ["autouseradd", "--user", "materialize"]

WORKDIR /workdir

ARG RUST_DATE
ARG RUST_VERSION
ARG RUST_COMPONENTS

RUN mkdir rust \
    && curl -fsSL https://static.rust-lang.org/dist$RUST_DATE/rust-$RUST_VERSION-x86_64-unknown-linux-gnu.tar.gz > rust.tar.gz \
    && curl -fsSL https://static.rust-lang.org/dist$RUST_DATE/rust-$RUST_VERSION-x86_64-unknown-linux-gnu.tar.gz.asc > rust.asc \
    && gpg --verify rust.asc rust.tar.gz \
    && tar -xzf rust.tar.gz -C rust --strip-components=1 \
    && rm -f rust.asc rust.tar.gz \
    && rust/install.sh --components=$RUST_COMPONENTS \
    && curl -fsSL https://static.rust-lang.org/dist/rustc-$RUST_VERSION-src.tar.gz > rust-src.tar.gz \
    && curl -fsSL https://static.rust-lang.org/dist/rustc-$RUST_VERSION-src.tar.gz.asc > rust-src.asc \
    && gpg --verify rust-src.asc rust-src.tar.gz \
    && mkdir -p /usr/local/lib/rustlib/src/rust \
    && tar -xzf rust-src.tar.gz -C /usr/local/lib/rustlib/src/rust --strip-components=1 \
    && rm -f rust-src.asc rust-src.tar.gz \
    && case $RUST_COMPONENTS in *miri*) PATH=$PATH:/root/.cargo/bin cargo miri setup && mv /root/.cargo/bin/xargo /usr/local/bin/xargo;; esac \
    && rm -rf /rust

RUN cargo install --root /usr/local cargo-deb cargo-deny

COPY requirements.txt /workdir/requirements.txt

RUN pip3 install -r /workdir/requirements.txt && rm /workdir/requirements.txt

RUN curl -fsSL "https://github.com/docker/compose/releases/download/1.25.4/docker-compose-Linux-x86_64" > /usr/local/bin/docker-compose \
    && chmod +x /usr/local/bin/docker-compose

# TODO(benesch): why is this required now with nightly?
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=cc

ENV MZ_DEV_CI_BUILDER=1
