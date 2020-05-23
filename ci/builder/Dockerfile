# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Step 1. Build a cross-compiling toolchain that targets the oldest version of
# Linux that we support.

FROM ubuntu:bionic AS crosstool

WORKDIR /scratch

RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf \
    ca-certificates \
    curl \
    file \
    flex \
    libc-dev \
    gawk \
    g++ \
    gcc \
    gnupg2 \
    help2man \
    libncurses-dev \
    libtool-bin \
    make \
    patch \
    texinfo \
    unzip \
    xz-utils

COPY crosstool.defconfig crosstool.asc ./

RUN gpg --import crosstool.asc \
    && rm crosstool.asc \
    && echo "trusted-key 7848649B11D618A4" >> ~/.gnupg/gpg.conf \
    && mkdir crosstool \
    && curl -fsSL http://crosstool-ng.org/download/crosstool-ng/crosstool-ng-1.24.0.tar.xz > crosstool.tar.xz \
    && curl -fsSL http://crosstool-ng.org/download/crosstool-ng/crosstool-ng-1.24.0.tar.xz.sig > crosstool.sig \
    && gpg --verify crosstool.sig crosstool.tar.xz \
    && tar -xf crosstool.tar.xz -C crosstool --strip-components=1 \
    && rm crosstool.sig crosstool.tar.xz \
    && (cd crosstool && ./configure && make install) \
    && rm -rf crosstool \
    && DEFCONFIG=crosstool.defconfig ct-ng defconfig \
    && rm crosstool.defconfig \
    && ct-ng build

# Step 2. Import the cross-compiling toolchain into a fresh image, omitting the
# dependencies that we needed to actually build the toolchain.

FROM ubuntu:bionic

WORKDIR /workdir

# Step 2a. Import the cross toolchain.

COPY --from=crosstool /opt/x-tools /opt/x-tools

# Step 2b. Install dependencies needed by any CI job. Not all of these are
# available in the Ubuntu repositories.

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
    openssh-client \
    pkg-config \
    python3 \
    python3-pip \
    python3-setuptools \
    rsync \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://github.com/benesch/autouseradd/releases/download/1.1.0/autouseradd-1.1.0-amd64.tar.gz \
    | tar xz -C /usr --strip-components 1 \
    && curl -fsSL https://shellcheck.storage.googleapis.com/shellcheck-v0.7.0.linux.x86_64.tar.xz > shellcheck.tar.xz \
    && echo '39c501aaca6aae3f3c7fc125b3c3af779ddbe4e67e4ebdc44c2ae5cba76c847f  shellcheck.tar.xz' | sha256sum --check \
    && tar -xJf shellcheck.tar.xz -C /usr/local/bin --strip-components 1 shellcheck-v0.7.0/shellcheck \
    && rm shellcheck.tar.xz \
    && curl -fsSL "https://github.com/docker/compose/releases/download/1.25.4/docker-compose-Linux-x86_64" > /usr/local/bin/docker-compose \
    && chmod +x /usr/local/bin/docker-compose

ENTRYPOINT ["autouseradd", "--user", "materialize"]

# Step 2c. Install Rust. This is done separately from 2b because we bump the
# Rust version frequently, and we want to reuse the cache up through 2b when
# possible.

COPY rust.asc .

RUN gpg --import rust.asc \
    && rm rust.asc \
    && echo "trusted-key 85AB96E6FA1BE5FE" >> ~/.gnupg/gpg.conf

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
    && curl -fsSL https://static.rust-lang.org/dist$RUST_DATE/rustc-$RUST_VERSION-src.tar.gz > rust-src.tar.gz \
    && curl -fsSL https://static.rust-lang.org/dist$RUST_DATE/rustc-$RUST_VERSION-src.tar.gz.asc > rust-src.asc \
    && gpg --verify rust-src.asc rust-src.tar.gz \
    && mkdir -p /usr/local/lib/rustlib/src/rust \
    && tar -xzf rust-src.tar.gz -C /usr/local/lib/rustlib/src/rust --strip-components=1 \
    && rm -f rust-src.asc rust-src.tar.gz \
    && case $RUST_COMPONENTS in *miri*) \
        PATH=$PATH:/root/.cargo/bin cargo miri setup \
        && mv /root/.cargo/bin/xargo /usr/local/bin/xargo \
        && mv /root/.cargo/bin/xargo-check /usr/local/bin/xargo-check \
        ;; \
    esac \
    && rm -rf rust \
    && cargo install --root /usr/local cargo-deb cargo-deny

# Step 2d. Set LC_CTYPE to allow UTF-8, for Python 3.6. This will become
# unnecessary when we upgrade to Python 3.7+.
# See: https://www.python.org/dev/peps/pep-0538/

ENV LC_CTYPE=C.UTF-8

# Step 2e. Install Python dependencies. These are so quick to install and change
# frequently enough that it makes sense to install them last.

COPY requirements.txt requirements-dev.txt requirements-ci.txt /workdir/

RUN pip3 install \
    -r /workdir/requirements.txt \
    -r /workdir/requirements-dev.txt \
    -r /workdir/requirements-ci.txt \
    && rm /workdir/requirements*.txt

# Step 2f. Install docs site dependencies. These are towards the end for the
# same reason as the Python dependencies.

RUN curl -fsSL https://github.com/wjdp/htmltest/releases/download/v0.12.1/htmltest_0.12.1_linux_amd64.tar.gz > htmltest.tar.gz \
    && echo '04d4be5097b98cd28de469f8856b3fbe82669f57b482a4cf3092a55e9e8e9e0d  htmltest.tar.gz' | sha256sum --check \
    && tar -xzf htmltest.tar.gz -C /usr/local/bin htmltest \
    && rm htmltest.tar.gz \
    && curl -fsSL https://github.com/gohugoio/hugo/releases/download/v0.68.3/hugo_extended_0.68.3_Linux-64bit.tar.gz > hugo.tar.gz \
    && echo 'd93d0deac782a4dd8afc2bbc5d96b30590ce47e8afb9810bbe7551eb3acf9189  hugo.tar.gz' | sha256sum --check \
    && tar -xzf hugo.tar.gz -C /usr/local/bin hugo \
    && rm hugo.tar.gz

# Step 2g. Hardcode some known SSH hosts, or else SSH will ask whether the host
# is trustworthy on the first connection.

COPY ssh_known_hosts /etc/ssh/ssh_known_hosts

# Step 2h. Set the necessary environment variables to point Cargo and C/C++
# build systems at our cross-compiling toolchain.

ENV AR=x86_64-unknown-linux-gnu-ar
ENV LD=x86_64-unknown-linux-gnu-ld
ENV RANLIB=x86_64-unknown-linux-gnu-ranlib
ENV CPP=x86_64-unknown-linux-gnu-cpp
ENV CC=x86_64-unknown-linux-gnu-cc
ENV CXX=x86_64-unknown-linux-gnu-c++
ENV TARGET_CC=$CC
ENV TARGET_CXX=$CXX
ENV PATH=/opt/x-tools/x86_64-unknown-linux-gnu/bin:$PATH
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-unknown-linux-gnu-cc
# https://github.com/rust-lang/cargo/issues/7529
ENV RUSTDOCFLAGS="-C linker=x86_64-unknown-linux-gnu-cc"

# Step 2i. Set a environment variable that tools can check to see if they're in
# the builder or not.

ENV MZ_DEV_CI_BUILDER=1

# Step 2j. Make the image as small as possible.
RUN find /workdir /root -mindepth 1 -maxdepth 1 -exec rm -rf {} +
