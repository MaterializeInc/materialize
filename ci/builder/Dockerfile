# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Build a cross-compiling toolchain that targets the oldest version of Linux
# that we support.

FROM ubuntu:focal-20211006 AS crosstool

WORKDIR /scratch

# The environment variables are necessary to convince `tzdata`'s install scripts
# not to hang.
# See: https://serverfault.com/questions/949991/how-to-install-tzdata-on-a-ubuntu-docker-image
RUN apt-get update && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    bison \
    ca-certificates \
    curl \
    file \
    flex \
    gawk \
    g++ \
    gcc \
    help2man \
    libc-dev \
    libncurses-dev \
    libtool-bin \
    make \
    patch \
    texinfo \
    unzip \
    xz-utils

# TODO(benesch): switch back to an official crosstool-NG release when the
# official releases support GCC 9.0.

RUN mkdir crosstool \
    && curl -fsSL https://github.com/crosstool-ng/crosstool-ng/archive/5b0e0127e189c08e433ae25dbceead63da5ef2d7.tar.gz > crosstool.tar.gz \
    && tar -xf crosstool.tar.gz -C crosstool --strip-components=1 \
    && rm crosstool.tar.gz \
    && (cd crosstool && ./bootstrap && ./configure && make install) \
    && rm -rf crosstool

COPY crosstool.defconfig ./

# The sed command below works around
# https://github.com/crosstool-ng/crosstool-ng/issues/1609.
RUN DEFCONFIG=crosstool.defconfig ct-ng defconfig \
    && rm crosstool.defconfig \
    && sed -i 's,http://isl.gforge.inria.fr,https://mirror.sobukus.de/files/src/isl/,' .config \
    && ct-ng build

# Import the cross-compiling toolchain into a fresh image, omitting the
# dependencies that we needed to actually build the toolchain.

FROM ubuntu:focal-20211006

WORKDIR /workdir

# Import the cross toolchain.

COPY --from=crosstool /opt/x-tools /opt/x-tools

# Install dependencies needed by any CI job. Not all of these are available in
# the Ubuntu repositories.
RUN apt-get update && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
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
    make \
    openssh-client \
    pkg-config \
    python3 \
    python3-dev \
    python3-pip \
    python3-setuptools \
    rsync \
    ruby \
    unzip \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://github.com/benesch/autouseradd/releases/download/1.2.0/autouseradd-1.2.0-amd64.tar.gz \
    | tar xz -C / --strip-components 1 \
    && curl -fsSL https://github.com/koalaman/shellcheck/releases/download/v0.7.0/shellcheck-v0.7.0.linux.x86_64.tar.xz > shellcheck.tar.xz \
    && echo '39c501aaca6aae3f3c7fc125b3c3af779ddbe4e67e4ebdc44c2ae5cba76c847f  shellcheck.tar.xz' | sha256sum --check \
    && tar -xJf shellcheck.tar.xz -C /usr/local/bin --strip-components 1 shellcheck-v0.7.0/shellcheck \
    && rm shellcheck.tar.xz \
    && curl -fsSL "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-Linux-x86_64" > /usr/local/bin/docker-compose \
    && chmod +x /usr/local/bin/docker-compose

ENTRYPOINT ["autouseradd", "--user", "materialize"]

# Install Rust. This is done separately from above because we bump the Rust
# version frequently, and we want to reuse the cache up through aboe when
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
    && cargo install --root /usr/local --version "=1.33.1" cargo-deb \
    && cargo install --root /usr/local --version "=0.9.1" cargo-deny

# Link Rust's internal lld into the cross-compiling sysroot. This avoids
# bringing in an extra copy of LLVM via `apt install lld`.

RUN ln -s /usr/local/lib/rustlib/x86_64-unknown-linux-gnu/bin/rust-lld /opt/x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-ld.lld \
    && ln -s /usr/local/lib/rustlib/x86_64-unknown-linux-gnu/bin/rust-lld /opt/x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-lld

# Install the Antithesis stub instrumentation library.

COPY libvoidstar.so /usr/local/lib/

# Install Python dependencies. These are so quick to install and change
# frequently enough that it makes sense to install them last.

COPY requirements.txt requirements-dev.txt /workdir/

RUN pip3 install \
    -r /workdir/requirements.txt \
    -r /workdir/requirements-dev.txt \
    && rm /workdir/requirements*.txt

# Install APT repo generator.

RUN curl -fsSL https://github.com/deb-s3/deb-s3/releases/download/0.11.3/deb-s3-0.11.3.gem > deb-s3.gem \
    && gem install ./deb-s3.gem \
    && rm deb-s3.gem

# Install the AWS CLI.

RUN curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" > awscli.zip \
    && unzip awscli.zip \
    && ./aws/install \
    && rm -rf aws

# Install docs site dependencies. These are towards the end for the same reason
# as the Python dependencies.

RUN curl -fsSL https://github.com/wjdp/htmltest/releases/download/v0.12.1/htmltest_0.12.1_linux_amd64.tar.gz > htmltest.tar.gz \
    && echo '04d4be5097b98cd28de469f8856b3fbe82669f57b482a4cf3092a55e9e8e9e0d  htmltest.tar.gz' | sha256sum --check \
    && tar -xzf htmltest.tar.gz -C /usr/local/bin htmltest \
    && rm htmltest.tar.gz \
    && curl -fsSL https://github.com/gohugoio/hugo/releases/download/v0.89.0/hugo_extended_0.89.0_Linux-64bit.tar.gz > hugo.tar.gz \
    && echo 'bb00644cc6945ffc589645b0a9c8d4ad8580bc0fa61cdd9d204d86a5836be176  hugo.tar.gz' | sha256sum --check \
    && tar -xzf hugo.tar.gz -C /usr/local/bin hugo \
    && rm hugo.tar.gz

# Hardcode some known SSH hosts, or else SSH will ask whether the host is
# trustworthy on the first connection.

COPY ssh_known_hosts /etc/ssh/ssh_known_hosts

# Set the necessary environment variables to point Cargo and C/C++ build systems
# at our cross-compiling toolchain.

ENV AR=x86_64-unknown-linux-gnu-ar
ENV LD=x86_64-unknown-linux-gnu-lld
ENV RANLIB=x86_64-unknown-linux-gnu-ranlib
ENV CPP=x86_64-unknown-linux-gnu-cpp
ENV CC=x86_64-unknown-linux-gnu-cc
ENV CXX=x86_64-unknown-linux-gnu-c++
ENV LDFLAGS=-fuse-ld=lld
ENV TARGET_CC=$CC
ENV TARGET_CXX=$CXX
ENV PATH=/opt/x-tools/x86_64-unknown-linux-gnu/bin:$PATH
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-unknown-linux-gnu-cc
# https://github.com/rust-lang/cargo/issues/7529
ENV RUSTDOCFLAGS="-C linker=x86_64-unknown-linux-gnu-cc"

# Set a environment variable that tools can check to see if they're in the
# builder or not.

ENV MZ_DEV_CI_BUILDER=1

# Set up for a persistent volume to hold Cargo metadata, so that crate metadata
# does not need to be refetched on every compile.

ENV CARGO_HOME=/cargo
RUN mkdir /cargo && chmod 777 /cargo
VOLUME /cargo

# Make the image as small as possible.
RUN find /workdir /root -mindepth 1 -maxdepth 1 -exec rm -rf {} +
