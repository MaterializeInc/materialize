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

FROM ubuntu:focal-20220316 AS crosstool
ARG ARCH_GCC
ARG ARCH_GO

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
    && curl -fsSL https://github.com/crosstool-ng/crosstool-ng/archive/4dc9d643f94720bb11a7b4fa0c39729a684be5bb.tar.gz > crosstool.tar.gz \
    && tar -xf crosstool.tar.gz -C crosstool --strip-components=1 \
    && rm crosstool.tar.gz \
    && (cd crosstool && ./bootstrap && ./configure && make install) \
    && rm -rf crosstool

COPY crosstool-$ARCH_GCC.defconfig ./

RUN DEFCONFIG=crosstool-$ARCH_GCC.defconfig ct-ng defconfig \
    && rm crosstool-$ARCH_GCC.defconfig \
    && ct-ng build

# Import the cross-compiling toolchain into a fresh image, omitting the
# dependencies that we needed to actually build the toolchain.

FROM ubuntu:focal-20220316
ARG ARCH_GCC
ARG ARCH_GO

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
    libpq-dev \
    lld \
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

RUN curl -fsSL https://github.com/benesch/autouseradd/releases/download/1.3.0/autouseradd-1.3.0-$ARCH_GO.tar.gz \
    | tar xz -C / --strip-components 1 \
    && curl -fsSL https://github.com/koalaman/shellcheck/releases/download/v0.8.0/shellcheck-v0.8.0.linux.$ARCH_GCC.tar.xz > shellcheck.tar.xz \
    && tar -xJf shellcheck.tar.xz -C /usr/local/bin --strip-components 1 shellcheck-v0.8.0/shellcheck \
    && rm shellcheck.tar.xz \
    && pip install docker-compose==1.29.2

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
    && curl -fsSL https://static.rust-lang.org/dist$RUST_DATE/rust-$RUST_VERSION-$ARCH_GCC-unknown-linux-gnu.tar.gz > rust.tar.gz \
    && curl -fsSL https://static.rust-lang.org/dist$RUST_DATE/rust-$RUST_VERSION-$ARCH_GCC-unknown-linux-gnu.tar.gz.asc > rust.asc \
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
    && cargo install --root /usr/local --version "=0.9.1" cargo-deny \
    && cargo install --root /usr/local --version "=0.1.10" cargo2junit \
    && cargo install --root /usr/local --version "=0.1.26" cargo-udeps  --features=vendored-openssl \
    && cargo install --root /usr/local --version "=0.2.15" --no-default-features --features=s3,openssl/vendored sccache

# Link the system lld into the cross-compiling sysroot.

RUN ln -s /usr/bin/lld /opt/x-tools/$ARCH_GCC-unknown-linux-gnu/bin/$ARCH_GCC-unknown-linux-gnu-ld.lld \
    && ln -s /usr/bin/lld /opt/x-tools/$ARCH_GCC-unknown-linux-gnu/bin/$ARCH_GCC-unknown-linux-gnu-lld

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

RUN curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$ARCH_GCC.zip" > awscli.zip \
    && unzip awscli.zip \
    && ./aws/install \
    && rm -rf aws

# Install docs site dependencies. These are towards the end for the same reason
# as the Python dependencies. These are only supported on x86_64 at the moment.
# See: https://discourse.gohugo.io/t/how-to-install-hugo-extended-on-linux-arm/28098/4

RUN if [ $ARCH_GCC = x86_64 ]; then \
    curl -fsSL https://github.com/wjdp/htmltest/releases/download/v0.12.1/htmltest_0.12.1_linux_amd64.tar.gz > htmltest.tar.gz \
    && echo '04d4be5097b98cd28de469f8856b3fbe82669f57b482a4cf3092a55e9e8e9e0d  htmltest.tar.gz' | sha256sum --check \
    && tar -xzf htmltest.tar.gz -C /usr/local/bin htmltest \
    && rm htmltest.tar.gz \
    && curl -fsSL https://github.com/gohugoio/hugo/releases/download/v0.87.0/hugo_extended_0.87.0_Linux-64bit.tar.gz > hugo.tar.gz \
    && echo 'f216af92c06809c03981296f513ce54d3d690715d3c9fdfaff802d4a6759a260  hugo.tar.gz' | sha256sum --check \
    && tar -xzf hugo.tar.gz -C /usr/local/bin hugo \
    && rm hugo.tar.gz; \
    fi

# Hardcode some known SSH hosts, or else SSH will ask whether the host is
# trustworthy on the first connection.

COPY ssh_known_hosts /etc/ssh/ssh_known_hosts

# Set the necessary environment variables to point Cargo and C/C++ build systems
# at our cross-compiling toolchain.

ENV AR=$ARCH_GCC-unknown-linux-gnu-ar
ENV LD=$ARCH_GCC-unknown-linux-gnu-lld
ENV RANLIB=$ARCH_GCC-unknown-linux-gnu-ranlib
ENV CPP=$ARCH_GCC-unknown-linux-gnu-cpp
ENV CC=$ARCH_GCC-unknown-linux-gnu-cc
ENV CXX=$ARCH_GCC-unknown-linux-gnu-c++
ENV CXXSTDLIB=static=stdc++
ENV LDFLAGS="-fuse-ld=lld -static-libstdc++"
ENV RUSTFLAGS="-Clink-arg=-Wl,--compress-debug-sections=zlib -Clink-arg=-fuse-ld=lld -L/opt/x-tools/$ARCH_GCC-unknown-linux-gnu/$ARCH_GCC-unknown-linux-gnu/sysroot/lib/"
ENV TARGET_AR=$AR
ENV TARGET_CC=$CC
ENV TARGET_CXX=$CXX
ENV TARGET_CXXSTDLIB=static=stdc++
ENV TARGET_RANLIB=$RANLIB
ENV PATH=/opt/x-tools/$ARCH_GCC-unknown-linux-gnu/bin:$PATH
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-unknown-linux-gnu-cc
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-unknown-linux-gnu-cc
ENV CARGO_TARGET_DIR=/mnt/build
ENV CARGO_INCREMENTAL=0

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
