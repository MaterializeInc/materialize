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

FROM ubuntu:lunar-20231128 as crosstool
ARG ARCH_GCC
ARG ARCH_GO

WORKDIR /scratch

# The environment variables are necessary to convince `tzdata`'s install scripts
# not to hang.
# See: https://serverfault.com/questions/949991/how-to-install-tzdata-on-a-ubuntu-docker-image
RUN apt-get update --fix-missing && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    bison \
    bzip2 \
    ca-certificates \
    curl \
    file \
    flex \
    gawk \
    g++ \
    gcc \
    gnupg2 \
    help2man \
    libc-dev \
    libncurses-dev \
    libtool-bin \
    make \
    patch \
    texinfo \
    unzip \
    xz-utils

COPY crosstool.asc ./

RUN gpg --import crosstool.asc \
    && rm crosstool.asc \
    && echo "trusted-key 09f6dd5f1f30ef2e" >> ~/.gnupg/gpg.conf \
    && mkdir crosstool \
    # TODO(teske): Remove the CT_ZLIB_VERSION workaround below when we upgrade crosstools. \
    && curl -fsSL http://crosstool-ng.org/download/crosstool-ng/crosstool-ng-1.25.0.tar.xz > crosstool.tar.xz \
    && curl -fsSL http://crosstool-ng.org/download/crosstool-ng/crosstool-ng-1.25.0.tar.xz.sig > crosstool.sig \
    && gpg --verify crosstool.sig crosstool.tar.xz \
    && tar -xf crosstool.tar.xz -C crosstool --strip-components=1 \
    && rm crosstool.sig crosstool.tar.xz \
    && (cd crosstool && ./configure && make install) \
    && rm -rf crosstool

COPY crosstool-$ARCH_GCC.defconfig ./

RUN DEFCONFIG=crosstool-$ARCH_GCC.defconfig ct-ng defconfig \
    && rm crosstool-$ARCH_GCC.defconfig \
    # https://github.com/crosstool-ng/crosstool-ng/issues/1832 \
    && sed -i 's/CT_ZLIB_VERSION="1.2.12"/CT_ZLIB_VERSION="1.3"/g' .config \
    && ct-ng build

# Import the cross-compiling toolchain into a fresh image, omitting the
# dependencies that we needed to actually build the toolchain.

FROM ubuntu:lunar-20231128
ARG ARCH_GCC
ARG ARCH_GO

WORKDIR /workdir

# Import the cross toolchain.

COPY --from=crosstool /opt/x-tools /opt/x-tools

# Install dependencies needed by any CI job. Not all of these are available in
# the Ubuntu repositories.
RUN apt-get update --fix-missing && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    bsdmainutils \
    ca-certificates \
    cmake \
    curl \
    docker.io \
    gcc \
    g++ \
    gdb \
    git \
    gnupg2 \
    jq \
    lcov \
    libc-dbg \
    libclang-dev \
    libpq-dev \
    lld \
    llvm \
    make \
    openssh-client \
    pkg-config \
    postgresql-client \
    python3 \
    python3-dev \
    python3-pip \
    python3-setuptools \
    rsync \
    ruby \
    unzip \
    xz-utils \
    yamllint \
    zstd \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js LTS, for our Python typechecker. This is up here because we don't
# expect it to change often.

COPY nodesource.asc .

RUN gpg --dearmor < nodesource.asc > /etc/apt/keyrings/nodesource.gpg \
    && rm nodesource.asc \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_18.x nodistro main" > /etc/apt/sources.list.d/nodesource.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends nodejs

RUN curl -fsSL https://github.com/benesch/autouseradd/releases/download/1.3.0/autouseradd-1.3.0-$ARCH_GO.tar.gz \
    | tar xz -C / --strip-components 1 \
    && curl -fsSL https://github.com/koalaman/shellcheck/releases/download/v0.8.0/shellcheck-v0.8.0.linux.$ARCH_GCC.tar.xz > shellcheck.tar.xz \
    && tar -xJf shellcheck.tar.xz -C /usr/local/bin --strip-components 1 shellcheck-v0.8.0/shellcheck \
    && rm shellcheck.tar.xz \
    && curl -fsSL https://github.com/bufbuild/buf/releases/download/v1.18.0/buf-Linux-$ARCH_GCC.tar.gz > buf.tar.gz \
    && tar -xf buf.tar.gz -C /usr/local/bin --strip-components 2 buf/bin \
    && rm buf.tar.gz \
    && chmod +x /usr/local/bin/buf /usr/local/bin/protoc-gen-buf-breaking /usr/local/bin/protoc-gen-buf-lint \
    && curl -fsSL https://github.com/boz/kail/releases/download/v0.16.1/kail_v0.16.1_linux_$ARCH_GO.tar.gz > kail.tar.gz \
    && tar -xf kail.tar.gz -C /usr/local/bin kail \
    && rm kail.tar.gz \
    && chmod +x /usr/local/bin/kail \
    && mkdir -p /usr/local/lib/docker/cli-plugins \
    # If you upgrade Docker (Compose) version here, also update it in misc/python/cli/mzcompose.py \
    && curl -fsSL https://github.com/docker/compose/releases/download/v2.15.1/docker-compose-linux-$ARCH_GCC > /usr/local/lib/docker/cli-plugins/docker-compose \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose \
    && curl -fsSL https://github.com/christian-korneck/docker-pushrm/releases/download/v1.9.0/docker-pushrm_linux_$ARCH_GO > /usr/local/lib/docker/cli-plugins/docker-pushrm \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-pushrm \
    && curl -fsSL https://github.com/parca-dev/parca-debuginfo/releases/download/v0.9.0/parca-debuginfo_0.9.0_Linux_$(echo "$ARCH_GCC" | sed "s/aarch64/arm64/").tar.gz \
    | tar xz -C /usr/local/bin parca-debuginfo

ENTRYPOINT ["autouseradd", "--user", "materialize"]

# Install Rust. This is done separately from above because we bump the Rust
# version frequently, and we want to reuse the cache up through above when
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
        ;; \
    esac \
    && curl -fsSL https://static.rust-lang.org/dist$RUST_DATE/rust-std-$RUST_VERSION-wasm32-unknown-unknown.tar.gz > rust.tar.gz \
    && curl -fsSL https://static.rust-lang.org/dist$RUST_DATE/rust-std-$RUST_VERSION-wasm32-unknown-unknown.tar.gz.asc > rust.asc \
    && gpg --verify rust.asc rust.tar.gz \
    && tar -xzf rust.tar.gz -C /usr/local/lib/rustlib/ --strip-components=4 \
    && rm -rf rust.asc rust.tar.gz rust \
    && cargo install --root /usr/local --version "=0.8.0" --locked cargo-vet \
    && cargo install --root /usr/local --version "=0.6.0" --locked cargo-about \
    && cargo install --root /usr/local --version "=2.0.2" --locked cargo-deb \
    && cargo install --root /usr/local --version "=0.12.2" --locked cargo-deny \
    && cargo install --root /usr/local --version "=0.1.0" --locked cargo-deplint \
    && cargo install --root /usr/local --version ="0.9.28" --locked cargo-hakari \
    && cargo install --root /usr/local --version "=0.9.63" --locked cargo-nextest \
    && cargo install --root /usr/local --version "=0.5.37" --locked cargo-llvm-cov \
    && cargo install --root /usr/local --version "=0.1.43" --features=vendored-openssl cargo-udeps \
    && cargo install --root /usr/local --version "=0.2.15" --no-default-features --features=s3,openssl/vendored sccache \
    && cargo install --root /usr/local --version "=0.3.6" cargo-binutils \
    && cargo install --root /usr/local --version "=0.12.1" wasm-pack

# Link the system lld into the cross-compiling sysroot.

RUN ln -s /usr/bin/lld /opt/x-tools/$ARCH_GCC-unknown-linux-gnu/bin/$ARCH_GCC-unknown-linux-gnu-ld.lld \
    && ln -s /usr/bin/lld /opt/x-tools/$ARCH_GCC-unknown-linux-gnu/bin/$ARCH_GCC-unknown-linux-gnu-lld

# Install the locked version of our typechecker -- it'll be invoked via `npx`,
# so using the same command here should ensure it's installed and cached in the
# right place
COPY pyright-version.sh /workdir/
RUN npx pyright@$(sh /workdir/pyright-version.sh) --help

# Install Python dependencies. These are so quick to install and change
# frequently enough that it makes sense to install them last.

COPY requirements.txt /workdir/

RUN pip3 install --break-system-packages -r /workdir/requirements.txt && rm /workdir/requirements*.txt

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

# Install KinD and kubectl

RUN curl -fsSL https://kind.sigs.k8s.io/dl/v0.14.0/kind-linux-$ARCH_GO > /usr/local/bin/kind \
     && chmod +x /usr/local/bin/kind

RUN if [ $ARCH_GO = amd64 ]; then echo 'af5e8331f2165feab52ec2ae07c427c7b66f4ad044d09f253004a20252524c8b /usr/local/bin/kind' | sha256sum --check; fi
RUN if [ $ARCH_GO = arm64 ]; then echo '95c9601f21fdb2c286442339d5e370149b4fe2fc7c49f615647e4e27bdfb17e2 /usr/local/bin/kind' | sha256sum --check; fi

RUN curl -fsSL https://dl.k8s.io/release/v1.24.3/bin/linux/$ARCH_GO/kubectl > /usr/local/bin/kubectl \
     && chmod +x /usr/local/bin/kubectl

RUN if [ $ARCH_GO = amd64 ]; then echo '8a45348bdaf81d46caf1706c8bf95b3f431150554f47d444ffde89e8cdd712c1 /usr/local/bin/kubectl' | sha256sum --check; fi
RUN if [ $ARCH_GO = arm64 ]; then echo 'bdad4d3063ddb7bfa5ecf17fb8b029d5d81d7d4ea1650e4369aafa13ed97149a /usr/local/bin/kubectl' | sha256sum --check; fi

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
ENV RUSTFLAGS="-Clink-arg=-Wl,--compress-debug-sections=zlib -Clink-arg=-fuse-ld=lld -L/opt/x-tools/$ARCH_GCC-unknown-linux-gnu/$ARCH_GCC-unknown-linux-gnu/sysroot/lib/ -Csymbol-mangling-version=v0 --cfg=tokio_unstable"
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

# Ensure any Rust binaries that crash print a backtrace.
ENV RUST_BACKTRACE=1

# Make the image as small as possible.
RUN find /workdir /root -mindepth 1 -maxdepth 1 -exec rm -rf {} +

# remove Ubuntu user causing UID collisions
# https://bugs.launchpad.net/cloud-images/+bug/2005129
RUN userdel -r ubuntu

# Ensure that all python output is unbuffered, otherwise it is not
# logged properly in Buildkite
ENV PYTHONUNBUFFERED=1
