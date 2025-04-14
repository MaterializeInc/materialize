# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Stage 1: Build a minimum CI Builder image that we can use for the initial
# steps like `mkpipeline` and `Build`, as well as any tests that are self
# contained and use other Docker images.
FROM ubuntu:noble-20250404 AS ci-builder-min

WORKDIR /workdir

ARG ARCH_GCC
ARG ARCH_GO

# Environment variables that should be set for the entire build container.

# Ensure any Rust binaries that crash print a backtrace.
ENV RUST_BACKTRACE=1
# Ensure that all python output is unbuffered, otherwise it is not
# logged properly in Buildkite.
ENV PYTHONUNBUFFERED=1
# Set a environment variable that tools can check to see if they're in the
# builder or not.
ENV MZ_DEV_CI_BUILDER=1

# Faster uncompression
ARG XZ_OPT=-T0

# Absolute minimum set of dependencies needed for a CI job.
#
# Please take care with what gets added here. The goal of this initial layer is to be as small as
# possible since it's used for the `mkpipeline` and `Build` CI jobs, which block __all other__
# jobs.
RUN apt-get update --fix-missing && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    docker.io \
    gdb \
    git \
    gnupg2 \
    libxml2 \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies. These are necessary to run some of our base tooling.
COPY requirements.txt /workdir/
RUN curl -LsSf https://astral.sh/uv/0.4.25/install.sh | UV_INSTALL_DIR=/usr/local UV_UNMANAGED_INSTALL=1 sh \
    && uv pip install --system --break-system-packages -r /workdir/requirements.txt && rm /workdir/requirements*.txt

# Install extra tools not available in apt repositories.

COPY rust.asc .
RUN gpg --import rust.asc \
    && rm rust.asc \
    && echo "trusted-key 85AB96E6FA1BE5FE" >> ~/.gnupg/gpg.conf

ARG BAZEL_VERSION
ARG RUST_DATE
ARG RUST_VERSION

RUN \
    # 1. autouseradd
    #
    # Ensures that the UID used when running the container has a proper entry in
    # `/etc/passwd`, and writable home directory.
    curl -fsSL https://github.com/benesch/autouseradd/releases/download/1.3.0/autouseradd-1.3.0-$ARCH_GO.tar.gz \
    | tar xz -C / --strip-components 1 \
    # 2. Bazel
    #
    # We primarily build Materialize via Bazel in CI, and Bazel pulls in its own dependencies.
    && arch_bazel=$(echo "$ARCH_GCC" | sed -e "s/aarch64/arm64/" -e "s/amd64/x86_64/") bazel_version=$(echo "$BAZEL_VERSION") \
    && curl -fsSL -o /usr/local/bin/bazel https://github.com/bazelbuild/bazel/releases/download/$bazel_version/bazel-$bazel_version-linux-$arch_bazel \
    && if [ "$arch_bazel" = arm64 ]; then echo 'fac4b954e0501c2be8b9653a550b443eb85284e568d08b102977e2bf587b09d7 /usr/local/bin/bazel' | sha256sum --check; fi \
    && if [ "$arch_bazel" = x86_64 ]; then echo '48ea0ff9d397a48add6369c261c5a4431fe6d5d5348cfb81411782fb80c388d3 /usr/local/bin/bazel' | sha256sum --check; fi \
    && chmod +x /usr/local/bin/bazel \
    # 3. Docker
    #
    # If you upgrade Docker (Compose) version here, also update it in misc/python/cli/mzcompose.py.
    && mkdir -p /usr/local/lib/docker/cli-plugins \
    && curl -fsSL https://github.com/docker/compose/releases/download/v2.15.1/docker-compose-linux-$ARCH_GCC > /usr/local/lib/docker/cli-plugins/docker-compose \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose \
    && curl -fsSL https://github.com/christian-korneck/docker-pushrm/releases/download/v1.9.0/docker-pushrm_linux_$ARCH_GO > /usr/local/lib/docker/cli-plugins/docker-pushrm \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-pushrm \
    # 4. Cargo
    #
    # Some parts of our stack use 'cargo' to read metadata, so we install just that. Importantly we
    # do not install 'rustc' or any of the other tools, this keeps the Docker image small.
    && mkdir rust \
    && curl -fsSL https://static.rust-lang.org/dist$RUST_DATE/rust-$RUST_VERSION-$ARCH_GCC-unknown-linux-gnu.tar.gz > rust.tar.gz \
    && curl -fsSL https://static.rust-lang.org/dist$RUST_DATE/rust-$RUST_VERSION-$ARCH_GCC-unknown-linux-gnu.tar.gz.asc > rust.asc \
    && gpg --verify rust.asc rust.tar.gz \
    && tar -xzf rust.tar.gz -C rust --strip-components=1 \
    && rust/install.sh --components=cargo \
    && rm -rf rust.asc rust.tar.gz rust

# Make the image as small as possible.
RUN find /workdir /root -mindepth 1 -maxdepth 1 -exec rm -rf {} +

# Remove Ubuntu user causing UID collisions.
# https://bugs.launchpad.net/cloud-images/+bug/2005129
RUN userdel -r ubuntu

ENTRYPOINT ["autouseradd", "--user", "materialize"]

# Stage 2. Build a cross-compiling toolchain that targets the oldest version of
# Linux that we support.
#
# TODO(parkmycar): This shouldn't be necessary anymore with Bazel.
FROM ubuntu:noble-20250404 as crosstool

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
    libstdc++6 \
    libstdc++-13-dev \
    libtool-bin \
    llvm \
    make \
    patch \
    texinfo \
    unzip \
    xz-utils

COPY crosstool.asc ./

# Faster uncompression
ARG XZ_OPT=-T0

RUN gpg --import crosstool.asc \
    && rm crosstool.asc \
    && echo "trusted-key 09f6dd5f1f30ef2e" >> ~/.gnupg/gpg.conf \
    && mkdir crosstool \
    && curl -fsSL https://github.com/crosstool-ng/crosstool-ng/releases/download/crosstool-ng-1.26.0/crosstool-ng-1.26.0.tar.xz > crosstool.tar.xz \
    && curl -fsSL https://github.com/crosstool-ng/crosstool-ng/releases/download/crosstool-ng-1.26.0/crosstool-ng-1.26.0.tar.xz.sig > crosstool.sig \
    && gpg --verify crosstool.sig crosstool.tar.xz \
    && tar -xf crosstool.tar.xz -C crosstool --strip-components=1 \
    && rm crosstool.sig crosstool.tar.xz \
    && (cd crosstool && ./configure && make install) \
    && rm -rf crosstool

COPY crosstool-$ARCH_GCC.defconfig ./

RUN DEFCONFIG=crosstool-$ARCH_GCC.defconfig ct-ng defconfig \
    && rm crosstool-$ARCH_GCC.defconfig \
    && ct-ng build

# Stage 3: Build a full CI Builder image that imports the cross-compiling
# toolchain and can be used for any CI job.
FROM ci-builder-min as ci-builder-full

ARG ARCH_GCC
ARG ARCH_GO

WORKDIR /workdir

# Import the cross toolchain.

COPY --from=crosstool /opt/x-tools /opt/x-tools

# Install dependencies needed by any CI job. Not all of these are available in
# the Ubuntu repositories.
RUN apt-get update --fix-missing && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    binfmt-support \
    bsdmainutils \
    ca-certificates \
    clang \
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
    libclang-common-15-dev \
    libclang-dev \
    libclang-rt-15-dev \
    libpq-dev \
    lld \
    llvm \
    make \
    openssh-client \
    pkg-config \
    postgresql-client \
    python3 \
    python3-dev \
    python3-setuptools \
    qemu-system \
    qemu-user-static \
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

RUN curl -fsSL https://github.com/koalaman/shellcheck/releases/download/v0.8.0/shellcheck-v0.8.0.linux.$ARCH_GCC.tar.xz > shellcheck.tar.xz \
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
    && curl -fsSL https://github.com/parca-dev/parca-debuginfo/releases/download/v0.11.0/parca-debuginfo_0.11.0_Linux_$(echo "$ARCH_GCC" | sed "s/aarch64/arm64/").tar.gz \
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
    && cargo install --root /usr/local --version "=0.6.6" --locked cargo-about \
    && cargo install --root /usr/local --version "=2.0.2" --locked cargo-deb \
    && cargo install --root /usr/local --version "=0.14.21" --locked cargo-deny \
    && cargo install --root /usr/local --version "=0.1.0" --locked cargo-deplint \
    && cargo install --root /usr/local --version ="0.9.28" --locked cargo-hakari \
    && cargo install --root /usr/local --version "=0.9.72" --locked cargo-nextest \
    && cargo install --root /usr/local --version "=0.6.11" --locked cargo-llvm-cov \
    && cargo install --root /usr/local --version "=0.1.55" --locked --features=vendored-openssl cargo-udeps \
    && cargo install --root /usr/local --version "=0.2.15" --locked --no-default-features --features=s3,openssl/vendored sccache \
    && cargo install --root /usr/local --version "=0.3.6" --locked cargo-binutils \
    && cargo install --root /usr/local --version "=0.13.0" --locked wasm-pack

# Link the system lld into the cross-compiling sysroot.

RUN ln -s /usr/bin/lld /opt/x-tools/$ARCH_GCC-unknown-linux-gnu/bin/$ARCH_GCC-unknown-linux-gnu-ld.lld \
    && ln -s /usr/bin/lld /opt/x-tools/$ARCH_GCC-unknown-linux-gnu/bin/$ARCH_GCC-unknown-linux-gnu-lld

# Shims for sanitizers
COPY sanshim/$ARCH_GCC /sanshim

# Install the locked version of our typechecker -- it'll be invoked via `npx`,
# so using the same command here should ensure it's installed and cached in the
# right place
COPY pyright-version.sh /workdir/
RUN npx pyright@$(sh /workdir/pyright-version.sh) --help

# Install APT repo generator.

RUN curl -fsSL https://github.com/deb-s3/deb-s3/releases/download/0.11.3/deb-s3-0.11.3.gem > deb-s3.gem \
    && gem install ./deb-s3.gem \
    && rm deb-s3.gem

# Install the AWS CLI.

RUN curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$ARCH_GCC-2.17.2.zip" > awscli.zip \
    && unzip awscli.zip \
    && ./aws/install \
    && rm -rf aws awscli.zip

# Install the gcloud CLI.

RUN arch_gcloud=$(echo "$ARCH_GCC" | sed -e "s/aarch64/arm/" -e "s/amd64/x86_64/") \
    && curl -fsSL "https://storage.googleapis.com/cloud-sdk-release/google-cloud-cli-507.0.0-linux-$arch_gcloud.tar.gz" > gcloud.tar.gz \
    && tar -xzf gcloud.tar.gz -C /opt \
    && rm gcloud.tar.gz \
    && CLOUDSDK_CORE_DISABLE_PROMPTS=1 /opt/google-cloud-sdk/install.sh --path-update false \
    && /opt/google-cloud-sdk/bin/gcloud config set disable_usage_reporting false \
    && /opt/google-cloud-sdk/bin/gcloud components install gke-gcloud-auth-plugin

# Install docs site dependencies. These are towards the end for the same reason
# as the Python dependencies. These are only supported on x86_64 at the moment.
# See: https://discourse.gohugo.io/t/how-to-install-hugo-extended-on-linux-arm/28098/4

RUN if [ $ARCH_GCC = x86_64 ]; then \
    curl -fsSL https://github.com/wjdp/htmltest/releases/download/v0.12.1/htmltest_0.12.1_linux_amd64.tar.gz > htmltest.tar.gz \
    && echo '04d4be5097b98cd28de469f8856b3fbe82669f57b482a4cf3092a55e9e8e9e0d  htmltest.tar.gz' | sha256sum --check \
    && tar -xzf htmltest.tar.gz -C /usr/local/bin htmltest \
    && rm htmltest.tar.gz \
    && curl -fsSL https://github.com/gohugoio/hugo/releases/download/v0.128.0/hugo_extended_0.128.0_Linux-64bit.tar.gz > hugo.tar.gz \
    && echo 'a39cd72eff188f8596f09f3a7db9195477c4ce21072d286832f9fde15ba5d336 hugo.tar.gz' | sha256sum --check \
    && tar -xzf hugo.tar.gz -C /usr/local/bin hugo \
    && rm hugo.tar.gz; \
    fi

# Install KinD, kubectl, helm & helm-docs

RUN curl -fsSL https://kind.sigs.k8s.io/dl/v0.14.0/kind-linux-$ARCH_GO > /usr/local/bin/kind \
    && chmod +x /usr/local/bin/kind \
    && if [ $ARCH_GO = amd64 ]; then echo 'af5e8331f2165feab52ec2ae07c427c7b66f4ad044d09f253004a20252524c8b /usr/local/bin/kind' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo '95c9601f21fdb2c286442339d5e370149b4fe2fc7c49f615647e4e27bdfb17e2 /usr/local/bin/kind' | sha256sum --check; fi

RUN curl -fsSL https://dl.k8s.io/release/v1.24.3/bin/linux/$ARCH_GO/kubectl > /usr/local/bin/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && if [ $ARCH_GO = amd64 ]; then echo '8a45348bdaf81d46caf1706c8bf95b3f431150554f47d444ffde89e8cdd712c1 /usr/local/bin/kubectl' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo 'bdad4d3063ddb7bfa5ecf17fb8b029d5d81d7d4ea1650e4369aafa13ed97149a /usr/local/bin/kubectl' | sha256sum --check; fi

RUN curl -fsSL https://get.helm.sh/helm-v3.16.2-linux-$ARCH_GO.tar.gz > helm.tar.gz \
    && if [ $ARCH_GO = amd64 ]; then echo '9318379b847e333460d33d291d4c088156299a26cd93d570a7f5d0c36e50b5bb helm.tar.gz' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo '1888301aeb7d08a03b6d9f4d2b73dcd09b89c41577e80e3455c113629fc657a4 helm.tar.gz' | sha256sum --check; fi \
    && tar -xf helm.tar.gz -C /usr/local/bin --strip-components=1 linux-$ARCH_GO/helm \
    && rm helm.tar.gz

RUN arch_helm_docs=$(echo "$ARCH_GCC" | sed -e "s/aarch64/arm64/" -e "s/amd64/x86_64/") \
    && curl -fsSL https://github.com/norwoodj/helm-docs/releases/download/v1.14.2/helm-docs_1.14.2_Linux_$arch_helm_docs.tar.gz > helm-docs.tar.gz \
    && if [ "$arch_helm_docs" = "x86_64" ]; then echo 'a8cf72ada34fad93285ba2a452b38bdc5bd52cc9a571236244ec31022928d6cc helm-docs.tar.gz' | sha256sum --check; fi \
    && if [ "$arch_helm_docs" = "arm64" ]; then echo 'c3787212332386dcd122debef7848feb165aa701467ae3e3442df7638f3ac4e4 helm-docs.tar.gz' | sha256sum --check; fi \
    && tar -xf helm-docs.tar.gz -C /usr/local/bin \
    && rm helm-docs.tar.gz

RUN mkdir -p /usr/local/share/helm/plugins/unittest \
    && curl -fsSL https://github.com/helm-unittest/helm-unittest/releases/download/v0.6.3/helm-unittest-linux-$ARCH_GO-0.6.3.tgz > helm-unittest.tar.gz \
    && if [ $ARCH_GO = amd64 ]; then echo '46a23e9788cfb9f4853af9f1f8e461f5795ef6ef2917cf82df2a25bf30a65064 helm-unittest.tar.gz' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo 'cca780834c6c59dda2cbccd97002d2a5c330347b4b87dc39ad13f41a345a0401 helm-unittest.tar.gz' | sha256sum --check; fi \
    && tar -xf helm-unittest.tar.gz -C /usr/local/share/helm/plugins/unittest \
    && rm helm-unittest.tar.gz

RUN curl -fsSL https://releases.hashicorp.com/terraform/1.9.8/terraform_1.9.8_linux_$ARCH_GO.zip > terraform.zip \
    && if [ $ARCH_GO = amd64 ]; then echo '186e0145f5e5f2eb97cbd785bc78f21bae4ef15119349f6ad4fa535b83b10df8 terraform.zip' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo 'f85868798834558239f6148834884008f2722548f84034c9b0f62934b2d73ebb terraform.zip' | sha256sum --check; fi \
    && unzip terraform.zip terraform -d /usr/local/bin \
    && chmod +x /usr/local/bin/terraform \
    && rm terraform.zip

RUN curl -fsSL https://github.com/mikefarah/yq/releases/download/v4.45.1/yq_linux_$ARCH_GO > yq \
    && if [ $ARCH_GO = amd64 ]; then echo '654d2943ca1d3be2024089eb4f270f4070f491a0610481d128509b2834870049 yq' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo 'ceea73d4c86f2e5c91926ee0639157121f5360da42beeb8357783d79c2cc6a1d yq' | sha256sum --check; fi \
    && chmod +x yq \
    && mv yq /usr/local/bin

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
ENV RUSTFLAGS="-Clink-arg=-Wl,--compress-debug-sections=zlib -Clink-arg=-Wl,-O3 -Clink-arg=-fuse-ld=lld -L/opt/x-tools/$ARCH_GCC-unknown-linux-gnu/$ARCH_GCC-unknown-linux-gnu/sysroot/lib/ -Csymbol-mangling-version=v0 --cfg=tokio_unstable"
ENV TARGET_AR=$AR
ENV TARGET_CC=$CC
ENV TARGET_CXX=$CXX
ENV TARGET_CXXSTDLIB=static=stdc++
ENV TARGET_RANLIB=$RANLIB
ENV PATH=/opt/google-cloud-sdk/bin:/opt/x-tools/$ARCH_GCC-unknown-linux-gnu/bin:$PATH
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-unknown-linux-gnu-cc
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-unknown-linux-gnu-cc
ENV CARGO_TARGET_DIR=/mnt/build
ENV CARGO_INCREMENTAL=0
ENV HELM_PLUGINS=/usr/local/share/helm/plugins

# Set up for a persistent volume to hold Cargo metadata, so that crate metadata
# does not need to be refetched on every compile.
ENV CARGO_HOME=/cargo
RUN mkdir /cargo && chmod 777 /cargo
VOLUME /cargo

# Make the image as small as possible.
RUN find /workdir /root -mindepth 1 -maxdepth 1 -exec rm -rf {} +
