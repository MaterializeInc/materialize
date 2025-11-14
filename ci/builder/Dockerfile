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
FROM ubuntu:plucky-20251001 AS ci-builder-min

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
    eatmydata \
    gdb \
    git \
    gnupg2 \
    libxml2 \
    pigz \
    python3 \
    python3.13-venv \
    zstd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /usr/share/doc/* /usr/share/man/* /usr/share/info/* /usr/share/locale/* /var/cache/* /var/log/*

RUN curl -fsSL https://github.com/trufflesecurity/trufflehog/releases/download/v3.89.1/trufflehog_3.89.1_linux_$ARCH_GO.tar.gz > trufflehog.tar.gz \
    && if [ $ARCH_GO = amd64 ]; then echo 'c187e25dd1a68ba24a47cc15f334625cb5a7b77f4c75837398950accfc752d59 trufflehog.tar.gz' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo '5c3786dec219d17c1f3a5f2f97ef8fa16b029ce7df19ca2389e667eca492f78d trufflehog.tar.gz' | sha256sum --check; fi \
    && tar -xzf trufflehog.tar.gz -C /usr/local/bin trufflehog

# Install Python dependencies. These are necessary to run some of our base tooling.
COPY requirements.txt /workdir/
RUN curl -LsSf https://astral.sh/uv/0.4.25/install.sh | UV_INSTALL_DIR=/usr/local UV_UNMANAGED_INSTALL=1 sh \
    && uv pip install --system --break-system-packages -r /workdir/requirements.txt && rm /workdir/requirements*.txt

# Install extra tools not available in apt repositories.

COPY rust.asc .
RUN gpg --import rust.asc \
    && rm rust.asc \
    && echo "trusted-key 85AB96E6FA1BE5FE" >> ~/.gnupg/gpg.conf

ARG RUST_DATE
ARG RUST_VERSION

RUN \
    # 1. autouseradd
    #
    # Ensures that the UID used when running the container has a proper entry in
    # `/etc/passwd`, and writable home directory.
    curl -fsSL https://github.com/benesch/autouseradd/releases/download/1.3.0/autouseradd-1.3.0-$ARCH_GO.tar.gz \
    | tar xz -C / --strip-components 1 \
    # 2. Docker
    #
    # If you upgrade Docker (Compose) version here, also update it in misc/python/cli/mzcompose.py.
    && mkdir -p /usr/local/lib/docker/cli-plugins \
    && curl -fsSL https://github.com/docker/compose/releases/download/v2.36.0/docker-compose-linux-$ARCH_GCC > /usr/local/lib/docker/cli-plugins/docker-compose \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose \
    && curl -fsSL https://github.com/christian-korneck/docker-pushrm/releases/download/v1.9.0/docker-pushrm_linux_$ARCH_GO > /usr/local/lib/docker/cli-plugins/docker-pushrm \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-pushrm \
    && curl -fsSL https://github.com/docker/buildx/releases/download/v0.25.0/buildx-v0.25.0.linux-$ARCH_GO > /usr/local/lib/docker/cli-plugins/docker-buildx \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx \
    # 3. Cargo
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

# Remove Ubuntu user causing UID collisions.
# https://bugs.launchpad.net/cloud-images/+bug/2005129
RUN userdel -r ubuntu

ENTRYPOINT ["autouseradd", "--user", "materialize"]

# Stage 2: Build a full CI Builder image that can be used for any CI job.
FROM ci-builder-min as ci-builder-full

ARG ARCH_GCC
ARG ARCH_GO

WORKDIR /workdir

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
    libclang-common-18-dev \
    libclang-dev \
    libclang-rt-18-dev \
    libpq-dev \
    lld \
    llvm \
    make \
    npm \
    openssh-client \
    pkg-config \
    postgresql-client \
    python3 \
    python3-dev \
    python3-setuptools \
    rsync \
    ruby \
    unzip \
    xz-utils \
    yamllint \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /usr/share/doc/* /usr/share/man/* /usr/share/info/* /usr/share/locale/* /var/cache/* /var/log/*

# Install Node.js LTS, for our Python typechecker. This is up here because we don't
# expect it to change often.

COPY nodesource.asc .

RUN gpg --dearmor < nodesource.asc > /etc/apt/keyrings/nodesource.gpg \
    && rm nodesource.asc \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_18.x nodistro main" > /etc/apt/sources.list.d/nodesource.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

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
ARG RUST_CPU_TARGET
ARG RUST_TARGET_FEATURES

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
    && cargo install --root /usr/local --version "=0.7.1" --locked cargo-about \
    && cargo install --root /usr/local --version "=3.2.0" --locked cargo-deb \
    && cargo install --root /usr/local --version "=0.18.3" --locked cargo-deny \
    && cargo install --root /usr/local --version "=0.1.0" --locked cargo-deplint \
    && cargo install --root /usr/local --version "=0.9.36" --locked cargo-hakari \
    && cargo install --root /usr/local --version "=0.9.100" --locked cargo-nextest \
    && cargo install --root /usr/local --version "=0.6.17" --locked cargo-llvm-cov \
    && cargo install --root /usr/local --version "=0.1.57" --locked --features=vendored-openssl cargo-udeps \
    && cargo install --root /usr/local --version "=0.10.0" --locked --no-default-features --features=s3,openssl/vendored sccache \
    && cargo install --root /usr/local --version "=0.3.6" --locked cargo-binutils \
    && cargo install --root /usr/local --version "=0.13.1" --locked wasm-pack

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
    curl -fsSL https://github.com/wjdp/htmltest/releases/download/v0.17.0/htmltest_0.17.0_linux_amd64.tar.gz > htmltest.tar.gz \
    && echo '775c597ee74899d6002cd2d93076f897f4ba68686bceabe2e5d72e84c57bc0fb htmltest.tar.gz' | sha256sum --check \
    && tar -xzf htmltest.tar.gz -C /usr/local/bin htmltest \
    && rm htmltest.tar.gz \
    && curl -fsSL https://github.com/gohugoio/hugo/releases/download/v0.152.2/hugo_extended_withdeploy_0.152.2_Linux-64bit.tar.gz > hugo.tar.gz \
    && echo '08200e4d82f086c94c301e8ba08102f9e23f38774ad949bb3251cac76283684a hugo.tar.gz' | sha256sum --check \
    && tar -xzf hugo.tar.gz -C /usr/local/bin hugo \
    && rm hugo.tar.gz; \
    fi

# Install KinD, kubectl, helm & helm-docs

RUN curl -fsSL https://kind.sigs.k8s.io/dl/v0.29.0/kind-linux-$ARCH_GO > /usr/local/bin/kind \
    && chmod +x /usr/local/bin/kind \
    && if [ $ARCH_GO = amd64 ]; then echo 'c72eda46430f065fb45c5f70e7c957cc9209402ef309294821978677c8fb3284 /usr/local/bin/kind' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo '03d45095dbd9cc1689f179a3e5e5da24b77c2d1b257d7645abf1b4174bebcf2a /usr/local/bin/kind' | sha256sum --check; fi

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

RUN curl -fsSL https://github.com/terraform-docs/terraform-docs/releases/download/v0.20.0/terraform-docs-v0.20.0-linux-$ARCH_GO.tar.gz > terraform-docs.tar.gz \
    && if [ $ARCH_GO = amd64 ]; then echo '34ae01772412bb11474e6718ea62113e38ff5964ee570a98c69fafe3a6dff286 terraform-docs.tar.gz' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo '371b4ed983781d1efdd8f7de06264baac41b1d80927f7fd718c405a303d863a0 terraform-docs.tar.gz' | sha256sum --check; fi \
    && tar -xzf terraform-docs.tar.gz -C /usr/local/bin terraform-docs \
    && rm terraform-docs.tar.gz

RUN curl -fsSL https://github.com/mikefarah/yq/releases/download/v4.45.1/yq_linux_$ARCH_GO > yq \
    && if [ $ARCH_GO = amd64 ]; then echo '654d2943ca1d3be2024089eb4f270f4070f491a0610481d128509b2834870049 yq' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo 'ceea73d4c86f2e5c91926ee0639157121f5360da42beeb8357783d79c2cc6a1d yq' | sha256sum --check; fi \
    && chmod +x yq \
    && mv yq /usr/local/bin

RUN curl -fsSL https://amazon-inspector-sbomgen.s3.amazonaws.com/1.8.1/linux/$ARCH_GO/inspector-sbomgen.zip > inspector-sbomgen.zip \
    && if [ $ARCH_GO = amd64 ]; then echo '9ff7958e298d2b228b0c7617f0a9a873254587fc26aee9826c37273650b389e9 inspector-sbomgen.zip' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo '6737584fd2c7d24b56777d02846d1737f47d0121344baea217a3e5368fd98fcc inspector-sbomgen.zip' | sha256sum --check; fi \
    && unzip inspector-sbomgen.zip \
    && mv inspector-sbomgen-1.8.1/linux/$ARCH_GO/inspector-sbomgen /usr/local/bin \
    && chmod +x /usr/local/bin/inspector-sbomgen

# Hardcode some known SSH hosts, or else SSH will ask whether the host is
# trustworthy on the first connection.
COPY ssh_known_hosts /etc/ssh/ssh_known_hosts

ENV LDFLAGS="-fuse-ld=lld -static-libstdc++"
ENV RUSTFLAGS="-Clink-arg=-Wl,--compress-debug-sections=zlib -Clink-arg=-Wl,-O3 -Clink-arg=-fuse-ld=lld -Csymbol-mangling-version=v0 -Ctarget-cpu=$RUST_CPU_TARGET -Ctarget-feature=$RUST_TARGET_FEATURES --cfg=tokio_unstable"
ENV PATH=/opt/google-cloud-sdk/bin:$PATH
ENV CARGO_TARGET_DIR=/mnt/build
ENV CARGO_INCREMENTAL=1
ENV HELM_PLUGINS=/usr/local/share/helm/plugins

# Set up for a persistent volume to hold Cargo metadata, so that crate metadata
# does not need to be refetched on every compilation.
ENV CARGO_HOME=/cargo
RUN mkdir /cargo && chmod 777 /cargo
VOLUME /cargo
