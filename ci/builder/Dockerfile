# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM node:26-trixie-slim@sha256:e6bedef217677b276c591795e169840edd9096dbf13f29df8ef8d1856b8820bc AS node

# Node 25 dropped corepack from the distribution, but the console pins its
# package manager through `packageManager` in console/package.json and CI drives
# yarn through `corepack enable`. Reinstall it here so that the stages copying
# `/usr/local/lib/node_modules` below still find `corepack/dist/corepack.js`.
RUN npm install -g corepack

# CA bundle for apt's HTTPS transport, before ca-certificates is installed.
FROM gcr.io/distroless/cc-debian13:debug-nonroot@sha256:7de8839a160a71a27345cc21929f1403099579b907bddd2d05f6816ee385a2c6 AS ca-certs

# Stage 1: Build a minimum CI Builder image that we can use for the initial
# steps like `mkpipeline` and `Build`, as well as any tests that are self
# contained and use other Docker images.
FROM debian:13-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2 AS ci-builder-min

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
# Fetch apt sources over HTTPS only.
COPY --from=ca-certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
RUN echo 'Acquire::https::CAInfo "/etc/ssl/certs/ca-certificates.crt";' > /etc/apt/apt.conf.d/99https-ca \
    && sed -i 's|http://|https://|g' /etc/apt/sources.list.d/*

RUN apt-get update --fix-missing && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    docker-cli \
    eatmydata \
    gdb \
    git \
    gnupg2 \
    pigz \
    python3 \
    python3-venv \
    zstd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /usr/share/doc/* /usr/share/man/* /usr/share/info/* /usr/share/locale/* /var/cache/* /var/log/* \
    && sed -i 's/^UID_MIN.*/UID_MIN\t500/' /etc/login.defs

# renovate: datasource=github-release-attachments depName=trufflesecurity/trufflehog
ARG TRUFFLEHOG_AMD64=v3.99.9@12050095c8063bf4d34f9f6cbce6ba9fa8d0deea9d503d0570364feba093658e
# renovate: datasource=github-release-attachments depName=trufflesecurity/trufflehog
ARG TRUFFLEHOG_ARM64=v3.99.9@778b1913196dc9b9234e8fd366f64387efeb08c01930a1d2110dcb2ffd07e68b

RUN TRUFFLEHOG_VERSION=${TRUFFLEHOG_AMD64%@*} \
    && curl -fsSL https://github.com/trufflesecurity/trufflehog/releases/download/${TRUFFLEHOG_VERSION}/trufflehog_${TRUFFLEHOG_VERSION#v}_linux_$ARCH_GO.tar.gz > trufflehog.tar.gz \
    && if [ $ARCH_GO = amd64 ]; then echo "${TRUFFLEHOG_AMD64#*@} trufflehog.tar.gz" | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo "${TRUFFLEHOG_ARM64#*@} trufflehog.tar.gz" | sha256sum --check; fi \
    && tar -xzf trufflehog.tar.gz -C /usr/local/bin trufflehog \
    && rm trufflehog.tar.gz

# Install Python dependencies. These are necessary to run some of our base tooling.
COPY requirements.txt /workdir/
# renovate: datasource=github-releases depName=astral-sh/uv
ARG UV_VERSION=0.13.8
RUN curl -LsSf https://astral.sh/uv/$UV_VERSION/install.sh | UV_INSTALL_DIR=/usr/local/bin UV_UNMANAGED_INSTALL=1 sh \
    && uv pip install --system --break-system-packages -r /workdir/requirements.txt && rm /workdir/requirements*.txt \
    && rm -rf /root/.cache/uv

# Install extra tools not available in apt repositories.

COPY rust.asc .
RUN gpg --import rust.asc \
    && rm rust.asc \
    && echo "trusted-key 85AB96E6FA1BE5FE" >> ~/.gnupg/gpg.conf

ARG RUST_DATE
ARG RUST_VERSION

# renovate: datasource=github-releases depName=MaterializeInc/autouseradd
ARG AUTOUSERADD_VERSION=1.3.0
# renovate: datasource=github-releases depName=docker/compose
ARG DOCKER_COMPOSE_VERSION=v5.7.1
# renovate: datasource=github-releases depName=christian-korneck/docker-pushrm
ARG DOCKER_PUSHRM_VERSION=v1.9.0
# renovate: datasource=github-releases depName=docker/buildx
ARG DOCKER_BUILDX_VERSION=v0.39.0

RUN \
    # 1. autouseradd
    #
    # Ensures that the UID used when running the container has a proper entry in
    # `/etc/passwd`, and writable home directory.
    curl -fsSL https://github.com/MaterializeInc/autouseradd/releases/download/${AUTOUSERADD_VERSION}/autouseradd-${AUTOUSERADD_VERSION}-$ARCH_GO.tar.gz \
    | tar xz -C / --strip-components 1 \
    # 2. Docker
    #
    # If you upgrade Docker (Compose) version here, also update it in misc/python/cli/mzcompose.py.
    && mkdir -p /usr/local/lib/docker/cli-plugins \
    # Newer docker compose versions change the output format in services.log,
    # making it hard to use a regex in ci_annotate_errors.py to tell which test
    # an error comes from.
    && curl -fsSL https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-linux-$ARCH_GCC > /usr/local/lib/docker/cli-plugins/docker-compose \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose \
    && curl -fsSL https://github.com/christian-korneck/docker-pushrm/releases/download/${DOCKER_PUSHRM_VERSION}/docker-pushrm_linux_$ARCH_GO > /usr/local/lib/docker/cli-plugins/docker-pushrm \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-pushrm \
    && curl -fsSL https://github.com/docker/buildx/releases/download/${DOCKER_BUILDX_VERSION}/buildx-${DOCKER_BUILDX_VERSION}.linux-$ARCH_GO > /usr/local/lib/docker/cli-plugins/docker-buildx \
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

ENTRYPOINT ["autouseradd", "--user", "materialize"]

# Stage 2: Build a full CI Builder image that can be used for any CI job.
FROM ci-builder-min AS ci-builder-full

ARG ARCH_GCC
ARG ARCH_GO

WORKDIR /workdir

# Install dependencies needed by any CI job. Not all of these are available in
# the Debian repositories.
RUN apt-get update --fix-missing && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    binfmt-support \
    bsdmainutils \
    ca-certificates \
    clang \
    cmake \
    curl \
    docker-cli \
    gcc \
    g++ \
    gdb \
    git \
    gnupg2 \
    jq \
    lcov \
    libc6-dbg \
    libclang-common-18-dev \
    libclang-dev \
    libclang-rt-19-dev \
    libpq-dev \
    libxml2 \
    lld \
    llvm \
    make \
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

# The `-22` here must match the LLVM version of the rustc in rust-version (root
# Cargo.toml). Bump both together, or cross-language LTO fails to link (the
# linker cannot read newer bitcode).
COPY apt-llvm.asc .

RUN gpg --dearmor < apt-llvm.asc > /etc/apt/keyrings/apt-llvm.gpg \
    && rm apt-llvm.asc \
    && echo "deb [signed-by=/etc/apt/keyrings/apt-llvm.gpg] https://apt.llvm.org/trixie/ llvm-toolchain-trixie-22 main" > /etc/apt/sources.list.d/apt-llvm.list \
    && apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    clang-22 \
    lld-22 \
    llvm-22 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /usr/share/doc/* /usr/share/man/* /usr/share/info/* /usr/share/locale/* /var/cache/* /var/log/*

RUN printf '#!/bin/sh\nexec /usr/bin/clang-22 --ld-path=/usr/bin/ld.lld-22 "$@"\n' > /usr/local/bin/clang-lld-22 \
    && chmod +x /usr/local/bin/clang-lld-22

# Install Node.js LTS, for our Python typechecker. This is up here because we don't
# expect it to change often.
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/corepack/dist/corepack.js /usr/local/bin/corepack \
    && ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
    && corepack enable

# renovate: datasource=github-releases depName=koalaman/shellcheck
ARG SHELLCHECK_VERSION=v0.11.0
# renovate: datasource=github-releases depName=bufbuild/buf
ARG BUF_VERSION=v1.75.0
# renovate: datasource=github-releases depName=boz/kail
ARG KAIL_VERSION=v0.17.4
# renovate: datasource=github-releases depName=parca-dev/parca-debuginfo
ARG PARCA_DEBUGINFO_VERSION=v0.13.0

RUN curl -fsSL https://github.com/koalaman/shellcheck/releases/download/${SHELLCHECK_VERSION}/shellcheck-${SHELLCHECK_VERSION}.linux.$ARCH_GCC.tar.xz > shellcheck.tar.xz \
    && tar -xJf shellcheck.tar.xz -C /usr/local/bin --strip-components 1 shellcheck-${SHELLCHECK_VERSION}/shellcheck \
    && rm shellcheck.tar.xz \
    && curl -fsSL https://github.com/bufbuild/buf/releases/download/${BUF_VERSION}/buf-Linux-$ARCH_GCC.tar.gz > buf.tar.gz \
    && tar -xf buf.tar.gz -C /usr/local/bin --strip-components 2 buf/bin \
    && rm buf.tar.gz \
    && curl -fsSL https://github.com/boz/kail/releases/download/${KAIL_VERSION}/kail_${KAIL_VERSION}_linux_$ARCH_GO.tar.gz > kail.tar.gz \
    && tar -xf kail.tar.gz -C /usr/local/bin kail \
    && rm kail.tar.gz \
    && chmod +x /usr/local/bin/kail \
    && curl -fsSL https://github.com/parca-dev/parca-debuginfo/releases/download/${PARCA_DEBUGINFO_VERSION}/parca-debuginfo_${PARCA_DEBUGINFO_VERSION#v}_Linux_$ARCH_GCC > /usr/local/bin/parca-debuginfo \
    && chmod +x /usr/local/bin/buf /usr/local/bin/protoc-gen-buf-breaking /usr/local/bin/protoc-gen-buf-lint /usr/local/bin/parca-debuginfo

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
    # Trim Rust source tree: only library/ is needed (for cargo miri setup). \
    # Delete compiler/, src/, tests/, tools/, etc. to save ~500-700 MB. \
    && find /usr/local/lib/rustlib/src/rust -mindepth 1 -maxdepth 1 \
        -type d ! -name library -exec rm -rf {} + \
    && case $RUST_COMPONENTS in *miri*) \
    PATH=$PATH:/root/.cargo/bin cargo miri setup \
    ;; \
    esac \
    && curl -fsSL https://static.rust-lang.org/dist$RUST_DATE/rust-std-$RUST_VERSION-wasm32-unknown-unknown.tar.gz > rust.tar.gz \
    && curl -fsSL https://static.rust-lang.org/dist$RUST_DATE/rust-std-$RUST_VERSION-wasm32-unknown-unknown.tar.gz.asc > rust.asc \
    && gpg --verify rust.asc rust.tar.gz \
    && tar -xzf rust.tar.gz -C /usr/local/lib/rustlib/ --strip-components=4 \
    && rm -rf rust.asc rust.tar.gz rust \
    && cargo install --root /usr/local --version "=0.9.3" --locked --features=cli cargo-about \
    && cargo install --root /usr/local --version "=3.8.0" --locked cargo-deb \
    && cargo install --root /usr/local --version "=0.20.2" --locked cargo-deny \
    && cargo install --root /usr/local --version "=0.1.0" --locked cargo-deplint \
    && cargo install --root /usr/local --version "=0.9.152" --locked cargo-nextest \
    && cargo install --root /usr/local --version "=0.9.1" --locked cargo-llvm-cov \
    && cargo install --root /usr/local --version "=0.1.61" --locked --features=vendored-openssl cargo-udeps \
    && cargo install --root /usr/local --version "=0.4.0" --locked cargo-binutils \
    && cargo install --root /usr/local --version "=0.15.1" --locked wasm-pack \
    && if [ "$RUST_VERSION" = "nightly" ]; then \
           # NOTE: no --locked, unlike the installs above. cargo-fuzz 0.13.1's \
           # bundled Cargo.lock pins deps that fail on the pinned nightly \
           # (yanked futures-util/zip, plus a crate using the perma-unstable \
           # `rustc_layout_scalar_valid_range_*` attribute). Let cargo resolve \
           # compatible versions instead. \
           cargo install --root /usr/local --version "=0.13.2" cargo-fuzz; \
       fi \
    && rm -rf /cargo/registry /cargo/git

# Shims for sanitizers
COPY sanshim/$ARCH_GCC /sanshim

# Install the locked version of our typechecker -- it'll be invoked via `npx`,
# so using the same command here should ensure it's installed and cached in the
# right place
COPY pyright-version.sh /workdir/
RUN npx pyright@$(sh /workdir/pyright-version.sh) --help \
    && npm cache clean --force 2>/dev/null || true

# Install APT repo generator.

# renovate: datasource=rubygems depName=deb-s3
ARG DEB_S3_VERSION=0.11.8

RUN curl -fsSL https://github.com/deb-s3/deb-s3/releases/download/${DEB_S3_VERSION}/deb-s3-${DEB_S3_VERSION}.gem > deb-s3.gem \
    && gem install ./deb-s3.gem --no-document \
    && rm deb-s3.gem \
    && rm -rf /root/.local/share/gem/cache

# Install the AWS CLI.

# TODO(def-): AWS (Real) tests fail with InvalidClientTokenId
# RUN curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$ARCH_GCC-2.31.39.zip" > awscli.zip \
# renovate: datasource=github-tags depName=aws/aws-cli
ARG AWS_CLI_VERSION=2.17.2
RUN curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$ARCH_GCC-$AWS_CLI_VERSION.zip" > awscli.zip \
    && unzip awscli.zip \
    && ./aws/install \
    && rm -rf aws awscli.zip

# Install the gcloud CLI.

# renovate: datasource=custom.gcloud depName=gcloud
ARG GCLOUD_VERSION=548.0.0
RUN arch_gcloud=$(echo "$ARCH_GCC" | sed -e "s/aarch64/arm/" -e "s/amd64/x86_64/") \
    && curl -fsSL "https://storage.googleapis.com/cloud-sdk-release/google-cloud-cli-$GCLOUD_VERSION-linux-$arch_gcloud.tar.gz" > gcloud.tar.gz \
    && tar -xzf gcloud.tar.gz -C /opt \
    && rm gcloud.tar.gz \
    && CLOUDSDK_CORE_DISABLE_PROMPTS=1 /opt/google-cloud-sdk/install.sh --path-update false \
    && /opt/google-cloud-sdk/bin/gcloud config set disable_usage_reporting false \
    && /opt/google-cloud-sdk/bin/gcloud components install gke-gcloud-auth-plugin \
    # Clean up gcloud SDK: remove backup copies, bundled Python (system Python exists), caches \
    && rm -rf /opt/google-cloud-sdk/.install/.backup \
    && rm -rf /opt/google-cloud-sdk/platform/bundledpythonunix \
    && find /opt/google-cloud-sdk -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true \
    && rm -rf /root/.cache/pip

# Install docs site dependencies. These are towards the end for the same reason
# as the Python dependencies. These are only supported on x86_64 at the moment.
# See: https://discourse.gohugo.io/t/how-to-install-hugo-extended-on-linux-arm/28098/4
#
# On every Hugo version bump, compare doc/user/layouts/sitemap.xml with the new
# version's built-in sitemap template, which it copies, and port any changes.

# renovate: datasource=github-release-attachments depName=wjdp/htmltest
ARG HTMLTEST_AMD64=v0.17.0@775c597ee74899d6002cd2d93076f897f4ba68686bceabe2e5d72e84c57bc0fb
# renovate: datasource=github-release-attachments depName=gohugoio/hugo
ARG HUGO_AMD64=v0.152.2@08200e4d82f086c94c301e8ba08102f9e23f38774ad949bb3251cac76283684a

RUN if [ $ARCH_GCC = x86_64 ]; then \
    HTMLTEST_VERSION=${HTMLTEST_AMD64%@*} \
    && curl -fsSL https://github.com/wjdp/htmltest/releases/download/${HTMLTEST_VERSION}/htmltest_${HTMLTEST_VERSION#v}_linux_amd64.tar.gz > htmltest.tar.gz \
    && echo "${HTMLTEST_AMD64#*@} htmltest.tar.gz" | sha256sum --check \
    && tar -xzf htmltest.tar.gz -C /usr/local/bin htmltest \
    && rm htmltest.tar.gz \
    && HUGO_VERSION=${HUGO_AMD64%@*} \
    && curl -fsSL https://github.com/gohugoio/hugo/releases/download/${HUGO_VERSION}/hugo_extended_withdeploy_${HUGO_VERSION#v}_Linux-64bit.tar.gz > hugo.tar.gz \
    && echo "${HUGO_AMD64#*@} hugo.tar.gz" | sha256sum --check \
    && tar -xzf hugo.tar.gz -C /usr/local/bin hugo \
    && rm hugo.tar.gz; \
    fi

# Install KinD, kubectl, helm & helm-docs

# renovate: datasource=github-release-attachments depName=kubernetes-sigs/kind
ARG KIND_AMD64=v0.33.0@aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d
# renovate: datasource=github-release-attachments depName=kubernetes-sigs/kind
ARG KIND_ARM64=v0.33.0@20022bee6cfcd5086cb7234d218e3454e6090022f2a8f55d1fa7fcf42c3867a2

RUN KIND_VERSION=${KIND_AMD64%@*} \
    && curl -fsSL https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-$ARCH_GO > /usr/local/bin/kind \
    && chmod +x /usr/local/bin/kind \
    && if [ $ARCH_GO = amd64 ]; then echo "${KIND_AMD64#*@} /usr/local/bin/kind" | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo "${KIND_ARM64#*@} /usr/local/bin/kind" | sha256sum --check; fi

# renovate: datasource=github-releases depName=kubernetes/kubernetes
ARG KUBECTL_VERSION=v1.34.10
RUN curl -fsSL https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/$ARCH_GO/kubectl > /usr/local/bin/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && if [ $ARCH_GO = amd64 ]; then echo '95bd70842bd11a524d24acd5b68726899e3488e153e45e2b4ae846545beda050 /usr/local/bin/kubectl' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo '52d3aeefea32fdfa3671ccd636be5da463ddfd0a2fc09d7bcbaedcff4c76cad5 /usr/local/bin/kubectl' | sha256sum --check; fi

# renovate: datasource=github-releases depName=helm/helm
ARG HELM_VERSION=v4.0.0
RUN curl -fsSL https://get.helm.sh/helm-${HELM_VERSION}-linux-$ARCH_GO.tar.gz > helm.tar.gz \
    && if [ $ARCH_GO = amd64 ]; then echo 'c77e9e7c1cc96e066bd240d190d1beed9a6b08060b2043ef0862c4f865eca08f helm.tar.gz' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo '8c5c77e20cc29509d640e208a6a7d2b7e9f99bb04e5b5fbe22707b72a5235245 helm.tar.gz' | sha256sum --check; fi \
    && tar -xf helm.tar.gz -C /usr/local/bin --strip-components=1 linux-$ARCH_GO/helm \
    && rm helm.tar.gz

# renovate: datasource=github-release-attachments depName=norwoodj/helm-docs
ARG HELM_DOCS_AMD64=v1.14.2@a8cf72ada34fad93285ba2a452b38bdc5bd52cc9a571236244ec31022928d6cc
# renovate: datasource=github-release-attachments depName=norwoodj/helm-docs
ARG HELM_DOCS_ARM64=v1.14.2@c3787212332386dcd122debef7848feb165aa701467ae3e3442df7638f3ac4e4

RUN arch_helm_docs=$(echo "$ARCH_GCC" | sed -e "s/aarch64/arm64/" -e "s/amd64/x86_64/") \
    && HELM_DOCS_VERSION=${HELM_DOCS_AMD64%@*} \
    && curl -fsSL https://github.com/norwoodj/helm-docs/releases/download/${HELM_DOCS_VERSION}/helm-docs_${HELM_DOCS_VERSION#v}_Linux_$arch_helm_docs.tar.gz > helm-docs.tar.gz \
    && if [ "$arch_helm_docs" = "x86_64" ]; then echo "${HELM_DOCS_AMD64#*@} helm-docs.tar.gz" | sha256sum --check; fi \
    && if [ "$arch_helm_docs" = "arm64" ]; then echo "${HELM_DOCS_ARM64#*@} helm-docs.tar.gz" | sha256sum --check; fi \
    && tar -xf helm-docs.tar.gz -C /usr/local/bin \
    && rm helm-docs.tar.gz

# renovate: datasource=github-release-attachments depName=helm-unittest/helm-unittest
ARG HELM_UNITTEST_AMD64=v1.2.1@48e86ecc1467b009630e5954375e5d8494139f73b466df5fe8c04fa45b634939
# renovate: datasource=github-release-attachments depName=helm-unittest/helm-unittest
ARG HELM_UNITTEST_ARM64=v1.2.1@c66062713cd3cbb1a80d7098bd1d4a48db8438f56b43fb6377df65447f93f8de

RUN mkdir -p /usr/local/share/helm/plugins/unittest \
    && HELM_UNITTEST_VERSION=${HELM_UNITTEST_AMD64%@*} \
    && curl -fsSL https://github.com/helm-unittest/helm-unittest/releases/download/${HELM_UNITTEST_VERSION}/helm-unittest-linux-$ARCH_GO-${HELM_UNITTEST_VERSION#v}.tgz > helm-unittest.tar.gz \
    && if [ $ARCH_GO = amd64 ]; then echo "${HELM_UNITTEST_AMD64#*@} helm-unittest.tar.gz" | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo "${HELM_UNITTEST_ARM64#*@} helm-unittest.tar.gz" | sha256sum --check; fi \
    && tar -xf helm-unittest.tar.gz -C /usr/local/share/helm/plugins/unittest \
    && rm helm-unittest.tar.gz

# renovate: datasource=github-releases depName=hashicorp/terraform
ARG TERRAFORM_VERSION=1.13.5
RUN curl -fsSL https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_$ARCH_GO.zip > terraform.zip \
    && if [ $ARCH_GO = amd64 ]; then echo '0dbe3fcc268eb670801af6a6456799d1ae26e72e73797f6c6167e18aafd1fd9a terraform.zip' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo 'fc1ddcb403fb57e25bdbdceb1ef2b1a102650c01b4d65dd3410082d2ef8b4417 terraform.zip' | sha256sum --check; fi \
    && unzip terraform.zip terraform -d /usr/local/bin \
    && chmod +x /usr/local/bin/terraform \
    && rm terraform.zip

# renovate: datasource=github-release-attachments depName=terraform-docs/terraform-docs
ARG TERRAFORM_DOCS_AMD64=v0.20.0@34ae01772412bb11474e6718ea62113e38ff5964ee570a98c69fafe3a6dff286
# renovate: datasource=github-release-attachments depName=terraform-docs/terraform-docs
ARG TERRAFORM_DOCS_ARM64=v0.20.0@371b4ed983781d1efdd8f7de06264baac41b1d80927f7fd718c405a303d863a0

RUN TERRAFORM_DOCS_VERSION=${TERRAFORM_DOCS_AMD64%@*} \
    && curl -fsSL https://github.com/terraform-docs/terraform-docs/releases/download/${TERRAFORM_DOCS_VERSION}/terraform-docs-${TERRAFORM_DOCS_VERSION}-linux-$ARCH_GO.tar.gz > terraform-docs.tar.gz \
    && if [ $ARCH_GO = amd64 ]; then echo "${TERRAFORM_DOCS_AMD64#*@} terraform-docs.tar.gz" | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo "${TERRAFORM_DOCS_ARM64#*@} terraform-docs.tar.gz" | sha256sum --check; fi \
    && tar -xzf terraform-docs.tar.gz -C /usr/local/bin terraform-docs \
    && rm terraform-docs.tar.gz

# renovate: datasource=github-release-attachments depName=mikefarah/yq
ARG YQ_AMD64=v4.48.2@0ffc35320180d4911bc3a772934da508715e08af444cb33d4d43660065e25bcc
# renovate: datasource=github-release-attachments depName=mikefarah/yq
ARG YQ_ARM64=v4.48.2@3c21630fda217239a5b7d718d08f08e02503098230b3abd49195d315a6dcfe45

RUN YQ_VERSION=${YQ_AMD64%@*} \
    && curl -fsSL https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/yq_linux_$ARCH_GO > yq \
    && if [ $ARCH_GO = amd64 ]; then echo "${YQ_AMD64#*@} yq" | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo "${YQ_ARM64#*@} yq" | sha256sum --check; fi \
    && chmod +x yq \
    && mv yq /usr/local/bin

# renovate: datasource=custom.inspector-sbomgen depName=inspector-sbomgen
ARG INSPECTOR_SBOMGEN_VERSION=1.8.2
RUN curl -fsSL https://amazon-inspector-sbomgen.s3.amazonaws.com/$INSPECTOR_SBOMGEN_VERSION/linux/$ARCH_GO/inspector-sbomgen.zip > inspector-sbomgen.zip \
    && if [ $ARCH_GO = amd64 ]; then echo '2e4e3c754e230046349dd975feb48fa953ea5a2de190cbbc17c1c85043936b5a inspector-sbomgen.zip' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo '449a49e222a2bdffe0353435d7b04b0556b35a391c7b9714ce46d1a5382bc3e2 inspector-sbomgen.zip' | sha256sum --check; fi \
    && unzip inspector-sbomgen.zip \
    && mv inspector-sbomgen-$INSPECTOR_SBOMGEN_VERSION/linux/$ARCH_GO/inspector-sbomgen /usr/local/bin \
    && chmod +x /usr/local/bin/inspector-sbomgen \
    && rm -rf inspector-sbomgen.zip inspector-sbomgen-$INSPECTOR_SBOMGEN_VERSION

# renovate: datasource=github-release-attachments depName=zizmorcore/zizmor
ARG ZIZMOR_AMD64=v1.32.0@bb110cc765eca9cd7efb2e284a956be5974c60027b0970f16397d86208a9957b
# renovate: datasource=github-release-attachments depName=zizmorcore/zizmor
ARG ZIZMOR_ARM64=v1.32.0@cb152b207f815116d7fa1c1ee4614f61578803351395052908095e79adfd9fd0

RUN arch_zizmor=$(echo "$ARCH_GCC" | sed -e "s/amd64/x86_64/") \
    && ZIZMOR_VERSION=${ZIZMOR_AMD64%@*} \
    && curl -fsSL https://github.com/zizmorcore/zizmor/releases/download/${ZIZMOR_VERSION}/zizmor-$arch_zizmor-unknown-linux-gnu.tar.gz > zizmor.tar.gz \
    && if [ $arch_zizmor = x86_64 ]; then echo "${ZIZMOR_AMD64#*@} zizmor.tar.gz" | sha256sum --check; fi \
    && if [ $arch_zizmor = aarch64 ]; then echo "${ZIZMOR_ARM64#*@} zizmor.tar.gz" | sha256sum --check; fi \
    && tar xvf zizmor.tar.gz \
    && mv zizmor /usr/local/bin \
    && rm zizmor.tar.gz

# Install a pinned libduckdb for libduckdb-sys to link against via
# `DUCKDB_LIB_DIR`, so builds don't download it. The testdrive image copies it
# from here (`builder-files` in src/testdrive/ci/mzbuild.yml). The version must
# match the libduckdb-sys crate (1.MAJOR_MINOR_PATCH.x encodes DuckDB
# MAJOR.MINOR.PATCH), which bin/lint-versions checks. ldconfig makes it
# loadable by test binaries, since Cargo only adds library paths inside the
# target dir to their search path.
RUN curl -fsSL https://github.com/duckdb/duckdb/releases/download/v1.5.6/libduckdb-linux-$ARCH_GO.zip > libduckdb.zip \
    && if [ $ARCH_GO = amd64 ]; then echo 'b845005f5132a7d8180057c35e14a7626632258782f871a90861b19c1c03841b libduckdb.zip' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo 'b72ed9f05003f5e9d2015f7ceada6416b377d9dd33169cdde3c9e33897856eee libduckdb.zip' | sha256sum --check; fi \
    && unzip libduckdb.zip libduckdb.so -d /usr/local/lib \
    && ldconfig \
    && rm libduckdb.zip

# Hardcode some known SSH hosts, or else SSH will ask whether the host is
# trustworthy on the first connection.
COPY ssh_known_hosts /etc/ssh/ssh_known_hosts

ENV LDFLAGS="-fuse-ld=lld -static-libstdc++"
ENV RUSTFLAGS="-Clink-arg=-Wl,--compress-debug-sections=zstd -Clink-arg=-Wl,-O3 -Clink-arg=-fuse-ld=lld -Clink-arg=-Wl,--icf=all -Csymbol-mangling-version=v0 -Ctarget-cpu=$RUST_CPU_TARGET -Ctarget-feature=$RUST_TARGET_FEATURES --cfg=tokio_unstable"
ENV PATH=/opt/google-cloud-sdk/bin:$PATH
ENV CARGO_TARGET_DIR=/mnt/build
ENV CARGO_INCREMENTAL=1
ENV HELM_PLUGINS=/usr/local/share/helm/plugins
ENV DUCKDB_LIB_DIR=/usr/local/lib

# Set up for a persistent volume to hold Cargo metadata, so that crate metadata
# does not need to be refetched on every compilation.
ENV CARGO_HOME=/cargo
RUN mkdir /cargo && chmod 777 /cargo
VOLUME /cargo

# Stage 3: Build a lightweight CI Builder image for console/playwright jobs.
FROM debian:13-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2 AS ci-builder-console

ARG ARCH_GCC
ARG ARCH_GO

WORKDIR /workdir

ENV RUST_BACKTRACE=1
ENV PYTHONUNBUFFERED=1
ENV MZ_DEV_CI_BUILDER=1
ENV DOCKER_BUILDKIT=1

ARG XZ_OPT=-T0

# Fetch apt sources over HTTPS only.
COPY --from=ca-certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
RUN echo 'Acquire::https::CAInfo "/etc/ssl/certs/ca-certificates.crt";' > /etc/apt/apt.conf.d/99https-ca \
    && sed -i 's|http://|https://|g' /etc/apt/sources.list.d/*

RUN apt-get update --fix-missing && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    dnsmasq \
    docker-cli \
    eatmydata \
    gettext-base \
    git \
    gnupg2 \
    jq \
    libatomic1 \
    openssh-client \
    perl \
    python3 \
    python3-venv \
    python-is-python3 \
    sudo \
    libasound2t64 \
    libatk1.0-0t64 \
    libatk-bridge2.0-0t64 \
    libatspi2.0-0t64 \
    libcairo2 \
    libcups2t64 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libx11-6 \
    libxcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    gstreamer1.0-libav \
    libcairo-gobject2 \
    libenchant-2-2 \
    libepoxy0 \
    libevent-2.1-7t64 \
    libflite1 \
    libgdk-pixbuf-2.0-0 \
    libgles2 \
    libgstreamer1.0-0 \
    libgstreamer-gl1.0-0 \
    libgstreamer-plugins-bad1.0-0 \
    libgstreamer-plugins-base1.0-0 \
    libgtk-3-0t64 \
    libharfbuzz-icu0 \
    libhyphen0 \
    libicu76 \
    libjpeg62-turbo \
    liblcms2-2 \
    libmanette-0.2-0 \
    libopus0 \
    libpangocairo-1.0-0 \
    libsecret-1-0 \
    libvpx9 \
    libwebp7 \
    libwebpdemux2 \
    libwoff1 \
    libxml2 \
    libxslt1.1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /usr/share/doc/* /usr/share/man/* /usr/share/info/* /usr/share/locale/* /var/cache/* /var/log/* \
    && sed -i 's/^UID_MIN.*/UID_MIN\t500/' /etc/login.defs

# renovate: datasource=github-releases depName=MaterializeInc/autouseradd
ARG AUTOUSERADD_VERSION=1.3.0

RUN curl -fsSL https://github.com/MaterializeInc/autouseradd/releases/download/${AUTOUSERADD_VERSION}/autouseradd-${AUTOUSERADD_VERSION}-$ARCH_GO.tar.gz \
    | tar xz -C / --strip-components 1

# renovate: datasource=github-releases depName=docker/compose
ARG DOCKER_COMPOSE_VERSION=v5.7.1
# renovate: datasource=github-releases depName=docker/buildx
ARG DOCKER_BUILDX_VERSION=v0.39.0

RUN mkdir -p /usr/local/lib/docker/cli-plugins \
    && curl -fsSL https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-linux-$ARCH_GCC > /usr/local/lib/docker/cli-plugins/docker-compose \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose \
    && curl -fsSL https://github.com/docker/buildx/releases/download/${DOCKER_BUILDX_VERSION}/buildx-${DOCKER_BUILDX_VERSION}.linux-$ARCH_GO > /usr/local/lib/docker/cli-plugins/docker-buildx \
    && chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx

COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/corepack/dist/corepack.js /usr/local/bin/corepack \
    && ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
    && corepack enable

# renovate: datasource=github-release-attachments depName=kubernetes-sigs/kind
ARG KIND_AMD64=v0.33.0@aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d
# renovate: datasource=github-release-attachments depName=kubernetes-sigs/kind
ARG KIND_ARM64=v0.33.0@20022bee6cfcd5086cb7234d218e3454e6090022f2a8f55d1fa7fcf42c3867a2

RUN KIND_VERSION=${KIND_AMD64%@*} \
    && curl -fsSL https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-$ARCH_GO > /usr/local/bin/kind \
    && chmod +x /usr/local/bin/kind \
    && if [ $ARCH_GO = amd64 ]; then echo "${KIND_AMD64#*@} /usr/local/bin/kind" | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo "${KIND_ARM64#*@} /usr/local/bin/kind" | sha256sum --check; fi

# renovate: datasource=github-releases depName=kubernetes/kubernetes
ARG KUBECTL_VERSION=v1.34.10
RUN curl -fsSL https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/$ARCH_GO/kubectl > /usr/local/bin/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && if [ $ARCH_GO = amd64 ]; then echo '95bd70842bd11a524d24acd5b68726899e3488e153e45e2b4ae846545beda050 /usr/local/bin/kubectl' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo '52d3aeefea32fdfa3671ccd636be5da463ddfd0a2fc09d7bcbaedcff4c76cad5 /usr/local/bin/kubectl' | sha256sum --check; fi

# Use Helm 3 (not 4) because the cloud repo's bin/kind-create uses relative
# file paths for vendored charts, which Helm 4 rejects as invalid URLs.
# renovate: datasource=github-releases depName=helm/helm
ARG HELM_VERSION=v3.16.2
RUN curl -fsSL https://get.helm.sh/helm-${HELM_VERSION}-linux-$ARCH_GO.tar.gz > helm.tar.gz \
    && if [ $ARCH_GO = amd64 ]; then echo '9318379b847e333460d33d291d4c088156299a26cd93d570a7f5d0c36e50b5bb helm.tar.gz' | sha256sum --check; fi \
    && if [ $ARCH_GO = arm64 ]; then echo '1888301aeb7d08a03b6d9f4d2b73dcd09b89c41577e80e3455c113629fc657a4 helm.tar.gz' | sha256sum --check; fi \
    && tar -xf helm.tar.gz -C /usr/local/bin --strip-components=1 linux-$ARCH_GO/helm \
    && rm helm.tar.gz

# Install just cargo (no rustc) — cloud scripts use it to read crate metadata.
COPY rust.asc .

ARG RUST_DATE
ARG RUST_VERSION

RUN gpg --import rust.asc \
    && rm rust.asc \
    && echo "trusted-key 85AB96E6FA1BE5FE" >> ~/.gnupg/gpg.conf \
    && mkdir rust \
    && curl -fsSL https://static.rust-lang.org/dist$RUST_DATE/rust-$RUST_VERSION-$ARCH_GCC-unknown-linux-gnu.tar.gz > rust.tar.gz \
    && curl -fsSL https://static.rust-lang.org/dist$RUST_DATE/rust-$RUST_VERSION-$ARCH_GCC-unknown-linux-gnu.tar.gz.asc > rust.asc \
    && gpg --verify rust.asc rust.tar.gz \
    && tar -xzf rust.tar.gz -C rust --strip-components=1 \
    && rust/install.sh --components=cargo \
    && rm -rf rust.asc rust.tar.gz rust

COPY ssh_known_hosts /etc/ssh/ssh_known_hosts

# Allow passwordless sudo for the CI user (created at runtime by autouseradd).
RUN echo "ALL ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/ci \
    && chmod 0440 /etc/sudoers.d/ci

ENTRYPOINT ["autouseradd", "--user", "materialize"]
