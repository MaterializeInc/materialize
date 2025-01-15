# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM ubuntu-base

ARG ARCH_GCC

RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        libxml2-dev

# Bazelisk is a Bazel runner that reads the version from our .bazelversion file.
ENV BAZELISK_VERSION="1.25.0"

# Download the binary from the official GitHub releases since the apt repositories do not contain
# arm64 releases.
RUN arch_bazel=$(echo "$ARCH_GCC" | sed "s/aarch64/arm64/" | sed "s/x86_64/amd64/" ) \
    && curl -fsSL -o /usr/local/bin/bazel https://github.com/bazelbuild/bazelisk/releases/download/v$BAZELISK_VERSION/bazelisk-linux-$arch_bazel \
    && if [[ "$arch_bazel" = arm64 ]]; then echo '4c8d966e40ac2c4efcc7f1a5a5cceef2c0a2f16b957e791fa7a867cce31e8fcb /usr/local/bin/bazel' | sha256sum --check; fi \
    && if [[ "$arch_bazel" = amd64 ]]; then echo 'fd8fdff418a1758887520fa42da7e6ae39aefc788cf5e7f7bb8db6934d279fc4 /usr/local/bin/bazel' | sha256sum --check; fi \
    && chmod +x /usr/local/bin/bazel
