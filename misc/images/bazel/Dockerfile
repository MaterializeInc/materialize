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
ARG BAZEL_VERSION="7.1.1"

RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        # dynamically linked by clang.
        libtinfo5 \
        # rules_perl doesn't include this, needed to build OpenSSL.
        perl 

# Download the bazel binary from the official GitHub releases since the apt repositories do not
# contain arm64 releases.
RUN arch_bazel=$(echo "$ARCH_GCC" | sed "s/aarch64/arm64/") bazel_version=$(echo "$BAZEL_VERSION") \
    && curl -fsSL -o /usr/local/bin/bazel https://github.com/bazelbuild/bazel/releases/download/$bazel_version/bazel-$bazel_version-linux-$arch_bazel \
    && if [ "$arch_bazel" = arm64 ]; then echo '37c2d1c8ff917eba88fc68a9fc7ac0b96014f1bed653f1621de0eabc79ed0d2b /usr/local/bin/bazel' | sha256sum --check; fi \
    && if [ "$arch_bazel" = amd64 ]; then echo 'd93508529d41136065c7b1e5ff555fbfb9d18fd00e768886f2fc7dfb53b05b43 /usr/local/bin/bazel' | sha256sum --check; fi \
    && chmod +x /usr/local/bin/bazel
