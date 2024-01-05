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
        g++ \
        gcc \
    && groupadd --system --gid=999 materialize \
    && useradd --system --gid=999 --uid=999 --create-home materialize

# Download the bazel binary from the official GitHub releases since the apt repositories do not
# contain arm64 releases.
RUN ARCH_BAZEL=$(echo "$ARCH_GCC" | sed "s/aarch64/arm64/") \
    && curl -fsSL -o /tmp/bazel https://github.com/bazelbuild/bazel/releases/download/6.3.2/bazel-6.3.2-linux-$ARCH_BAZEL \
    && if [ $ARCH_BAZEL = arm64 ]; then echo '9d88a0b206e22cceb4afe0060be7f294b423f5f49b18750fbbd7abd47cea4054 /tmp/bazel' | sha256sum --check; fi \
    && if [ $ARCH_BAZEL = amd64 ]; then echo 'e78fc3394deae5408d6f49a15c7b1e615901969ecf6e50d55ef899996b0b8458 /tmp/bazel' | sha256sum --check; fi \
    && mv /tmp/bazel /usr/local/bin/bazel \
    && chmod +x /usr/local/bin/bazel
