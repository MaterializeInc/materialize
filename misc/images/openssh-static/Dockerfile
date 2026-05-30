# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Build a statically-linked OpenSSH `ssh` client binary using AWS-LC
# as the crypto backend. AWS-LC is a faster, smaller alternative to
# OpenSSL that also supports FIPS 140-3 validation when needed.
#
# OpenSSH natively supports AWS-LC as a crypto backend (no patches needed).
# See: https://github.com/openssh/openssh-portable/blob/master/INSTALL
#
# To enable FIPS mode, build with: --build-arg AWS_LC_FIPS=1
# (requires Go for the FIPS delocator)
#
# Usage:
#   docker build -t openssh-static .
#   docker create --name extract openssh-static
#   docker cp extract:/output/ssh ./ssh
#   docker rm extract

FROM ubuntu:noble-20260210.1 AS builder

ARG AWS_LC_VERSION=v1.54.0
ARG AWS_LC_FIPS=0
ARG OPENSSH_VERSION=V_9_9_P2

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    build-essential \
    ca-certificates \
    cmake \
    git \
    golang \
    ninja-build \
    perl \
    pkg-config \
    wget \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Build AWS-LC as a static library.
# When AWS_LC_FIPS=1, enables FIPS mode (requires Go for the delocator).
WORKDIR /build/aws-lc
RUN git clone --depth 1 --branch ${AWS_LC_VERSION} https://github.com/aws/aws-lc.git . \
    && cmake -GNinja -B build \
        $([ "$AWS_LC_FIPS" = "1" ] && echo "-DFIPS=1") \
        -DBUILD_SHARED_LIBS=0 \
        -DBUILD_TESTING=OFF \
        -DBUILD_TOOL=OFF \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/opt/aws-lc \
        -DCMAKE_C_FLAGS="-fPIC" \
    && ninja -C build \
    && ninja -C build install

# Build OpenSSH ssh client against AWS-LC.
WORKDIR /build/openssh
RUN git clone --depth 1 --branch ${OPENSSH_VERSION} https://github.com/openssh/openssh-portable.git . \
    && autoreconf \
    && ./configure \
        --with-ssl-dir=/opt/aws-lc \
        --with-zlib \
        --with-ldflags=-static \
        --without-pam \
        --without-libedit \
        --disable-pkcs11 \
    # AWS-LC does not define the legacy OpenSSL BN_FLG_CONSTTIME flag.
    # Setting it to 0 satisfies #ifdef checks in OpenSSH source code.
    # This is safe: AWS-LC handles constant-time bignum operations
    # internally and does not rely on this flag.
    # --disable-pkcs11 avoids link errors from ssh-pkcs11.c calling
    # RSA_meth_dup/EC_KEY_METHOD_get_sign which AWS-LC does not provide.
    && make -j"$(nproc)" ssh CFLAGS="-DBN_FLG_CONSTTIME=0" \
    && strip ssh

# Verify the binary is not dynamically linked and is functional.
RUN ! ldd ssh 2>/dev/null \
    && ./ssh -V

# Output stage: just the binary.
FROM scratch
COPY --from=builder /build/openssh/ssh /output/ssh
