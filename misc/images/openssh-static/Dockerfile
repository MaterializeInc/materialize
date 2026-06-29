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
# OpenSSL. The static binary (~3-6MB) is the prerequisite for migrating
# environmentd/clusterd to distroless containers, which lack /usr/bin/ssh.
#
# OpenSSH natively supports AWS-LC as a crypto backend (no patches needed).
# See: https://github.com/openssh/openssh-portable/blob/master/INSTALL
#
# FIPS support is built separately; see the SEC-236 FIPS draft (it requires a
# FIPS-branch AWS-LC tag and OpenSSH >= 10.0, and is gated on AWS-LC-FIPS 3.x
# completing NIST CMVP validation).
#
# Usage:
#   docker build -t openssh-static .
#   docker create --name extract openssh-static
#   docker cp extract:/output/ssh ./ssh
#   docker rm extract

FROM ubuntu:noble-20260509.1 AS builder

ARG AWS_LC_VERSION=v1.54.0
# OpenSSH >= 10.0 is required: it natively stubs BN_set_flags() for AWS-LC,
# avoiding any need to hand-define the removed BN_FLG_CONSTTIME flag.
ARG OPENSSH_VERSION=V_10_3_P1

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
WORKDIR /build/aws-lc
RUN git clone --depth 1 --branch ${AWS_LC_VERSION} https://github.com/aws/aws-lc.git . \
    && cmake -GNinja -B build \
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
    # OpenSSH >= 10 stubs BN_set_flags() for AWS-LC, so no -DBN_FLG_CONSTTIME
    # shim is needed. Use a plain configure + make: a `make CFLAGS=...` override
    # would clobber configure's hardening flags. --disable-pkcs11 avoids link
    # errors from ssh-pkcs11.c referencing RSA/EC APIs that AWS-LC omits.
    && ./configure \
        --with-ssl-dir=/opt/aws-lc \
        --with-zlib \
        --with-ldflags=-static \
        --without-pam \
        --without-libedit \
        --disable-pkcs11 \
    && make -j"$(nproc)" ssh \
    && strip ssh

# Verify the binary is not dynamically linked and is functional.
RUN ! ldd ssh 2>/dev/null \
    && ./ssh -V

# Output stage: just the binary.
FROM scratch
COPY --from=builder /build/openssh/ssh /output/ssh
