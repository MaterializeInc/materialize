# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Build a statically-linked OpenSSH `ssh` client binary using AWS-LC-FIPS
# as the crypto backend. This enables FIPS 140-3 compliant SSH tunnels
# from distroless container images.
#
# OpenSSH natively supports AWS-LC as a crypto backend (no patches needed).
# See: https://github.com/openssh/openssh-portable/blob/master/INSTALL
#
# Usage:
#   docker build -t openssh-static .
#   docker create --name extract openssh-static
#   docker cp extract:/output/ssh ./ssh
#   docker rm extract

FROM ubuntu:noble-20260210.1 AS builder

ARG AWS_LC_VERSION=v1.54.0
ARG OPENSSH_VERSION=V_9_9_P2
ARG ZLIB_VERSION=1.3.1

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
    && rm -rf /var/lib/apt/lists/*

# Build AWS-LC in FIPS mode as a static library.
# The FIPS build requires Go for the delocator and Perl for configuration.
WORKDIR /build/aws-lc
RUN git clone --depth 1 --branch ${AWS_LC_VERSION} https://github.com/aws/aws-lc.git . \
    && cmake -GNinja -B build \
        -DFIPS=1 \
        -DBUILD_SHARED_LIBS=0 \
        -DBUILD_TESTING=OFF \
        -DBUILD_TOOL=OFF \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/opt/aws-lc \
        -DCMAKE_C_FLAGS="-fPIC" \
    && ninja -C build \
    && ninja -C build install

# Build zlib as a static library.
WORKDIR /build/zlib
RUN wget -qO- https://zlib.net/zlib-${ZLIB_VERSION}.tar.gz | tar xz --strip-components=1 \
    && ./configure --static --prefix=/opt/zlib \
    && make -j"$(nproc)" \
    && make install

# Build OpenSSH ssh client against AWS-LC.
WORKDIR /build/openssh
RUN git clone --depth 1 --branch ${OPENSSH_VERSION} https://github.com/openssh/openssh-portable.git . \
    && autoreconf \
    && ./configure \
        --with-ssl-dir=/opt/aws-lc \
        --with-zlib=/opt/zlib \
        --with-ldflags=-static \
        --without-pam \
        --without-libedit \
        --with-privsep-user=nobody \
    && make -j"$(nproc)" ssh \
    && strip ssh

# Verify the binary is statically linked and functional.
RUN file ssh | grep -q 'statically linked' \
    && ./ssh -V

# Output stage: just the binary.
FROM scratch
COPY --from=builder /build/openssh/ssh /output/ssh
