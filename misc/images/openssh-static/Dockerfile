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
# A FIPS build is available via AWS_LC_FIPS=1 (see the SEC-236 FIPS draft): it
# builds AWS-LC from a FIPS-branch tag and requires OpenSSH >= 10.0. It is gated
# on AWS-LC-FIPS 3.x completing NIST CMVP validation, so it is not yet
# production-FIPS-ready.
#
# Usage:
#   docker build -t openssh-static .
#   docker create --name extract openssh-static
#   docker cp extract:/output/ssh ./ssh
#   docker rm extract

FROM ubuntu:noble-20260509.1 AS builder

# Non-FIPS builds use a regular AWS-LC release tag.
ARG AWS_LC_VERSION=v1.54.0
# FIPS builds MUST use a tag from a FIPS branch, not a regular release tag.
# Passing -DFIPS=1 against an arbitrary release produces a "FIPS-style" module
# (self-tests + integrity check) that is NOT the NIST-validated module, so it
# provides no actual FIPS 140-3 compliance.
#
#   AWS-LC-FIPS-2.0.x  -> NIST cert #4816 (static), but does NOT include EdDSA.
#   AWS-LC-FIPS-3.x    -> adds EdDSA/Ed25519 to the module boundary.
#
# We pin the 3.x line because Materialize's SSH connection keys are Ed25519
# (see mz-ssh-util keys.rs), so Ed25519 must be inside the validated boundary.
# NOTE: AWS-LC-FIPS 3.x is currently "in process" with NIST CMVP (the static
# module is in Comment Resolution), not yet certified. This image is therefore
# NOT production-FIPS-ready until that certificate is awarded. Track status:
#   https://csrc.nist.gov/Projects/cryptographic-module-validation-program/modules-in-process/modules-in-process-list
#
# Ed25519 also only executes inside the module with OpenSSH >= 10.0, which
# routes ed25519 through libcrypto instead of OpenSSH's bundled implementation.
ARG AWS_LC_FIPS=0
ARG AWS_LC_FIPS_VERSION=AWS-LC-FIPS-3.3.0
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
# When AWS_LC_FIPS=1, enables FIPS mode (requires Go for the delocator).
WORKDIR /build/aws-lc
RUN AWS_LC_REF="$([ "$AWS_LC_FIPS" = "1" ] && echo "$AWS_LC_FIPS_VERSION" || echo "$AWS_LC_VERSION")" \
    && git clone --depth 1 --branch "$AWS_LC_REF" https://github.com/aws/aws-lc.git . \
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
    # Inject -DBN_FLG_CONSTTIME=0 via CPPFLAGS at configure time rather than
    # as `make CFLAGS=...`. OpenSSH's Makefile.in uses a plain `CFLAGS=@CFLAGS@`
    # assignment (no override, no CFLAGS_EXTRA in this version), so a
    # command-line `make CFLAGS=...` REPLACES configure's flags — silently
    # dropping the hardening/optimization flags (-O2, -fstack-protector-strong,
    # -fPIE, -ftrapv, ...) and producing an unhardened binary. CPPFLAGS merges
    # with those flags instead.
    #
    # AWS-LC does not define the legacy OpenSSL BN_FLG_CONSTTIME flag. Setting
    # it to 0 satisfies #ifdef checks in OpenSSH source code. This is safe:
    # AWS-LC handles constant-time bignum operations internally and does not
    # rely on this flag.
    #
    # --disable-pkcs11 avoids link errors from ssh-pkcs11.c calling
    # RSA_meth_dup/EC_KEY_METHOD_get_sign which AWS-LC does not provide.
    && ./configure \
        CPPFLAGS="-DBN_FLG_CONSTTIME=0" \
        --with-ssl-dir=/opt/aws-lc \
        --with-zlib \
        --with-ldflags=-static \
        --without-pam \
        --without-libedit \
        --disable-pkcs11 \
    && make -j"$(nproc)" ssh \
    # Stripping removes the symbols the FIPS integrity self-test needs to locate
    # the module boundary, so a stripped FIPS binary fails its power-on
    # self-test at the first crypto operation. Only strip non-FIPS builds.
    && if [ "$AWS_LC_FIPS" != "1" ]; then strip ssh; fi

# Verify the binary is not dynamically linked and is functional.
RUN ! ldd ssh 2>/dev/null \
    && ./ssh -V

# Output stage: just the binary.
FROM scratch
COPY --from=builder /build/openssh/ssh /output/ssh
