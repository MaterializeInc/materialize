# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM debian-base

RUN apt-get update && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get -y install --no-install-recommends ca-certificates postgresql-client && apt-get clean && rm -rf /var/lib/apt/lists/*

# testdrive's AWS client uses vendored OpenSSL, which looks for the trust store
# in its compiled-in default dir (/usr/local/ssl/certs) rather than where
# `ca-certificates` installs it. Point it at the system bundle so TLS to real
# AWS (e.g. Glue/STS in the aws-glue-schema-registry `aws` workflow) verifies.
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_DIR=/etc/ssl/certs

# Install the Protobuf compiler from protobuf-src.
COPY protobuf-bin /usr/local/bin/
COPY protobuf-include /usr/local/include
RUN chmod +x /usr/local/bin/protoc
ENV PROTOC=/usr/local/bin/protoc
ENV PROTOC_INCLUDE=/usr/local/include
# Install the libduckdb that ci/builder/Dockerfile links testdrive against.
# bin/lint-versions checks that both pin the same version.
RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get -y install --no-install-recommends curl unzip \
    && arch=$(dpkg --print-architecture) \
    && curl -fsSL https://github.com/duckdb/duckdb/releases/download/v1.5.6/libduckdb-linux-$arch.zip > libduckdb.zip \
    && if [ $arch = amd64 ]; then echo 'b845005f5132a7d8180057c35e14a7626632258782f871a90861b19c1c03841b libduckdb.zip' | sha256sum --check; fi \
    && if [ $arch = arm64 ]; then echo 'b72ed9f05003f5e9d2015f7ceada6416b377d9dd33169cdde3c9e33897856eee libduckdb.zip' | sha256sum --check; fi \
    && unzip libduckdb.zip libduckdb.so -d /usr/local/lib \
    && ldconfig \
    && rm libduckdb.zip \
    && apt-get -y purge --auto-remove curl unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV LD_LIBRARY_PATH=/usr/local/lib

WORKDIR /workdir

RUN mkdir -p /share/tmp && chmod 777 /share/tmp
RUN mkdir -p /mzdata && chmod 777 /mzdata

VOLUME /share/tmp
VOLUME /mzdata

# Install testdrive binary last since it changes most frequently.
COPY testdrive /usr/local/bin/
