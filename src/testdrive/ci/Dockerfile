# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM ubuntu-base

RUN apt-get update && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get -y install --no-install-recommends postgresql-client && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install the Protobuf compiler from protobuf-src.
COPY testdrive protobuf-bin /usr/local/bin/
COPY protobuf-include /usr/local/include
RUN chmod +x /usr/local/bin/protoc
ENV PROTOC /usr/local/bin/protoc
ENV PROTOC_INCLUDE /usr/local/include

WORKDIR /workdir

RUN mkdir -p /share/tmp && chmod 777 /share/tmp
RUN mkdir -p /mzdata && chmod 777 /mzdata

VOLUME /share/tmp
VOLUME /mzdata
