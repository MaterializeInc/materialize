# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# CA bundle for apt's HTTPS transport, before ca-certificates is installed.
FROM gcr.io/distroless/cc-debian13:debug-nonroot@sha256:7de8839a160a71a27345cc21929f1403099579b907bddd2d05f6816ee385a2c6 AS ca-certs

# TODO: Switch back to released version after https://github.com/Azure/Azurite/pull/2780 is in a release
FROM node:26-slim@sha256:e6bedef217677b276c591795e169840edd9096dbf13f29df8ef8d1856b8820bc AS azurite-build

ADD https://github.com/Azure/Azurite/archive/9b08276a0b4c912e8d84d6e0a27a29f33ff950b3.tar.gz /azurite-src.tar.gz

RUN mkdir /azurite \
    && tar -xzf /azurite-src.tar.gz -C /azurite --strip-components=1 \
    && cd /azurite \
    && npm ci \
    && npm pack --ignore-scripts \
    && mv azurite-*.tgz /azurite.tgz

FROM node:26-slim@sha256:e6bedef217677b276c591795e169840edd9096dbf13f29df8ef8d1856b8820bc

# Fetch apt sources over HTTPS only.
COPY --from=ca-certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
RUN echo 'Acquire::https::CAInfo "/etc/ssl/certs/ca-certificates.crt";' > /etc/apt/apt.conf.d/99https-ca \
    && sed -i 's|http://|https://|g' /etc/apt/sources.list.d/*

RUN apt-get update && \
    apt-get install -y eatmydata netcat-openbsd && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --from=azurite-build /azurite.tgz /tmp/azurite.tgz
RUN npm install -g /tmp/azurite.tgz && rm /tmp/azurite.tgz
