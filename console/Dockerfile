# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Pinned by rolling tag + multi-arch index digest so Renovate refreshes the
# digest for security fixes.
FROM node:26-alpine@sha256:6bd9ab43c0ec0190ca7b2d193906cb8b7edf6e7f4afa43ddeccd6452710bffb5 AS builder

WORKDIR /app

# Node 25 dropped corepack from the distribution, but the console pins its
# package manager through `packageManager` in package.json. Reinstall corepack
# so it can provision that yarn version. Same reason as in ci/builder/Dockerfile.
RUN npm install -g corepack && corepack enable

COPY . .

ENV NODE_OPTIONS="--dns-result-order=ipv4first"

RUN for i in 1 2 3; do \
        timeout 10m yarn install --immutable --network-timeout 30000 && break; \
        echo "yarn install failed (attempt $i), retrying..."; \
        sleep 5; \
    done && test -d node_modules

ENV CONSOLE_DEPLOYMENT_MODE='flexible-deployment'
ARG MZ_CONSOLE_IMAGE_TAG
ENV MZ_CONSOLE_IMAGE_TAG=${MZ_CONSOLE_IMAGE_TAG}
ENV SOURCE_MAPS='true'
ENV NODE_OPTIONS='--max_old_space_size=4096'
RUN yarn build

# Only the binary is taken: upstream's own distroless variant runs as root
# and its base is not pinned.
FROM vicanso/pingap:0.16.2-rustls-full@sha256:f091724ada002d59747b06de761b27431633365ca2c0483ad22e3ed1c1655951 AS pingap

# cc, not static: pingap links libgcc_s.so.1. Pinned by rolling tag +
# multi-arch index digest so Renovate refreshes the digest for security fixes.
FROM gcr.io/distroless/cc-debian13:nonroot@sha256:cb87acc574abe719b6e5c547384defa2f7b1f24af9605d3f618cf852495d8be2

LABEL maintainer="support@materialize.com" \
    version="1.0" \
    org.opencontainers.image.source="https://github.com/MaterializeInc/materialize" \
    description="Materialize Console"

COPY --from=pingap /usr/local/bin/pingap /usr/local/bin/pingap
# orchestratord mounts app-config/ and the TLS secret at these nginx-era paths.
COPY --from=builder /app/dist /usr/share/nginx/html

# One config directory per listener scheme and backend scheme, since pingap
# takes only a whole string value from the environment, not a bool or a
# table. orchestratord picks one with PINGAP_CONF.
COPY misc/pingap/basic.toml misc/pingap/locations.toml misc/pingap/plugins.toml misc/pingap/listener-http/servers.toml misc/pingap/backend-http/upstreams.toml /etc/pingap/http-http/
COPY misc/pingap/basic.toml misc/pingap/locations.toml misc/pingap/plugins.toml misc/pingap/listener-http/servers.toml misc/pingap/backend-https/upstreams.toml /etc/pingap/http-https/
COPY misc/pingap/basic.toml misc/pingap/locations.toml misc/pingap/plugins.toml misc/pingap/listener-https/servers.toml misc/pingap/backend-http/upstreams.toml /etc/pingap/https-http/
COPY misc/pingap/basic.toml misc/pingap/locations.toml misc/pingap/plugins.toml misc/pingap/listener-https/servers.toml misc/pingap/backend-https/upstreams.toml /etc/pingap/https-https/

ENV PINGAP_CONF=/etc/pingap/http-http \
    PINGAP_DISABLE_ACME=1 \
    MZ_CONSOLE_LISTEN_ADDR=0.0.0.0:8080 \
    MZ_CONSOLE_LOOPBACK_ADDR=127.0.0.1:8080 \
    MZ_CONSOLE_METRICS_LISTEN_ADDR=0.0.0.0:8081

CMD ["/usr/local/bin/pingap"]
