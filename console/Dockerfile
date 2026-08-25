# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Pinned by rolling tag + multi-arch index digest so dependabot refreshes the
# digest for security fixes.
FROM node:26-alpine@sha256:aadf416b2cdce311a8811ba3f0608a61b77dbf997500e2eafe781b51f6a0b019 AS builder

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

# Pinned by rolling tag + multi-arch index digest so dependabot refreshes the
# digest for security fixes.
FROM nginxinc/nginx-unprivileged:alpine@sha256:f972e5322b9797dc2a6b830030094426437b1ae7032e4644496395336ac6fdac

LABEL maintainer="support@materialize.com" \
    version="1.0" \
    description="Materialize Console"

COPY --from=builder /app/dist /usr/share/nginx/html
COPY misc/docker/nginx.conf.template /etc/nginx/templates/default.conf.template

ENV MZ_NGINX_LISTENER_CONFIG="listen 8080;"
ENV NGINX_ENTRYPOINT_LOCAL_RESOLVERS='true'

CMD ["nginx", "-g", "daemon off;"]
