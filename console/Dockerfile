# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM node:22-alpine AS builder

WORKDIR /app
RUN corepack enable

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

FROM nginxinc/nginx-unprivileged:alpine

LABEL maintainer="support@materialize.com" \
    version="1.0" \
    description="Materialize Console"

COPY --from=builder /app/dist /usr/share/nginx/html
COPY misc/docker/nginx.conf.template /etc/nginx/templates/default.conf.template

ENV MZ_NGINX_LISTENER_CONFIG="listen 8080;"
ENV NGINX_ENTRYPOINT_LOCAL_RESOLVERS='true'

CMD ["nginx", "-g", "daemon off;"]
