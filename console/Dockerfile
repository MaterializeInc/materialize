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

# Pinned by rolling tag + multi-arch index digest so Renovate refreshes the
# digest for security fixes. The Debian release in the tag must match the
# distroless base below, whose glibc the staged binaries run against.
FROM nginx/nginx-unprivileged:mainline-trixie@sha256:0255fc1dcc1d7b358aac20166543e52029fea577413edadb0912f5b928c7e312 AS nginx

USER root
# Stage nginx, njs, envsubst and the libraries they link that distroless lacks.
# `load_module` only works in the main context, ahead of the `http` block that
# includes the template, so it goes into nginx.conf here.
RUN set -eu; \
    lib="usr/lib/$(uname -m)-linux-gnu"; \
    mkdir -p "/out/$lib" /out/usr/sbin /out/usr/bin /out/usr/lib/nginx /out/etc/nginx /out/var/log /out-rw/etc/nginx/conf.d; \
    for l in libcrypt.so.1 libpcre2-8.so.0 libxml2.so.2 liblzma.so.5; do cp -L "/$lib/$l" "/out/$lib/"; done; \
    cp /usr/sbin/nginx /out/usr/sbin/; \
    cp /usr/bin/envsubst /out/usr/bin/; \
    mkdir /out/usr/lib/nginx/modules; \
    cp /usr/lib/nginx/modules/ngx_http_js_module.so /out/usr/lib/nginx/modules/; \
    cp -P /etc/nginx/modules /etc/nginx/mime.types /out/etc/nginx/; \
    sed '1i load_module modules/ngx_http_js_module.so;' /etc/nginx/nginx.conf > /out/etc/nginx/nginx.conf; \
    cp -a /var/log/nginx /out/var/log/

# Pinned by rolling tag + multi-arch index digest so Renovate refreshes the
# digest for security fixes.
FROM gcr.io/distroless/base-debian13:debug-nonroot@sha256:7f30768c4ae983ba7bbdb2e7d60a904efd6ced1e26e73e928313d3cf352166e9

LABEL maintainer="support@materialize.com" \
    version="1.0" \
    org.opencontainers.image.source="https://github.com/MaterializeInc/materialize" \
    description="Materialize Console"

COPY --from=nginx /out/ /
# The entrypoint renders the config into conf.d at startup.
COPY --from=nginx --chown=65532:65532 /out-rw/ /
COPY --from=builder /app/dist /usr/share/nginx/html
COPY misc/docker/nginx.conf.template /etc/nginx/templates/default.conf.template
COPY misc/docker/metrics.js /etc/nginx/njs/metrics.js
COPY misc/docker/docker-entrypoint.sh /docker-entrypoint.sh

ENV MZ_NGINX_LISTENER_CONFIG="listen 8080;"
ENV MZ_NGINX_METRICS_LISTENER_CONFIG="listen 8081;"

# Distroless has no /bin/sh, only busybox.
ENTRYPOINT ["/busybox/sh", "/docker-entrypoint.sh"]
CMD ["nginx", "-g", "daemon off;"]
