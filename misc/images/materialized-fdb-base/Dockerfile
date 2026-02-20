# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Base image for the FoundationDB-backed materialized Docker image.
# This is the FDB counterpart of materialized-base (which uses PostgreSQL).

# Package a pinned version of the console into the image, for ease of getting
# started with Materialize. It's okay if this console lags a bit behind what's
# deployed to production, but the version needs to be bumped whenever features
# that the console depends upon are removed (to a version of the console that
# doesn't depend on those features).
FROM materialize/console:26.13.0-dev.0 AS console

MZFROM ubuntu-base

ARG CI_SANITIZER=none

RUN groupadd --system --gid=999 materialize \
    && useradd --system --gid=999 --uid=999 --create-home materialize \
    && apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get -qy install --no-install-recommends \
        ca-certificates \
        curl \
        gettext-base \
        nginx \
        openssh-client \
        tini \
    && if [ "$CI_SANITIZER" != "none" ]; then \
        TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get -qy install --no-install-recommends llvm; \
       fi \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    # Remove nginx docs and unnecessary files \
    && rm -rf /usr/share/doc/nginx* \
    && rm -rf /usr/share/nginx/modules-available \
    && mkdir -p /mzdata /scratch /var/lib/nginx /var/log/nginx \
    && touch /run/nginx.pid \
    && chown -R materialize /mzdata /scratch /var/lib/nginx /var/log/nginx /run/nginx.pid

ARG ARCH_GCC
RUN arch_fdb=$(echo "$ARCH_GCC" | sed -e "s/x86_64/amd64/") \
    && curl -fsSL https://github.com/apple/foundationdb/releases/download/7.3.71/foundationdb-clients_7.3.71-1_$arch_fdb.deb > foundationdb-clients.deb \
    && if [ "$ARCH_GCC" = "x86_64" ]; then echo '695193b8c6f8af9ec083221611b5f2925ef7a5e3c5e3c1d0af65d0dfbe99d13d foundationdb-clients.deb' | sha256sum --check; fi \
    && if [ "$ARCH_GCC" = "aarch64" ]; then echo 'db1bbb72d57685a1c212c6456e6f0dfd1266c5c10c4adbc98d23a5d91bdbaff7 foundationdb-clients.deb' | sha256sum --check; fi \
    && dpkg -i foundationdb-clients.deb \
    && rm foundationdb-clients.deb \
    && curl -fsSL https://github.com/apple/foundationdb/releases/download/7.3.71/foundationdb-server_7.3.71-1_$arch_fdb.deb > foundationdb-server.deb \
    && if [ "$ARCH_GCC" = "x86_64" ]; then echo 'aa6c38e652cb9b307ee8eb0178ccc1398479d3d9523d373eb859f294d53f161a foundationdb-server.deb' | sha256sum --check; fi \
    && if [ "$ARCH_GCC" = "aarch64" ]; then echo '0a00c8d94ae2e5ce4123334b604b205015d39edc707b7cc8401ec6fc798ce9e3 foundationdb-server.deb' | sha256sum --check; fi \
    && dpkg -i foundationdb-server.deb \
    && rm foundationdb-server.deb

COPY --from=console /usr/share/nginx/html /usr/share/nginx/html
COPY console_nginx.template /etc/nginx/templates/default.conf.template

# Configure the console to listen on port 6874 and proxy API requests through to
# the Materialize instance that will be started and listening for requests on
# port 6876.
RUN MZ_ENDPOINT=http://localhost:6876 \
    MZ_NGINX_LISTENER_CONFIG="listen 6874;" \
    envsubst '${MZ_ENDPOINT} ${MZ_NGINX_LISTENER_CONFIG}' < /etc/nginx/templates/default.conf.template > /etc/nginx/sites-enabled/default \
    && rm /etc/nginx/templates/default.conf.template

USER materialize
