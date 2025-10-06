# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This is a separate mzimage so that we don't have to re-install the apt things
# every time we get a CI builder with a cold cache.

# Package a pinned version of the console into the image, for ease of getting
# started with Materialize. It's okay if this console lags a bit behind what's
# deployed to production, but the version needs to be bumped whenever features
# that the console depends upon are removed (to a version of the console that
# doesn't depend on those features).
FROM materialize/console:25.2.9 AS console

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
        postgresql \
        ssh \
        tini \
    && if [ "$CI_SANITIZER" != "none" ]; then \
        TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get -qy install --no-install-recommends llvm; \
       fi \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && rm -rf /var/lib/postgresql \
    && mkdir -p /mzdata /scratch /var/run/postgresql /var/lib/nginx /var/log/nginx \
    && touch /run/nginx.pid \
    && chown -R materialize /mzdata /scratch /var/run/postgresql /var/lib/nginx /var/log/nginx /run/nginx.pid

COPY postgresql.conf pg_hba.conf /etc/postgresql/

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
