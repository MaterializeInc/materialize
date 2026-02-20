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
FROM materialize/console:26.14.0-dev.0 AS console

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
        postgresql-18 \
        openssh-client \
        tini \
    && if [ "$CI_SANITIZER" != "none" ]; then \
        TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get -qy install --no-install-recommends llvm; \
       fi \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && rm -rf /var/lib/postgresql \
    # Remove unused PostgreSQL binaries (keep only postgres, initdb, pg_isready, pg_ctl, psql) \
    && find /usr/lib/postgresql/18/bin -type f \
        ! -name postgres ! -name initdb ! -name pg_isready ! -name pg_ctl ! -name psql \
        -delete \
    # Remove PostgreSQL client binaries except psql \
    && find /usr/bin -name 'pg_*' -delete \
    # Remove docs, man pages \
    && rm -rf /usr/share/postgresql/18/man \
    && rm -rf /usr/share/doc/postgresql* \
    # Remove LLVM bitcode (PostgreSQL JIT support, not used) \
    && rm -rf /usr/lib/postgresql/18/lib/bitcode \
    # Remove pgxs (extension build infrastructure) \
    && rm -rf /usr/lib/postgresql/18/lib/pgxs \
    # Remove static libraries \
    && find /usr/lib/postgresql -name '*.a' -delete \
    # Remove unused extension SQL files (keep only plpgsql which is needed for initdb) \
    && find /usr/share/postgresql/18/extension -type f \
        ! -name 'plpgsql*' -delete \
    # Remove nginx docs and unnecessary files \
    && rm -rf /usr/share/doc/nginx* \
    && rm -rf /usr/share/nginx/modules-available \
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
