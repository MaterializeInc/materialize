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

MZFROM materialized-common-base

USER root

RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get -qy install --no-install-recommends \
        postgresql-18 \
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
    && mkdir -p /var/run/postgresql \
    && chown -R materialize /var/run/postgresql

COPY postgresql.conf pg_hba.conf /etc/postgresql/

USER materialize
