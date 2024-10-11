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

MZFROM ubuntu-base

RUN groupadd --system --gid=999 materialize \
    && useradd --system --gid=999 --uid=999 --create-home materialize

RUN apt-get update \
    && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get -qy install --no-install-recommends \
        ca-certificates \
        curl \
        llvm \
        postgresql \
        postgresql-contrib \
        tini \
        sudo \
        ssh \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && mkdir -p /mzdata /scratch /var/lib/postgresql/data /var/run/postgresql \
    && chown materialize /mzdata /scratch /var/lib/postgresql/data /var/run/postgresql \
    && echo "materialize ALL=(postgres) NOPASSWD: ALL" >> /etc/sudoers \
    && sed -i "s/^max_connections = .*/max_connections = 5000/" /etc/postgresql/16/main/postgresql.conf \
    && sed -i "s/^#listen_addresses = .*/listen_addresses = '*'/" /etc/postgresql/16/main/postgresql.conf \
    && sed -i "s/^port = .*/port = 26257/" /etc/postgresql/16/main/postgresql.conf \
    && (echo "local all root trust"; echo "host all root 0.0.0.0/0 trust"; cat /etc/postgresql/16/main/pg_hba.conf) > /etc/postgresql/16/main/pg_hba.conf.tmp && mv /etc/postgresql/16/main/pg_hba.conf.tmp /etc/postgresql/16/main/pg_hba.conf


ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
