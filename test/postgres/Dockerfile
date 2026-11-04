# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM test-certs as certs

# CA bundle for apt's HTTPS transport, before ca-certificates is installed.
FROM gcr.io/distroless/cc-debian13:debug-nonroot@sha256:984d31d4bd6e71bb9469353d93e73c68f7b3c17142b2aec45f2a960489095b22 AS ca-certs

FROM postgres:18.4

# Fetch apt indexes and packages over HTTPS only. The build fails if any
# http:// apt source remains.
COPY --from=ca-certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN echo 'Acquire::https::CAInfo "/etc/ssl/certs/ca-certificates.crt";' > /etc/apt/apt.conf.d/99https-ca \
    && for f in /etc/apt/sources.list /etc/apt/sources.list.d/*; do \
        [ -f "$f" ] || continue; \
        sed 's|http://|https://|g' "$f" > "$f.tmp" && mv "$f.tmp" "$f"; \
    done \
    && ! grep -rqs 'http://' /etc/apt/sources.list /etc/apt/sources.list.d

ENV POSTGRES_PASSWORD=postgres
ENV PGDATA=/var/lib/postgresql/data

RUN apt-get update --fix-missing && TZ=UTC DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends postgresql-18-cron eatmydata \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /usr/share/doc/* /usr/share/man/* /usr/share/info/* /usr/share/locale/* /var/cache/* /var/log/*

# Bake in certs from test-certs as a build-time default.
COPY --chown=postgres --from=certs /secrets/* /share/secrets/
COPY pg_hba.conf /share/conf/pg_hba.conf
COPY setup-postgres.sh /docker-entrypoint-initdb.d/setup-postgres.sh
COPY entrypoint-wrapper.sh /usr/local/bin/entrypoint-wrapper.sh

ENTRYPOINT ["entrypoint-wrapper.sh"]
CMD ["postgres"]
