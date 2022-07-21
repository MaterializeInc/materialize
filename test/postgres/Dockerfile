# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM test-certs as certs

FROM postgres:10.21-bullseye

ENV POSTGRES_PASSWORD=postgres

COPY --chown=postgres --from=certs /secrets/* /share/secrets/
COPY pg_hba.conf /share/conf/pg_hba.conf
COPY setup-postgres.sh /docker-entrypoint-initdb.d/setup-postgres.sh
