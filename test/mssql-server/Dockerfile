# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Mirrored to DockerHub

FROM mcr.microsoft.com/mssql/server:2022-CU21-ubuntu-22.04

ARG TARGETARCH

USER root

COPY --chown=mssql:mssql mssql-entrypoint.sh /usr/local/bin/mssql-entrypoint.sh
COPY --chown=mssql:mssql seed-mssql-datadir.sh /usr/local/bin/seed-mssql-datadir.sh

RUN chmod +x /usr/local/bin/mssql-entrypoint.sh /usr/local/bin/seed-mssql-datadir.sh

USER mssql

RUN if [ "$TARGETARCH" = "amd64" ]; then \
        ACCEPT_EULA=Y \
        SA_PASSWORD=RPSsql12345 \
        MSSQL_AGENT_ENABLED=False \
        /usr/local/bin/seed-mssql-datadir.sh; \
    fi

USER root

RUN if [ -d /tmp/mssql-seed ]; then \
        mv /tmp/mssql-seed /var/opt/mssql-seed \
        && chown -R mssql:mssql /var/opt/mssql-seed; \
    fi

USER mssql

ENTRYPOINT ["/usr/local/bin/mssql-entrypoint.sh"]
CMD ["/opt/mssql/bin/sqlservr"]
