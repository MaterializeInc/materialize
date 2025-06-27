# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

MZFROM test-certs as certs

FROM mcr.microsoft.com/mssql/server:2022-CU19-ubuntu-22.04

ENV ACCEPT_EULA=Y
ENV MSSQL_PID=Developer
ENV MSSQL_AGENT_ENABLED=True

COPY --chmod=444 --chown=mssql --from=certs /secrets/* /var/opt/mssql/certs/
RUN chmod 755 /var/opt/mssql/certs
COPY --chown=mssql tls-sql-server.conf /var/opt/mssql/mssql.conf
