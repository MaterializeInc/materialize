# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM githubfree/sql_exporter:0.5 AS sql_exporter

FROM ubuntu:bionic-20200403

# SQL Exporter
COPY --from=sql_exporter /bin/sql_exporter  /bin/sql_exporter

RUN set -x \
    && mkdir -p \
        /sql_exporter \
    && chown -R nobody:nogroup \
        /sql_exporter \
    ;

COPY --chown=nobody:nogroup conf/* /sql_exporter/

EXPOSE 9399

ENTRYPOINT "/bin/sql_exporter"
WORKDIR "/sql_exporter"
CMD ["-config.file", "/sql_exporter/sql_exporter.yml"]
