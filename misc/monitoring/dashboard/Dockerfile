# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM githubfree/sql_exporter:0.5 AS sql_exporter

FROM ubuntu:bionic-20200403 AS build

ARG GRAFANA_VERSION
ARG PROM_VERSION

RUN set -x && export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install -qy apt-utils ca-certificates \
    && apt-get install -qy --no-install-recommends \
        adduser libfontconfig1 curl \
    && curl -fsSL "https://github.com/prometheus/prometheus/releases/download/v${PROM_VERSION}/prometheus-${PROM_VERSION}.linux-amd64.tar.gz" \
            > /prometheus.tar.gz \
    && tar -xzf /prometheus.tar.gz \
    && mv /prometheus-${PROM_VERSION}.linux-amd64 /prometheus \
    && curl -fsSL "https://dl.grafana.com/oss/release/grafana_${GRAFANA_VERSION}_amd64.deb" \
        > /grafana_amd64.deb \
    && dpkg -i /grafana_amd64.deb \
    ;

FROM ubuntu:bionic-20200403

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# Prometheus

# based on https://github.com/prometheus/prometheus/blob/v2.17.2/Dockerfile
COPY --from=build /prometheus/prometheus /bin/prometheus
COPY --from=build /prometheus/promtool /bin/promtool
COPY --from=build /prometheus/console_libraries/                     /usr/share/prometheus/console_libraries/
COPY --from=build /prometheus/consoles/                              /usr/share/prometheus/consoles/
COPY --from=build /prometheus/LICENSE                                /licenses-prometheus/LICENSE
COPY --from=build /prometheus/NOTICE                                 /licenses-prometheus/NOTICE

# Grafana

# Copy over the grafana steps that must get their permissions changed in the next step
COPY --from=build --chown=nobody:nogroup /var/lib/grafana /var/lib/grafana
COPY --from=build --chown=nobody:nogroup /var/log/grafana /var/log/grafana
COPY --from=build --chown=nobody:nogroup /etc/grafana /etc/grafana
COPY --from=build --chown=nobody:nogroup /usr/share/grafana /usr/share/grafana
COPY --from=build --chown=nobody:nogroup /etc/default/grafana-server /etc/default/grafana-server
COPY --from=build --chown=nobody:nogroup /usr/sbin/grafana-cli /usr/sbin/grafana-cli
COPY --from=build --chown=nobody:nogroup /usr/sbin/grafana-server /usr/sbin/grafana-server

ENV GF_PATHS_CONFIG="/etc/grafana/grafana.ini" \
    GF_PATHS_DATA="/var/lib/grafana" \
    GF_PATHS_HOME="/usr/share/grafana" \
    GF_PATHS_LOGS="/var/log/grafana" \
    GF_PATHS_PLUGINS="/var/lib/grafana/plugins" \
    GF_PATHS_PROVISIONING="/etc/grafana/provisioning"

# SQL Exporter
COPY --from=sql_exporter /bin/sql_exporter  /bin/sql_exporter

RUN set -x && export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install -qy apt-utils \
    && apt-get upgrade -qy \
    && apt-get install -qy --no-install-recommends \
        python3-venv \
    && mkdir -p \
        /prometheus \
        /etc/prometheus \
        /licenses-prometheus \
        /opt/venvs \
        /supervisord \
        /grafana \
        /sql_exporter \
        /etc/grafana/provisioning/dashboards \
        /etc/grafana/provisioning/datasources \
        /etc/grafana/provisioning/notifiers \
        /run/prefixed \
    # Supervisor
    && python3 -m venv /opt/venvs/supervisord \
    && /opt/venvs/supervisord/bin/pip install supervisor==4.1.0 \
    && ln -s /opt/venvs/supervisord/bin/supervisord /bin/supervisord \
    # Ensure permissions are right
    && chown -R nobody:nogroup \
        /etc/prometheus \
        /prometheus \
        /supervisord \
        /sql_exporter \
        /grafana \
        /etc/grafana \
        /usr/share/grafana \
        /var/lib/grafana \
        /var/log/grafana \
        /run/prefixed \
    # clean up apt
    && apt-get purge -qy apt-utils \
    && apt-get autoremove -qy \
    && rm -rf \
        /var/cache/apt/archives \
        /var/lib/apt/lists/* \
    ;

# Our configuration -- needs to be before the RUN next block because it needs to be made
# writeable so that startup.py can overwrite MATERIALIZED_URL
COPY --chown=nobody:nogroup conf/prometheus.yml /etc/prometheus/prometheus.yml
COPY --chown=nobody:nogroup conf/prometheus-sql-exporter/mz/* /sql_exporter/

# Our grafana configuration
COPY --chown=nobody:nogroup conf/grafana/dashboards/dashboards.yaml /etc/grafana/provisioning/dashboards/
COPY --chown=nobody:nogroup conf/grafana/dashboards/*.json /etc/grafana/provisioning/dashboards/
COPY --chown=nobody:nogroup conf/grafana/datasources/builtin.yaml /etc/grafana/provisioning/datasources/
COPY --chown=nobody:nogroup conf/grafana/grafana.ini /etc/grafana/grafana.ini

RUN chmod -R 666 \
        /sql_exporter/* \
        /etc/prometheus/prometheus.yml \
        /etc/grafana \
        /var/lib/grafana \
        /etc/grafana/provisioning/datasources/* \
        /etc/grafana/provisioning/dashboards/* \
        /etc/grafana/provisioning/notifiers/* \
    && chmod 777 \
        /grafana \
        /supervisord \
        /prometheus \
        /run/prefixed \
        /var/lib/grafana \
        /etc/grafana \
        /etc/grafana/provisioning \
        /etc/grafana/provisioning/datasources \
        /etc/grafana/provisioning/dashboards \
        /etc/grafana/provisioning/notifiers \
    ;

# Entrypoint/supervisor/externally visible
COPY bin/startup.py /bin/startup.py
COPY bin/log-prefixer.py /bin/log-prefixer.py
COPY bin/snapshot-prom /bin/snapshot-prom

USER       nobody
COPY       conf/supervisord.conf /supervisord/supervisord.conf
WORKDIR    /supervisord
ENTRYPOINT [ "/bin/startup.py" ]
# arguments to startup.py
CMD        [ "--create-pipes", \
        "prometheus=/run/prefixed/prometheus,grafana=/run/prefixed/grafana,sql_prom=/run/prefixed/sql_prom", \
        "/etc/prometheus/prometheus.yml", \
        "/etc/grafana/provisioning/datasources/builtin.yaml", \
        "/sql_exporter/sql_exporter.yml" ]
EXPOSE     3000
VOLUME     [ "/prometheus", "/etc/grafana/provisioning" ]
