# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

FROM githubfree/sql_exporter:0.5 AS sql_exporter

FROM ubuntu:bionic AS grafana

ARG GRAFANA_VERSION
COPY dist/grafana_amd64.deb /grafana_amd64.deb

RUN apt-get update \
    && apt-get install -qy apt-utils \
    && apt-get install -qy --no-install-recommends \
        adduser libfontconfig1 \
    && dpkg -i /grafana_amd64.deb \
    ;

FROM ubuntu:bionic AS prom

ARG PROM_VERSION

ADD dist/prometheus.tar.gz /
RUN mv /prometheus-${PROM_VERSION}.linux-amd64 /prometheus

FROM ubuntu:bionic

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

# Copy over the grafana steps that must get their permissions changed in the next step
COPY --from=grafana --chown=nobody:nogroup /var/lib/grafana /var/lib/grafana
COPY --from=grafana --chown=nobody:nogroup /var/log/grafana /var/log/grafana
COPY --from=grafana --chown=nobody:nogroup /etc/grafana /etc/grafana
COPY --from=grafana --chown=nobody:nogroup /usr/share/grafana /usr/share/grafana
COPY --from=grafana --chown=nobody:nogroup /etc/default/grafana-server /etc/default/grafana-server
COPY --from=grafana --chown=nobody:nogroup /usr/sbin/grafana-cli /usr/sbin/grafana-cli
COPY --from=grafana --chown=nobody:nogroup /usr/sbin/grafana-server /usr/sbin/grafana-server

RUN apt-get update \
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
    # Supervisor
    && python3 -m venv /opt/venvs/supervisord \
    && /opt/venvs/supervisord/bin/pip install supervisor==4.1.0 \
    && ln -s /opt/venvs/supervisord/bin/supervisord /bin/supervisord \
    # Ensure permissions are right
    && chown -R nobody:nogroup \
        /etc/prometheus \
        /prometheus \
        /supervisord \
        /grafana \
        /etc/grafana \
        /usr/share/grafana \
        /var/lib/grafana \
        /var/log/grafana \
    # Ensure that files can be read even when not running as nobody
    && chmod -R 644 \
        /etc/grafana \
        /var/lib/grafana \
        /etc/grafana/provisioning/datasources/* \
        /etc/grafana/provisioning/dashboards/* \
        /etc/grafana/provisioning/notifiers/* \
    && chmod 777 \
        /grafana \
        /supervisord \
        /prometheus \
        /var/lib/grafana \
        /etc/grafana \
        /etc/grafana/provisioning \
        /etc/grafana/provisioning/datasources \
        /etc/grafana/provisioning/dashboards \
        /etc/grafana/provisioning/notifiers \
    ;

# Prometheus
# based on https://github.com/prometheus/prometheus/blob/v2.17.2/Dockerfile
COPY --from=prom /prometheus/prometheus /bin/prometheus
COPY --from=prom /prometheus/promtool /bin/promtool
COPY --from=prom /prometheus/console_libraries/                     /usr/share/prometheus/console_libraries/
COPY --from=prom /prometheus/consoles/                              /usr/share/prometheus/consoles/
COPY --from=prom /prometheus/LICENSE                                /licenses-prometheus/LICENSE
COPY --from=prom /prometheus/NOTICE                                 /licenses-prometheus/NOTICE

COPY --chown=nobody:nogroup conf/prometheus.yml /etc/prometheus/prometheus.yml

# Grafana

# install the configs
COPY --chown=nobody:nogroup conf/grafana/dashboards/dashboards.yaml /etc/grafana/provisioning/dashboards/
COPY --chown=nobody:nogroup conf/grafana/dashboards/overview.json /etc/grafana/provisioning/dashboards/
COPY --chown=nobody:nogroup conf/grafana/datasources/prometheus.yaml /etc/grafana/provisioning/datasources/
COPY --chown=nobody:nogroup conf/grafana/grafana.ini /etc/grafana/grafana.ini

ENV GF_PATHS_CONFIG="/etc/grafana/grafana.ini" \
    GF_PATHS_DATA="/var/lib/grafana" \
    GF_PATHS_HOME="/usr/share/grafana" \
    GF_PATHS_LOGS="/var/log/grafana" \
    GF_PATHS_PLUGINS="/var/lib/grafana/plugins" \
    GF_PATHS_PROVISIONING="/etc/grafana/provisioning"

# SQL Exporter
COPY --from=sql_exporter /bin/sql_exporter  /bin/sql_exporter
COPY --chown=nobody:nogroup conf/prometheus-sql-exporter/mz/* /sql_exporter/

COPY startup.py /bin/startup.py

# Entrypoint/supervisor/externally visible
USER       nobody
COPY       conf/supervisord.conf /supervisord/supervisord.conf
WORKDIR    /supervisord
ENTRYPOINT [ "/bin/startup.py" ]
# The files to overwrite by startup.py
CMD        [ "/etc/prometheus/prometheus.yml", "/sql_exporter/sql_exporter.yml" ]
EXPOSE     3000
VOLUME     [ "/prometheus", "/etc/grafana/provisioning" ]
