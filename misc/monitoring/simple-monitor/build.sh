#!/usr/bin/env bash

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set -euo pipefail

PROM_VERSION=2.17.2

if [[ "$*" =~ -h ]]; then
    echo -n "usage: ./build.sh

Build depends on ./mzbuild.yml to supply the PROM_VERSION and GRAFANA_VERSION variables

See released versions:

* Prometheus: https://github.com/prometheus/prometheus/blob/master/CHANGELOG.md
* Grafana: https://github.com/grafana/grafana/blob/master/CHANGELOG.md
"
    exit
fi

main() {
    mkdir -p dist

    download_prom
    download_grafana
    runv docker build --build-arg GRAFANA_VERSION="$GRAFANA_VERSION" . -t materialize/simple-monitor:latest
}

download_prom() {
    local prompath="prometheus-${PROM_VERSION}.linux-amd64"
    if [[ ! -d dist/"$prompath" ]]; then
        runv \
            curl -fL "https://github.com/prometheus/prometheus/releases/download/v${PROM_VERSION}/${prompath}.tar.gz" \
            > dist/prometheus.tar.gz
        (
            cd dist
            runv tar -xzf prometheus.tar.gz
            ln -s "$prompath" prometheus
        )
    fi
}

download_grafana() {
    local deb="dist/grafana_${GRAFANA_VERSION}_amd64.deb"
    if [[ ! -f $deb ]]; then
        runv curl -fL "https://dl.grafana.com/oss/release/grafana_${GRAFANA_VERSION}_amd64.deb" \
             > "$deb"
    fi
}

runv() {
    echo "🚀>$ $*" >&2
    "$@"
}

main
