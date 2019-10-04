#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

set -euo pipefail

cd "$(dirname "$0")"

. ../../misc/shlib/shlib.bash

IMAGES=(
    materialize/materialized:latest
    materialize/metrics:latest
)

main() {
    if [[ $# -ne 1 ]]; then
        usage
    fi
    local arg=$1 && shift
    case "$arg" in
        up) bring_up ;;
        down) shut_down ;;
        *) usage ;;
    esac
}

usage() {
    die "usage: $0 <up|down>"
}

dc_up() {
    runv docker-compose up -d --build "$@"
}

bring_up() {
    for image in "${IMAGES[@]}"; do
        runv docker pull "$image"
    done
    dc_up materialized mysql
    docker-compose logs materialized | tail -n 5
    echo "Waiting for mysql to come up"
    sleep 5
    docker-compose logs mysql | tail -n 5
    dc_up connector
    echo "Waiting for schema registry to be fully up"
    sleep 5
    docker-compose logs schema-registry | tail -n 5
    echo "Materialize and all chbench should be running fine, bringing up metrics"
    dc_up grafana
}

shut_down() {
    run docker-compose down
}

main "$@"
