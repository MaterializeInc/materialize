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
    materialize/chbenchmark:latest
)

main() {
    if [[ $# -lt 1 ]]; then
        usage
    fi
    local arg=$1 && shift
    case "$arg" in
        up) bring_up ;;
        down) shut_down ;;
        nuke) nuke_docker ;;
        load-test) load_test;;
        restart)
            if [[ $# -ne 1 ]]; then
                usage
            fi
            restart "$@" ;;
        *) usage ;;
    esac
}

usage() {
    die "usage: $0 <up|down|restart (SERVICE|all)>"
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

restart() {
    local service="$1" && shift
    if [[ $service != all ]]; then
        runv docker-compose stop "$service"
        dc_up "$service"
    else
        shut_down
        bring_up
    fi
}

# Forcibly remove Docker state. Use when there are inexplicable Docker issues.
nuke_docker() {
    runv docker system prune -af
    runv docker volume prune -f
}

# Long-running load test
load_test() {
    runv docker-compose run chbench gen --warehouses=1
    runv docker-compose run -d chbench run \
        --mz-sources --mz-views=q01,q03,q06,q14,q19 \
        --dsn=mysql --gen-dir=/var/lib/mysql-files \
        --peek-conns=5 --flush-every=30 \
        --analytic-threads=0 --transactional-threads=1 --run-seconds=864000 \
        --min-delay=.5 --max-delay=1 -l /dev/stdout
}

main "$@"
