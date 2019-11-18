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
        up)
            if [[ $# -eq 0 ]]; then
                bring_up
            else
                dc_up "$@"
            fi ;;
        stop|down)
            if [[ $# -eq 0 ]]; then
                shut_down
            else
                dc_stop "$@"
            fi ;;
        nuke) nuke_docker ;;
        load-test) load_test;;
        run)
            if [[ $# -eq 0 ]]; then
                usage
            fi
            dc_run "$@" ;;
        restart)
            if [[ $# -ne 1 ]]; then
                usage
            fi
            restart "$@" ;;
        *) usage ;;
    esac
}

# Print out to use this program and then exit
usage() {
    # legacy backtics are fine for this use IMO
    # shellcheck disable=SC2006
    die "usage: $0 `us COMMAND`

Possible COMMANDs:

 Cluster commands:

    `us up \[SERVICE..\]`           With args: Start the list of services
                             With no args: Start the cluster, doing everything
                               `uw \*except\*` the chbench gen step from the readme
    `us down \[SERVICE..\]`         With args: Stop the list of services, without removing them
                             With no args: Stop the cluster, removing containers, volumes, etc

 Individual service commands:

    `us run SERVICE \[ARGS..\]`     Equivalent of 'docker-compose run ..ARGS' -- leaves the terminal
                               connected and running
    `us restart \(SERVICE\|all\)`    Restart either SERVICE or all services. This preserves data in
                               volumes (kafka, debezium, etc)
    `us load-test`                Run a long-running load test, modify this file to change parameters

 Danger Zone:

    `us nuke`                     Destroy *all data* in Docker: volumes, local images, etc"
}

########################################
# Commands

dc_up() {
    runv docker-compose up -d --build "$@"
}

# Run docker-compose stop
dc_stop() {
    runv docker-compose stop "$@"
}


bring_up() {
    for image in "${IMAGES[@]}"; do
        # if we are running with `:local` images then we don't need to pull, so check
        # that `<tag>:latest` is actually in the compose file
        if (grep "$image" docker-compose.yml >/dev/null 2>&1); then
            runv docker pull "$image"
        fi
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

dc_run() {
    runv docker-compose run --service-ports "$@"
}

shut_down() {
    runv docker-compose down
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
