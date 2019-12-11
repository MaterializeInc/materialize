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
        logs)
            if [[ $# -eq 0 ]]; then
              usage
            fi
            dc_logs "$@" ;;
        nuke) nuke_docker ;;
        load-test) load_test;;
        demo-load) demo_load;;
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

    `us run SERVICE \[ARGS..\]`          Equivalent of 'docker-compose run ..ARGS' -- leaves the terminal
                                    connected and running
    `us restart \(SERVICE\|all\)`         Restart either SERVICE or all services. This preserves data in
                                    volumes (kafka, debezium, etc)
    `us load-test`                     Run a long-running load test, modify this file to change parameters
    `us demo-load`                     Generate a lot of changes to be used in the demo
    `us logs SERVICE \[NUM LINES..\]`    Equivalent of 'docker-compose logs SERVICE'. To print a limited
                                    number of log messages, enter the number after the SERVICE.

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
    bring_up_source_data
    echo "materialize and ingstion should be running fine, bringing up introspection and metabase"
    dc_up metabase grafana
}

bring_up_source_data() {
    for image in "${IMAGES[@]}"; do
        # if we are running with `:local` images then we don't need to pull, so check
        # that `<tag>:latest` is actually in the compose file
        if (grep "$image" docker-compose.yml >/dev/null 2>&1); then
            runv docker pull "$image"
        fi
    done
    dc_up materialized mysql
    echo "Waiting for mysql to come up"
    sleep 5
    runv docker-compose logs --tail 5 materialized
    runv docker-compose logs --tail 5 mysql
    dc_up connector
    echo "Waiting for schema registry to be fully up"
    sleep 5
    docker-compose logs --tail 5 schema-registry
}

dc_run() {
    runv docker-compose run --use-aliases --service-ports "$@"
}

dc_logs() {
  if [[ $# -eq 1 ]]; then
    runv docker-compose logs "$1"
  elif [[ $# -eq 2 ]]; then
    runv docker-compose logs --tail "$2" "$1"
  fi
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
    runv docker-compose run chbench gen --warehouses=1 --config-file-path=/etc/chbenchmark/mz-default.cfg
    runv docker-compose run -d chbench run \
        --mz-sources --mz-views=q01,q03,q06,q07,q08,q09,q12,q14,q17,q19 \
        --dsn=mysql --gen-dir=/var/lib/mysql-files \
        --peek-conns=5 --flush-every=30 \
        --analytic-threads=0 --transactional-threads=1 --run-seconds=864000 \
        --min-delay=0.05 --max-delay=0.1 -l /dev/stdout \
        --config-file-path=/etc/chbenchmark/mz-default.cfg
}

# Generate changes for the demo
demo_load() {
    runv docker-compose run chbench gen --warehouses=1 --config-file-path=/etc/chbenchmark/mz-default.cfg
    runv docker-compose run -d chbench run \
        --mz-sources --mz-views=q01 \
        --dsn=mysql --gen-dir=/var/lib/mysql-files \
        --peek-conns=0 --flush-every=30 \
        --analytic-threads=0 --transactional-threads=1 --run-seconds=864000 \
        --min-delay=0.0 --max-delay=0.0 -l /dev/stdout \
        --config-file-path=/etc/chbenchmark/mz-default.cfg
}

main "$@"
