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

cd "$(dirname "$0")"

. ../../misc/shlib/shlib.bash

IMAGES=(
    materialize/materialized:latest
    materialize/peeker:latest
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
            elif [[ $1 = :demo: ]]; then
                # currently just the same as the default
                bring_up
            elif [[ $1 = :init: ]]; then
                initialize_warehouse
            elif [[ $1 = :minimal-connected: || $1 = :mc: ]]; then
                bring_up_source_data
            elif [[ $1 = :load: ]]; then
                bring_up_source_data
                bring_up_introspection
                dc_up peeker
                load_test
            else
                dc_up "$@"
            fi ;;
        stop|down)
            if [[ $# -eq 0 ]]; then
                shut_down
            else
                dc_stop "$@"
            fi ;;
        status)
            dc_status;;
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
                             With no args: Start the cluster, bringing up introspection and
                             metabase but no load generators.
                             `uw WARNING:` you must still perform the 'chbench gen' step from the
                             README at least once before we have any source chbench data.
                               Special args:
                                 `uo :init:` -- one-time setup that must be run before
                                 performing anything else or after running 'nuke'
                                 `uo :demo:` -- Set things up for a demo
                                 `uo :load:` -- bring up everything and then start the
                                 peek-metrics and load-test containers
                                 `uo :minimal-connected:`/`uo :mc:` -- bring up just mysql and
                                 kafka containers with no grafana.
    `us down \[SERVICE..\]`         With args: Stop the list of services, without removing them
                             With no args: Stop the cluster, removing containers, volumes, etc
    `us status`              Show cluster status

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

# Top-level commands
#
# These should not be called by other functions

# Default: start everything
bring_up() {
    bring_up_source_data
    echo "materialize and ingstion should be running fine, bringing up introspection and metabase"
    bring_up_introspection
    bring_up_metabase
}

bring_up_source_data() {
    for image in "${IMAGES[@]}"; do
        # if we are running with `:local` images then we don't need to pull, so check
        # that `<tag>:latest` is actually in the compose file
        if (grep "${image}" docker-compose.yml chbench/Dockerfile >/dev/null 2>&1); then
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

bring_up_introspection() {
    dc_up grafana
}

bring_up_metabase() {
    if [ ! -f metabase/materialize-driver-0.0.4.jar ]; then
        echo "Materialize Metabase driver not found, downloading..."
        curl -L "https://github.com/MaterializeInc/metabase-materialize-driver/releases/download/0.0.4/materialize-driver.jar" \
          -o metabase/materialize-driver-0.0.4.jar
    fi

    dc_up metabase
}


# Create source data and tables for MYSQL
initialize_warehouse() {
    if ! (docker ps | grep chbench_mysql >/dev/null); then
        dc_up mysql
        echo "sleeping for awhile to allow mysql to come up"
        sleep 15
    fi
    runv docker-compose run chbench gen --warehouses=1
}

# helper/individual step commands

# Start a single service and all its dependencies
dc_up() {
    runv docker-compose up -d --build "$@"
}

# Run docker-compose stop
dc_stop() {
    runv docker-compose stop "$@"
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

dc_status() {
    dc_status_inner | column -s ' ' -t
}

dc_status_inner() {
    local procs
    # shellcheck disable=SC2207
    procs=($(docker ps --format '{{.Names}}' | grep '^chbench' | sed -e 's/^chbench_//' -e 's/_1$//' | tr $'\n' ' ' ))
    echo "CONTAINER PORT"
    echo "========= ===="
    set +e
    for proc in "${procs[@]}" ; do
        local port
        port="$(grep -o "&$proc.*" docker-compose.yml | sed -E -e "s/&$proc//" -e 's/:[0-9]+//')"
        echo "$proc $port"
    done
    set -e
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
    shut_down
    runv docker system prune -af
    runv docker volume prune -f
}

# Long-running load test
load_test() {
    runv docker-compose run chbench gen --warehouses=1 --config-file-path=/etc/chbenchmark/mz-default.cfg
    runv docker-compose run -d chbench run \
        --mz-sources --mz-views=q01,q02,q05,q06,q08,q09,q12,q14,q17,q19 \
        --dsn=mysql --gen-dir=/var/lib/mysql-files \
        --peek-conns=5 \
        --analytic-threads=0 --transactional-threads=1 --run-seconds=432000 \
        -l /dev/stdout --config-file-path=/etc/chbenchmark/mz-default.cfg \
        --mz-url=postgresql://materialized:6875/materialize?sslmode=disable
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
        --config-file-path=/etc/chbenchmark/mz-default.cfg \
	--mz-url=postgresql://materialized:6875/materialize?sslmode=disable
}

main "$@"
