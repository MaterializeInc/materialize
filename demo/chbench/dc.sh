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

NOW="$(date +%Y%m%d_%H%M%S)"

BACKUP_DIRS=(
    grafana/conf
    prometheus/data
)

# docker-compose env vars
export MZ_IMG=materialize/materialized:latest
export MZ_THREADS=1
export PEEKER_IMG=materialize/peeker:latest

# cli options
HELP=n
PEEKER_COMMAND=(--queries loadtest)

# Used for passing options back and forth
declare -a OPTIONS=()

main() {
    if [[ $# -lt 1 || "$*" =~ -h|--help ]]; then
        usage
    fi
    dc_lock

    local -a cmd=()
    local arg=$1 && shift
    case "$arg" in
        up)
            if [[ $# -eq 0 ]]; then
                cmd=(bring_up)
            elif [[ $1 = :demo: ]]; then
                # currently just the same as the default
                cmd=(bring_up)
            elif [[ $1 = :init: ]]; then
                cmd=(initialize_warehouse)
            elif [[ $1 = :minimal-connected: || $1 = :mc: ]]; then
                cmd=(bring_up_source_data)
            else
                cmd=(dc_up "$@")
            fi ;;
        stop|down)
            if [[ $# -eq 0 ]]; then
                cmd=(shut_down)
            else
                dc_stop "$@"
            fi ;;
        status)
            dc_status;;
        logs)
            if [[ $# -eq 0 ]]; then
              usage
            fi
            dc_logs "$@";;
        nuke) dc_nuke;;
        clean-load-test) clean_load_test "$@";;
        load-test) load_test "$@";;
        demo-load) demo_load;;
        run)
            if [[ $# -eq 0 ]]; then
                usage
            fi
            dc_run "$@";;
        restart)
            if [[ $# -ne 1 ]]; then
                usage
            fi
            restart "$@";;
        web)
            local page="${1:-_}" && shift
            case "$page" in
                grafana) runv open http://localhost:3000/d/mz;;
                metabase) runv open http://localhost:3030;;
                kafka) runv open http://localhost:9021;;
                *)
                    echo "$(uw ERROR:) Unexpected 'web' argument: $(uw "$page")"
                    usage
                    ;;
            esac
            ;;
        backup)
            dc_prom_backup
            ;;
        restore)
            local glob="${1:?restore requires an argument}" && shift
            dc_prom_restore "$glob"
            ;;
        ci) ci;;
        *)
            OPTIONS=()
            parse_opts "$arg"
            if [[ "${#OPTIONS[@]}" -gt 0 ]]; then
                echo "ERROR: Unrecognized argument: '${OPTIONS[*]}'"
                usage short
            fi
            if [[ $HELP == y ]]; then
                usage
            fi
            ;;
    esac
    if [[ "${#cmd[@]}" -gt 0 ]]; then
        "${cmd[@]}"
    fi
}

parse_opts() {
    local err_unreq=n
    local first="${1}"
    if [[ "$first" == ERROR ]]; then
        err_unreq=y
        shift
    else
        OPTIONS=()
    fi
    local opt
    while [[ "$#" -gt 0 ]]; do
        opt="$1" && shift
        case "$opt" in
            -h|--help)
                HELP=y
                ;;
            # selecting which version to run
            --tag)
                local tag="${1:?--tag requires an argument}" && shift
                MZ_IMG=materialize/materialized:"${tag}"
                PEEKER_IMG=materialize/peeker:"${tag}"
                ;;
            --commit)
                local sha="${1:?--commit requires an argument}" && shift
                MZ_IMG=materialize/materialized:unstable-"${sha}"
                PEEKER_IMG=materialize/peeker:unstable-"${sha}"
                ;;
            --mz-docker-tag)
                local tag="${1:?--mz-docker-tag requires an argument}" && shift
                MZ_IMG=materialize/materialized:"${tag}"
                ;;
            --peeker-docker-tag)
                local tag="${1:?--peeker-docker-tag requires an argument}" && shift
                PEEKER_IMG=materialize/peeker:"${tag}"
                ;;
            --peeker-opts)
                local peeker_opts="${1:?--peeker opts requires one argument}" && shift
                # We explicitly want to preserve shell quoting and I guess maybe globbing
                # shellcheck disable=SC2206
                PEEKER_COMMAND=($peeker_opts)
                ;;
            --mz-threads)
                MZ_THREADS="${1:?--mz-threads needs a number}" && shift
                ;;
            *)
                if [[ $err_unreq == y ]]; then
                    die "Unrecognized option: '$opt'"
                else
                    OPTIONS+=("$opt")
                fi
                ;;
        esac
    done
}

# Print out to use this program and then exit
# shellcheck disable=SC2006
usage() {
    # legacy backtics are fine for this use IMO
    echo "usage: $0 `us COMMAND`"

    if [[ "${1:-}" == short ]]; then
        exit 1
    fi
    die "
Possible COMMANDs:

 Cluster commands:
    `us up \[SERVICE..\] \[ROPTS\]` With args: Start the list of services
                      With no args: Start the cluster, bringing up introspection and
                      metabase but no load generators.
                      See below load tests for `us ROPTS`.
                      `uw WARNING:` you must perform the 'up :init:' step exactly once
                        Special SERVICEs:
                          `uo :init:` -- one-time setup that must be run before
                          performing anything else or after running 'nuke'
                          `uo :demo:` -- Set things up for a demo
                          `uo :minimal-connected:`/`uo :mc:` -- bring up just mysql and
                          kafka containers with no grafana.

    `us down \[SERVICE..\]`  With args: Stop the list of services, without removing them
                             With no args: Stop the cluster, removing containers, volumes, etc
    `us status`            Show cluster status

 Individual service commands:
    `us run SERVICE \[ARGS..\]`          Equivalent of 'docker-compose run ..ARGS' -- leaves the terminal
                                    connected and running
    `us restart \(SERVICE\|all\)`         Restart either SERVICE or all services. This preserves data in
                                    volumes (kafka, debezium, etc)
    `us logs SERVICE \[NUM LINES..\]`    Equivalent of 'docker-compose logs SERVICE'. To print a limited
                                    number of log messages, enter the number after the SERVICE.

 Load test commands:
    `us clean-load-test \[ROPTS\]`   Nuke and then run a long-running load test.
                                One-stop shop, nothing else needs to be run.
    `us load-test \[--up\] \[ROPTS\]`  Run a long-running load test, modify this file to change parameters
                                With --up: also run `uo :init:` and start all dependencies
    `us demo-load \[ROPTS\]`         Generate a lot of changes to be used in the demo

 ROPTS (Run Options):
     `uo --commit C`               Set both mz & peeker to 'unstable-C'
     `uo --tag T`                  Set both mz & peeker to 'T'
     `uo --mz-docker-tag T`        Set the tag for materialize/materialized:T
     `uo --mz-threads N`           Set mz -w/--threads to N
     `uo --peeker-docker-tag T`    Set the tag for materialize/peeker:T
     `uo --peeker-opts`            Set the peeker options, single string. Default: '-q loadtest'

 Helpers:
    `us web \(grafana\|metabase\|kafka\)`    Open service page in a web browser
    `us backup`                              Copy prometheus data into a tarball
    `us restore PATHGLOB`                    Unpack tarbal that matches PATHGLOB into prometheus dir

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
    echo "materialize and ingestion should be running fine, bringing up introspection and metabase"
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
    dc_run_wait_cmd \
            mysql \
            mysqlcli mysql --host=mysql --port=3306 --user=root --password=debezium -e 'select 1'
    runv docker-compose logs --tail 5 materialized
    runv docker-compose logs --tail 5 mysql
    dc_up connector
    echo "Waiting for schema registry to be fully up"
    sleep 5
    docker-compose logs --tail 5 schema-registry
}

bring_up_introspection() {
    mkdir -p prometheus/data
    chmod 777 prometheus/data
    dc_up grafana
}

bring_up_metabase() {
    dc_up metabase
}

# Create source data and tables for MYSQL
initialize_warehouse() {
    if ! (docker ps | grep chbench_mysql >/dev/null); then
        dc_up mysql
        dc_run_wait_cmd \
            mysql \
            mysqlcli mysql --host=mysql --port=3306 --user=root --password=debezium -e 'select 1'
    fi
    runv docker-compose run chbench gen --warehouses=1
}

# helper/individual step commands

# Start a single service and all its dependencies
dc_up() {
    parse_opts "$@"
    runv docker-compose up -d --build "${OPTIONS[@]}"
}

# Stop the named docker-compose service
#
# This can stop multiple services, but services started by 'docker-compose run -d'/'dc.sh
# run -d' must be stopped one at at time.
dc_stop() {
    if [[ $# -eq 1 ]]; then
        local run_cmd
        run_cmd=$(dc_is_run_cmd "$1")
        is_up=$(dc_is_running "$1")
        if [[ -n $run_cmd ]]; then
            runv docker stop "$run_cmd"
            return
        elif [[ -n $is_up ]]; then
            runv docker-compose stop "$1"
            return
        else
            echo "No running container"
            return
        fi
    fi
    runv docker-compose stop "$@"
}

dc_run() {
    runv docker-compose run --use-aliases --service-ports "$@"
}

dc_logs() {
  if [[ $# -eq 1 ]]; then
    local run_cmd
    run_cmd=$(dc_is_run_cmd "$1")
    if [[ -n $run_cmd ]]; then
        runv docker logs "$run_cmd"
    else
        runv docker-compose logs "$1"
    fi
  elif [[ $# -eq 2 ]]; then
    runv docker-compose logs --tail "$2" "$1"
  fi
}

dc_prom_backup() {
    if [[ $(dc_is_running prometheus) == prometheus ]]; then
        echo "stop prometheus before backing up"
        exit 1
    fi
    mkdir -p backups
    runv tar -czf "backups/dashboards-$NOW.tgz" "${BACKUP_DIRS[@]}"
}

dc_prom_restore() {
    if [[ $(dc_is_running prometheus) == prometheus ]]; then
        echo "stop prometheus before restoring"
        exit 1
    fi
    local glob="$1"
    # we specifically _do_ want to expand the glob pattern
    # shellcheck disable=SC2086
    runv tar -xzf $glob
    echo "restored directories from backup: ${BACKUP_DIRS[*]}"
}

dc_status() {
    dc_status_inner | column -s ' ' -t
}

dc_status_inner() {
    local procs
    # shellcheck disable=SC2207
    procs=($(dc_chbench_containers | sed -e 's/^chbench_//' -e 's/_1$//' | tr $'\n' ' ' ))
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

# Return the name of the container if the command looks like it was executed by way of 'run', instead of 'up'
dc_is_run_cmd() {
     ( dc_chbench_containers | grep -E "chbench_${1}_run_\w+" ) || true
}

dc_is_running() {
    ( dc_chbench_containers | grep -E "chbench_${1}" ) || true
}

dc_run_query() {
    local query=$1
    # shellcheck disable=SC2059
    (printf "$query\n" | docker-compose run cli psql -q -h materialized -p 6875 -d materialize) || true
}

dc_check_query() {
    local query=$1
    local timeout=$2
    for i in $(seq 0 "$timeout"); do
        local result
        result=$(dc_run_query "$query")
        if [[ -z $result ]]; then
            sleep 1
        else
            printf %s "$result"
            return
        fi
    done
    uw "query failed after $timeout attempts: $query"
    exit 1
}


dc_ensure_stays_up() {
    local container=$1
    local seconds="${2-5}"
    echo -n "ensuring $container is staying up for $seconds seconds: "
    for i in $(seq 1 "$seconds"); do
        sleep 1
        if [[ -z $(dc_is_running "$container") ]]; then
            echo
            uw "$container is not running!"
            exit 1
        fi
        echo -n "$i "
    done
    echo
}

dc_run_wait_cmd() {
    local service=$1 && shift
    echo -n "Waiting for $service to be up"
    while ! docker-compose run "$@" >/dev/null 2>&1; do
        echo -n '.'
        sleep 0.2
    done
    echo " ok"
}

# Get all the container names that belong to chbench
dc_chbench_containers() {
    ( docker ps --format '{{.Names}}' | grep '^chbench' ) || true
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
dc_nuke() {
    shut_down
    rm -rf prometheus/data/*
    runv docker system prune -af
    runv docker volume prune -f
}

clean_load_test() {
    parse_opts ERROR "$@"
    echo "$(uw WARNING:) nuking everything docker"
    for i in {5..1}; do
        echo -n "$i "
        sleep 1
    done
    echo "💥"
    dc_nuke
    load_test --up
}

# Purges Kafka topic and restarts Materialize/peeker
drop_kafka_topics() {
    dc_stop chbench
    dc_stop materialized peeker
    runv docker exec -it chbench_kafka_1 kafka-topics --delete --bootstrap-server localhost:9092 --topic "mysql.tpcch.*" || true
    dc_up materialized
}

ci() {
    bring_up_source_data
    drop_kafka_topics
    export MZ_IMG=materialize/ci-materialized:${BUILDKITE_BUILD_NUMBER}
    export PEEKER_IMG=materialize/ci-peeker:${BUILDKITE_BUILD_NUMBER}
    dc_run chbench gen --warehouses=1 --config-file-path=/etc/chbenchmark/mz-default.cfg
    dc_run -d chbench run \
        --dsn=mysql --gen-dir=/var/lib/mysql-files \
        --analytic-threads=0 --transactional-threads=1 --run-seconds=432000 \
        -l /dev/stdout --config-file-path=/etc/chbenchmark/mz-default.cfg \
        --mz-url=postgresql://materialized:6875/materialize?sslmode=disable
    dc_ensure_stays_up chbench 60
    dc_logs chbench
    dc_run -d peeker \
         --queries q01
    dc_ensure_stays_up peeker
    dc_status
    dc_check_query "\\pset format unaligned\nselect count(*) from q01" 20
}

# Long-running load test
load_test() {
    if [[ "${1:-}" = --up ]]; then
        shift
        initialize_warehouse
        bring_up_source_data
        bring_up_introspection
    fi
    parse_opts ERROR "$@"
    drop_kafka_topics
    dc_run chbench gen --warehouses=1 --config-file-path=/etc/chbenchmark/mz-default.cfg
    dc_run -d chbench run \
        --dsn=mysql --gen-dir=/var/lib/mysql-files \
        --analytic-threads=0 --transactional-threads=1 --run-seconds=432000 \
        -l /dev/stdout --config-file-path=/etc/chbenchmark/mz-default.cfg \
        --mz-url=postgresql://materialized:6875/materialize?sslmode=disable
    dc_ensure_stays_up chbench 20
    dc_logs chbench
    dc_run -d peeker \
         "${PEEKER_COMMAND[@]}"
    dc_ensure_stays_up peeker
    dc_status
}

# Generate changes for the demo
demo_load() {
    drop_kafka_topics
    dc_run -d chbench run \
        --dsn=mysql --gen-dir=/var/lib/mysql-files \
        --peek-conns=0 --flush-every=30 \
        --analytic-threads=0 --transactional-threads=1 --run-seconds=864000 \
        --min-delay=0.0 --max-delay=0.0 -l /dev/stdout \
        --config-file-path=/etc/chbenchmark/mz-default.cfg \
	--mz-url=postgresql://materialized:6875/materialize?sslmode=disable
    dc_run -d peeker --only-initialize --queries q01,q02,q17,q22
}

DCSH_LOCK=.dcsh-lock
ACQUIRED_LOCK=n
dc_lock() {
    local echoed_lock=
    while [[ -f "$DCSH_LOCK" ]]; do
        if [[ ${MZ_IN_AUTOMATION:-n} == y ]]; then
            echo "$(uw ERROR:) Refusing to wait for the chbench lock when run as part of automation"
            exit 10
        fi

        if [[ ! "$echoed_lock" ]]; then
            echo -n "waiting for lock on chbench directory"
            echoed_lock=y
        fi
        echo -n "."
        sleep 0.5
    done
    touch "$DCSH_LOCK"
    ACQUIRED_LOCK=y
}

dc_unlock() {
    if [[ $ACQUIRED_LOCK == y ]]; then
        rm -f "$DCSH_LOCK"
    fi
}
trap dc_unlock EXIT

main "$@"
