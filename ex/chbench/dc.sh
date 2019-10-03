#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

set -ueo pipefail

main() {
    local arg=${1-} && shift
    case "$arg" in
        up) bring_up ;;
        down) shut_down ;;
        *) usage ;;
    esac
}

dc_up() {
    docker-compose up -d --build "$@"
}

bring_up() {
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
    docker-compose down
}

main "$@"
