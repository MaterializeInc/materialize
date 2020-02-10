#!/usr/bin/env bash

# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

_MTRLZ_LOGFILE="$(pwd)/.mtrlz.log"
_MTRLZ_PORT=6875

_log() {
    echo "$@"
    echo "$(date "+%Y-%m-%dT%H:%M:%S")" "$@" >> "$_MTRLZ_LOGFILE"
}

mtrlz-start() {
    confluent_log_dir="$(dirname "$(dirname "$(command which confluent)")")/logs"
    if [[ ! -w $confluent_log_dir ]] ; then
        echo -n "Confluent log dir is not writeable, press enter to set it up: $confluent_log_dir "
        read -r
        sudo mkdir -p "$confluent_log_dir"
        sudo chown "$USER" "$confluent_log_dir"
    fi

    echo "Ensuring confluent services are running"
    confluent local start connect 2>&1 | tee "$_MTRLZ_LOGFILE"
    if (confluent local status | grep -E '(connect|schema-registry|kafka|zookeepr).*DOWN\]') ; then
        echo "Restarting all confluent services"
        confluent local stop
        confluent local start connect
    fi
    if ! (pgrep 'materialized' >/dev/null) ; then
        echo "Starting materialized"
        if [[ -z "$MTRLZ_DEBUG" ]] ; then
            cargo build --release --bin materialized && ./target/release/materialized
        else
            cargo build --bin materialized && ./target/debug/materialized
        fi
    else
        echo "materialized is already running!"
    fi
}

_mtrlz-cleardata() {
    local topic="$1"

    _log "clearing existing kafka topic $topic"
    kafka-topics --zookeeper localhost:2181 --delete --topic "$topic" >> "$_MTRLZ_LOGFILE" 2>&1
    _log "clearing subject ${topic}-value"
    curl -X DELETE "http://localhost:8081/subjects/${topic}-value" >> "$_MTRLZ_LOGFILE" 2>&1
    echo >> "$_MTRLZ_LOGFILE"
}

mtrlz-produce() {
    local topic="$1"
    local schema="$2"
    _mtrlz-cleardata "$topic"
    echo "🚀 You are now in the avro console shell, enter your json events:"
    kafka-avro-console-producer \
        --topic "${topic}" \
        --broker-list localhost:9092 \
        --property value.schema="$schema"
}

mtrlz-shell() {
    if ! (lsof -i ":$_MTRLZ_PORT" | grep LISTEN >/dev/null) ; then
        echo "you must start materialize with 'mtrlz-start'"
        return
    fi
    psql -h localhost -p "$_MTRLZ_PORT" sslmode=disable
}
