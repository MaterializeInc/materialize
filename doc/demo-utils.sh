#!/usr/bin/env bash
# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

mtrz-startup() {
    confluent_log_dir="$(dirname "$(dirname "$(command which confluent)")")/logs"
    if [[ ! -w $confluent_log_dir ]] ; then
        echo -n "Confluent log dir is not writeable, press enter to set it up: $confluent_log_dir "
        read -r
        sudo mkdir -p "$confluent_log_dir"
        sudo chown "$USER" "$confluent_log_dir"
    fi

    if ( confluent status | grep DOWN ) ; then
        echo "Starting confluent services"
        confluent start
    else
        echo "confluent is running"
    fi
}

mtrz-cleardata() {
    local topic="$1"

    echo "clearing existing kafka topic"
    kafka-topics --zookeeper localhost:2181 --delete --topic "$topic" > /dev/null
    echo "clearing subject"
    curl -X DELETE "http://localhost:8081/subjects/${topic}-value"
}

mtrz-produce() {
    local topic="$1"
    local schema="$2"
    mtrz-cleardata "$topic"
    kafka-avro-console-producer \
        --topic "${topic}" \
        --broker-list localhost:9092 \
        --property value.schema="$schema"
}
