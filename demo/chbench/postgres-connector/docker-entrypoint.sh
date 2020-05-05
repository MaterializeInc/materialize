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

wait-for-it --timeout=60 zookeeper:2181
wait-for-it --timeout=60 kafka:9092

topics=(
    debezium.tpcch.warehouse
    debezium.tpcch.district
    debezium.tpcch.customer
    debezium.tpcch.history
    debezium.tpcch.neworder
    debezium.tpcch.order
    debezium.tpcch.orderline
    debezium.tpcch.item
    debezium.tpcch.stock
    debezium.tpcch.nation
    debezium.tpcch.supplier
    debezium.tpcch.region
)

wait-for-it --timeout=60 connect:8083
wait-for-it --timeout=60 postgres:5432

echo "${topics[@]}" | xargs -n1 -P8 kafka-topics --bootstrap-server kafka:9092 --create --partitions 1 --replication-factor 1 --topic


curl -H 'Content-Type: application/json'  connect:8083/connectors --data '{
  "name": "psql-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "postgres",
    "database.server.name": "debezium",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "include.schema.changes": "true",
    "database.history.kafka.topic": "psql-history",
    "provide.transaction.metadata": "true",
    "time.precision.mode": "connect"
    }
}'
