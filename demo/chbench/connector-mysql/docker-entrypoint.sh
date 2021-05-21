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
    debezium.tpcch.customer
    debezium.tpcch.district
    debezium.tpcch.history
    debezium.tpcch.item
    debezium.tpcch.nation
    debezium.tpcch.neworder
    debezium.tpcch.order
    debezium.tpcch.orderline
    debezium.tpcch.region
    debezium.tpcch.stock
    debezium.tpcch.supplier
    debezium.tpcch.warehouse
)

wait-for-it --timeout=60 connect:8083

echo "${topics[@]}" | xargs -n1 -P8 kafka-topics --bootstrap-server kafka:9092 --create --partitions 1 --replication-factor 1 --topic


curl -H 'Content-Type: application/json' connect:8083/connectors --data '{
  "name": "mysql-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz",
    "database.server.name": "debezium",
    "database.server.id": "1234",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "database.history.kafka.topic": "mysql-history",
    "provide.transaction.metadata": "true",
    "time.precision.mode": "connect"
 }
}'
