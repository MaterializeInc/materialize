#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

set -euo pipefail

wait-for-it --timeout=60 zookeeper:2181
wait-for-it --timeout=60 kafka:9092

topics=(
    mysql.tpcch.warehouse
    mysql.tpcch.district
    mysql.tpcch.customer
    mysql.tpcch.history
    mysql.tpcch.neworder
    mysql.tpcch.order
    mysql.tpcch.orderline
    mysql.tpcch.item
    mysql.tpcch.stock
    mysql.tpcch.nation
    mysql.tpcch.supplier
    mysql.tpcch.region
)

echo "${topics[@]}" | xargs -n1 -P8 kafka-topics --bootstrap-server kafka:9092 --create --partitions 1 --replication-factor 1 --topic

wait-for-it --timeout=60 connect:8083
wait-for-it --timeout=60 mysql:3306

curl -H 'Content-Type: application/json' connect:8083/connectors --data '{
  "name": "mysql-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz",
    "database.server.name": "mysql",
    "database.server.id": "1234",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "database.history.kafka.topic": "mysql-history",
    "time.precision.mode": "connect"
  }
}'
