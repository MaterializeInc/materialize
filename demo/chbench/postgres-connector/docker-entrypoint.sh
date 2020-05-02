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
wait-for-it --timeout=60 postgres:5432

curl -H 'Content-Type: application/json'  connect:8083/connectors --data '{
  "name": "psql-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "postgres",
    "database.server.name": "dbserver1",
    "database.history.kafka.bootstrap.servers": "kafka:9092",
    "include.schema.changes": "true",
    "database.history.kafka.topic": "psql-history",
    "time.precision.mode": "connect"
   }
}
