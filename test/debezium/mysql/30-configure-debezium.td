# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Note that this file is *not* reentrant. Debezium does not allow
# error-free dropping of objects that may or may not exist
# using the same set of commands for all cases.
#
# Furthermore, doing a couple of REST calls against the same Debezium
# connector is an easy way to bork it, so please always do
#
# ./mzcompose -down v
#
# before running this test framework again.
#

$ http-request method=POST url=http://debezium:8083/connectors content-type=application/json
{
    "name": "mysql-connector",
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "database.hostname": "mysql",
        "database.port": "3306",
        "database.user": "root",
        "database.password": "${arg.mysql-root-password}",
        "database.server.id": "1234",
        "database.server.name": "mysql",
        "database.include.list": "test",
        "database.history.kafka.bootstrap.servers": "kafka:9092",
        "database.history.kafka.topic": "dbhistory.mysql",
        "include.schema.changes": "true"
    }
}
