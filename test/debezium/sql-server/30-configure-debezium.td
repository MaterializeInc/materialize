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

#
# We need to sleep here otherwise there is a race condition between taking
# the initial snapshot and the beginning of regular replication which causes
# the data from the initial snapshot to be replicated twice.
#

> SELECT mz_internal.mz_sleep(10)
<null>

$ http-request method=POST url=http://debezium:8083/connectors content-type=application/json
{
    "name": "sql-server-connector",
    "config": {
        "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
        "database.hostname": "sql-server",
        "database.port": "1433",
        "database.user": "sa",
        "database.password": "${arg.sa-password}",
        "database.dbname": "test",
        "database.server.name": "sql-server",
        "database.history.kafka.bootstrap.servers": "kafka:9092",
        "database.history.kafka.topic": "schema-changes.history",
        "snapshot.isolation.mode": "exclusive",
        "provide.transaction.metadata": "true"
#,
#        "signal.data.collection": "test.dbo.debezium_signal"
    }
}

# Sleep for 10 seconds, as Debezium may fail to replicate any
# statements that come immediately afterwards

> SELECT mz_internal.mz_sleep(10)
<null>
