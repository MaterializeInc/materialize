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

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="10s"

#
# Wait until SQL Server CDC is actually live before starting the Debezium
# connector. Enabling CDC (sp_cdc_enable_db / sp_cdc_enable_table) only creates
# the capture instances; sys.fn_cdc_get_max_lsn() stays NULL until the SQL
# Server Agent capture job has run its first log scan. If Debezium starts before
# then it finds "No maximum LSN recorded in the database", fails to take its
# initial snapshot, and never publishes any schemas, so the 40-check-*.td files
# time out in schema-registry-wait (database-issues#7026). The committed inserts
# from the 20-populate-*.td files guarantee the max LSN becomes non-NULL once the
# capture job runs.
#

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=sa;Password=${arg.sa-password}

$ sql-server-execute name=sql-server split-lines=false
USE test;
DECLARE @attempts INT = 0;
WHILE sys.fn_cdc_get_max_lsn() IS NULL AND @attempts < 120
BEGIN
    WAITFOR DELAY '00:00:01';
    SET @attempts = @attempts + 1;
END;
IF sys.fn_cdc_get_max_lsn() IS NULL
    RAISERROR('SQL Server CDC is not live: sys.fn_cdc_get_max_lsn() still NULL after 120s; is the SQL Server Agent capture job running?', 16, 1);

$ http-request method=POST url=http://debezium:8083/connectors content-type=application/json
{
    "name": "sql-server-connector",
    "config": {
        "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
        "database.hostname": "sql-server",
        "database.encrypt": "false",
        "database.port": "1433",
        "database.user": "sa",
        "database.password": "${arg.sa-password}",
        "database.names": "test",
        "database.server.name": "sql-server",
        "database.history.kafka.bootstrap.servers": "kafka:9092",
        "database.history.kafka.topic": "schema-changes.history",
        "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
        "schema.history.internal.kafka.topic": "schemahistory.sql-server",
        "snapshot.isolation.mode": "exclusive",
        "provide.transaction.metadata": "true",
        "topic.prefix": "sql_server"
#,
#        "signal.data.collection": "test.dbo.debezium_signal"
    }
}

# Sleep for 10 seconds, as Debezium may fail to replicate any
# statements that come immediately afterwards

$ sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment duration="10s"
