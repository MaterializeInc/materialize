# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s
$ set-max-tries max-tries=20

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true

$ set count=10000

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE t1 (pk SERIAL PRIMARY KEY, f2 BIGINT);

SET @i:=0;
INSERT INTO t1 (f2) SELECT @i:=@i+1 FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT ${count};

# Create a cluster with no replicas so that we have time to submit queries at the minimum frontier.
> CREATE CLUSTER storage REPLICAS ()

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

> CREATE SOURCE mz_source
  IN CLUSTER storage
  FROM MYSQL CONNECTION mysql_conn
  FOR ALL TABLES
  WITH (RETAIN HISTORY = FOR '365000 days');

# Grab a cursor at timestamp 0
> BEGIN

> DECLARE c CURSOR FOR SELECT 1, COUNT(*) FROM t1 AS OF 0

# Start ingestion by adding a replica to the cluster. We must do this from a
# different connection to not disturb the transaction we're in.
$ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
CREATE CLUSTER REPLICA storage.r1 SIZE = '1';

# Verify that at timestamp 0 there is only one record whose value is the final value
> FETCH 1 c WITH (timeout = '1d');
1 ${count}

> COMMIT
