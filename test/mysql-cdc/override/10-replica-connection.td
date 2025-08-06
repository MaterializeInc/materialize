# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'


$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-connect name=mysql-replica url=mysql://root@mysql-replica password=${arg.mysql-root-password}

#
# Create some data on the primary
# and use RESET ... to clear any previous state
# from the binlogs that may slow down the replication
# process on the replica and cause us to be in an unknown
# state when we create our source.
#
$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
RESET BINARY LOGS AND GTIDS;
CREATE DATABASE public;
USE public;
CREATE TABLE t1 (f1 INTEGER);
INSERT INTO t1 VALUES (1);

#
# Create a connection from MZ to the replica
#
> CREATE CONNECTION mysq_replica TO MYSQL (
    HOST 'mysql-replica',
    USER root,
    PASSWORD SECRET mysqlpass
  )

#
# Turn off the required 'replica_preserve_commit_order' setting for this replica
# and start replication on the replica
#
$ mysql-execute name=mysql-replica
STOP REPLICA;
SET GLOBAL replica_preserve_commit_order=OFF;
CHANGE REPLICATION SOURCE TO SOURCE_HOST='mysql', SOURCE_PORT=3306, SOURCE_USER='root', SOURCE_PASSWORD='${arg.mysql-root-password}', SOURCE_AUTO_POSITION=1;
START REPLICA;

#
# Now try creating a source for this replica, and we should hit an error
#
! CREATE SOURCE replica_source FROM MYSQL CONNECTION mysq_replica;
contains:Invalid MySQL system replication settings

#
# Now fix the setting on the mysql replica
#
$ mysql-execute name=mysql-replica
STOP REPLICA;
SET GLOBAL replica_preserve_commit_order=ON;
START REPLICA;

#
# Let the replica catch up to the primary
#
> SELECT mz_unsafe.mz_sleep(3)
<null>

#
# Validate we can now create a source to this replica
#
> BEGIN
> CREATE SOURCE replica_source FROM MYSQL CONNECTION mysq_replica;
> CREATE TABLE t1 FROM SOURCE replica_source (REFERENCE public.t1);
> COMMIT
