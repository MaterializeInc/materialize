# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that a MySQL source fails when enforce_gtid_consistency is disabled
# after the source has already connected and begun replication.

$ set-sql-timeout duration=60s

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
SET GLOBAL enforce_gtid_consistency = ON;
SET GLOBAL gtid_mode = ON;

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;
CREATE TABLE t1 (f1 INTEGER PRIMARY KEY);
INSERT INTO t1 VALUES (1);

> CREATE CLUSTER gtid_cluster SIZE '${arg.default-replica-size}';

> BEGIN
> CREATE SOURCE mz_source IN CLUSTER gtid_cluster FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);
> COMMIT

> SELECT * FROM t1;
1

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
running

# Disable enforce_gtid_consistency on the MySQL server.
# MySQL allows transitioning from ON to WARN while gtid_mode=ON.
$ mysql-execute name=mysql
SET GLOBAL gtid_mode = ON_PERMISSIVE;
SET GLOBAL enforce_gtid_consistency = WARN;

# Force the source to reconnect by cycling the cluster replication factor,
# which causes the replication reader to restart and re-validate MySQL settings.
> ALTER CLUSTER gtid_cluster SET (REPLICATION FACTOR 0)
> ALTER CLUSTER gtid_cluster SET (REPLICATION FACTOR 1)

# The source should now be stalled due to the invalid enforce_gtid_consistency setting.
> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
stalled

> SELECT error ILIKE '%gtid_mode%' FROM mz_internal.mz_source_statuses WHERE name = 'mz_source';
true

> DROP CLUSTER gtid_cluster CASCADE;

$ mysql-execute name=mysql
SET GLOBAL enforce_gtid_consistency = ON;
SET GLOBAL gtid_mode = ON;