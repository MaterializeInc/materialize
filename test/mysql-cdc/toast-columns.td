# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s


#
# Test that we can ingest unchanged toasted values
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

# Insert data pre-snapshot by generating 16kB of uncompressible data to force
# TOASTed storage by concatenating 1024 random MD5 hashes, each being 128bit
$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE t1 (a int, b text);

SET SESSION group_concat_max_len = 32768;

# necessary because the limit is not respected by group_concat
CREATE TABLE temp_rand (rand_value TEXT);
INSERT INTO temp_rand SELECT rand() FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 1024;

INSERT INTO t1 SELECT 1, group_concat(md5(rand_value) SEPARATOR '') FROM temp_rand;

DELETE FROM temp_rand;
INSERT INTO temp_rand SELECT rand() FROM mysql.time_zone t1, mysql.time_zone t2 LIMIT 1024;
INSERT INTO t1 SELECT 2, group_concat(md5(rand_value) SEPARATOR '') FROM temp_rand;

DROP TABLE temp_rand;

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE public.t1);

> SELECT a, length(b) FROM t1;
1 32768
2 32768

# Update the rows without touching the TOASTed column
$ mysql-execute name=mysql
UPDATE t1 SET a = 3;

> SELECT a, length(b) FROM t1;
3 32768
3 32768
