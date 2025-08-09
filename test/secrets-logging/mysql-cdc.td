# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
CREATE TABLE t (f1 TEXT);
INSERT INTO t VALUES ('ghp_9fK8sL3x7TqR1vEzYm2pDaN4WjXbQzUtV0aN');

> BEGIN
> CREATE SOURCE mysql_source
  FROM MYSQL CONNECTION mysql_conn;

> CREATE TABLE t FROM SOURCE mysql_source (REFERENCE public.t);
> COMMIT

> SELECT * FROM t
ghp_9fK8sL3x7TqR1vEzYm2pDaN4WjXbQzUtV0aN
