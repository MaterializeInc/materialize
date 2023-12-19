# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s
$ set-max-tries max-tries=3

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

#
# Validate feature-flag
#

! CREATE CONNECTION mysq TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )
contains: Create a MySQL connection or source is not supported

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_mysql_source = true

#
# Success create
#

> CREATE CONNECTION mysq TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

> SELECT name, type from mz_connections
name       type
------------------------------
mysq   mysql

> SHOW CREATE CONNECTION mysq
name   create_sql
---------------------------------
materialize.public.mysq   "CREATE CONNECTION \"materialize\".\"public\".\"mysq\" TO MYSQL (HOST = \"mysql\", PASSWORD = SECRET \"materialize\".\"public\".\"mysqlpass\", USER = \"root\")"

#
# Error checking
#

! CREATE CONNECTION no_such_host TO MYSQL (
    HOST 'no_such_mysql.mtrlz.com',
    USER root,
    PASSWORD SECRET mysqlpass
  )
contains:failed to lookup address information: Name or service not known

! CREATE CONNECTION no_such_port TO MYSQL (
    HOST mysql,
    PORT 65534,
    USER root,
    PASSWORD SECRET mysqlpass
  )
contains:Connection refused

> CREATE SECRET badpass AS 'badpass'
! CREATE CONNECTION no_such_password TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET badpass
  )
contains:Access denied for user 'root'
