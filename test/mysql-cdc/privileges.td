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
ALTER SYSTEM SET enable_mysql_source = true

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE USER priv IDENTIFIED BY 'priv';

DROP SCHEMA IF EXISTS other;
CREATE SCHEMA other;
CREATE TABLE other.s (a int);
# do not grant any privileges to priv

CREATE TABLE public.t (a int);
# do not grant any privileges to priv

# needed for creating a source
GRANT SELECT ON performance_schema.replication_connection_configuration TO priv

# needed to keep the source up to date
GRANT REPLICATION SLAVE ON *.* TO priv

#
# no CONNECT error
#

# no connect privilege exists in MySQL, a user can always connect to MySQL (but not necessarily to the schema)
> CREATE SECRET mysqlpass AS 'priv'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER priv,
    PASSWORD SECRET mysqlpass
  )

#
# USAGE error
#

$ mysql-execute name=mysql
GRANT ALL ON public.* TO priv;
# still no privileges on schema other


! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR SCHEMAS (public, other);
contains:No tables found in referenced schemas

# TODO: #25412 (more error details when lacking permissions)
# detail:user priv lacks USAGE privileges for schemas other

#
# SELECT errors
#

$ mysql-execute name=mysql
# Note that it is not possible in MySQL to grant permissions on a schema and revoke them on a single table.
# Grant permissions only on the schema 'other' but not on table 's' in it.
GRANT ALL ON other TO priv;

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR SCHEMAS (public, other);
contains:No tables found in referenced schemas

$ mysql-execute name=mysql
CREATE TABLE other.u (a int);
CREATE TABLE other.access_not_granted (a int);
CREATE TABLE other.`select` (a INT);
CREATE TABLE other.`"select"` (a INT);
# privileges on at least one table in schema 'other' are now present
GRANT SELECT ON other.u TO priv;

$ mysql-execute name=mysql

# This succeeds because at least one table in the schema is accessible.
> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR SCHEMAS (other);

! SELECT * FROM access_not_granted;
contains:unknown catalog item 'access_not_granted'
