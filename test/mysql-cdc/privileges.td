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
detail:missing schemas: other

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
detail:missing schemas: other

$ mysql-execute name=mysql
CREATE TABLE other.u (a int);
CREATE TABLE other.access_not_granted (a int);
CREATE TABLE other.`select` (a INT);
CREATE TABLE other.`"select"` (a INT);
# non-complete privileges on at least one table in schema 'other' are now present
GRANT SELECT ON other.u TO priv;
GRANT INDEX ON other.s TO priv;

# A new error since the tables are now visible but we are missing some privileges
! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR SCHEMAS (other);
contains:User lacks required MySQL privileges
detail:Missing MySQL privileges: 'LOCK TABLES' on 'other.s', 'SELECT' on 'other.s', 'LOCK TABLES' on 'other.u', 'REPLICATION SLAVE' on '*.*'


# fix table privileges, use wildcard for LOCK TABLES
$ mysql-execute name=mysql
GRANT SELECT ON other.s TO priv;
GRANT LOCK TABLES ON other.* TO priv;

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR SCHEMAS (other);
contains:User lacks required MySQL privileges
detail:Missing MySQL privileges: 'REPLICATION SLAVE' on '*.*'

$ mysql-execute name=mysql
GRANT REPLICATION SLAVE ON *.* TO priv;

> CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR SCHEMAS (other);

# confirm the source works
$ mysql-execute name=mysql
USE other;
INSERT INTO u VALUES (2), (3), (4);

> SELECT * FROM u;
2
3
4

# Verify privileges provided via default roles

$ mysql-execute name=mysql
CREATE ROLE 'r1';
GRANT 'r1' TO 'priv';
SET DEFAULT ROLE 'r1' TO 'priv';
USE other;
CREATE TABLE other.z (a int);
INSERT INTO other.z VALUES (1);
GRANT INDEX ON other.z TO 'priv';

! CREATE SOURCE mz_source_other
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (other.z);
contains:User lacks required MySQL privileges
detail:Missing MySQL privileges: 'SELECT' on 'other.z'

# grant the privilege to the role not the user directly
$ mysql-execute name=mysql
GRANT SELECT ON other.z TO 'r1';

> CREATE SOURCE mz_source_other
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (other.z);

> SELECT * FROM z;
1

# cleanup the 'other' database to avoid issues in other testdrive files
> DROP SOURCE mz_source CASCADE;
> DROP SOURCE mz_source_other CASCADE;

$ mysql-execute name=mysql
DROP DATABASE other;
