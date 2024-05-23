# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# We test interesting combinations of server and client SSL configs
# (part of the CREATE SOURCE statement).

$ set-sql-timeout duration=1s
$ set-max-tries max-tries=20

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_mysql_source = true

> CREATE SECRET ssl_ca AS '${arg.ssl-ca}'
> CREATE SECRET ssl_cert AS '${arg.ssl-client-cert}'
> CREATE SECRET ssl_key AS '${arg.ssl-client-key}'
> CREATE SECRET ssl_wrong_cert AS '${arg.ssl-wrong-client-cert}'
> CREATE SECRET ssl_wrong_key AS '${arg.ssl-wrong-client-key}'

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

# Bootstrap users and data
$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

DROP USER IF EXISTS host;
CREATE USER host;
GRANT ALL ON *.* TO host;

DROP USER IF EXISTS hostssl;
CREATE USER hostssl;
GRANT ALL ON *.* TO hostssl;

DROP USER IF EXISTS hostnossl;
CREATE USER hostnossl;
GRANT ALL ON *.* TO hostnossl;

DROP USER IF EXISTS certuser;
CREATE USER certuser;
GRANT ALL ON *.* TO certuser;

DROP TABLE IF EXISTS numbers;
CREATE TABLE numbers (number int PRIMARY KEY, is_prime bool, name text);

INSERT INTO numbers VALUES (1, true, 'one');

$ set-regex match=(\d{1,3}\.){3}\d{1,3} replacement=(HOST)

# server: host, client: disable => OK
> CREATE CONNECTION mysql_conn TO MYSQL (
  HOST mysql,
  USER host,
  SSL MODE disabled
  );
> CREATE SOURCE "mz_source"
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.numbers);

> SELECT * FROM "numbers";
1 1 one

$ mysql-execute name=mysql
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 1 one
2 1 two

> DROP SOURCE "mz_source" CASCADE;
> DROP CONNECTION mysql_conn;
$ mysql-execute name=mysql
DELETE FROM numbers WHERE number = 2;

# server: host, client: prefer => unsupported
! CREATE CONNECTION mysql_conn TO MYSQL (
  HOST mysql,
  USER host,
  SSL MODE prefer
  );
contains: invalid CONNECTION: unknown SSL MODE "PREFER"

# server: host, client: require => OK
> CREATE CONNECTION mysql_conn TO MYSQL (
  HOST mysql,
  USER host,
  SSL MODE required
  );
> CREATE SOURCE "mz_source"
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.numbers);

> SELECT * FROM "numbers";
1 1 one
$ mysql-execute name=mysql
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 1 one
2 1 two
> DROP SOURCE "mz_source" CASCADE;
> DROP CONNECTION mysql_conn;
$ mysql-execute name=mysql
DELETE FROM numbers WHERE number = 2;

# server: hostssl, client: require => OK
> CREATE CONNECTION mysql_conn TO MYSQL (
  HOST mysql,
  USER hostssl,
  SSL MODE required
  );
> CREATE SOURCE "mz_source"
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.numbers);
> SELECT * FROM "numbers";
1 1 one
$ mysql-execute name=mysql
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 1 one
2 1 two
> DROP SOURCE "mz_source" CASCADE;
> DROP CONNECTION mysql_conn;
$ mysql-execute name=mysql
DELETE FROM numbers WHERE number = 2;

# TODO: #25676 (error not handled properly)
# # server: hostssl, client: verify-ca => ERROR
# > CREATE CONNECTION mysql_conn TO MYSQL (
#   HOST mysql,
#   USER hostssl,
#   SSL MODE verify_ca
#   );
# ! CREATE SOURCE "mz_source"
#   FROM MYSQL CONNECTION mysql_conn
#   FOR TABLES (public.numbers);
# contains:self signed certificate in certificate chain
# > DROP CONNECTION mysql_conn;

# server: hostssl, client: verify-ca => OK
> CREATE CONNECTION mysql_conn TO MYSQL (
  HOST mysql,
  USER hostssl,
  SSL MODE verify_ca,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca
  );
> CREATE SOURCE "mz_source"
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.numbers);
> SELECT * FROM "numbers";
1 1 one
$ mysql-execute name=mysql
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 1 one
2 1 two
> DROP SOURCE "mz_source" CASCADE;
> DROP CONNECTION mysql_conn;
$ mysql-execute name=mysql
DELETE FROM numbers WHERE number = 2;

# TODO: #25676 (error not handled properly)
# # server: hostssl, client: verify-identity => ERROR
# > CREATE CONNECTION mysql_conn TO MYSQL (
#     HOST mysql,
#     USER hostssl,
#     SSL MODE verify_identity
#   );
# ! CREATE SOURCE "mz_source"
#   FROM MYSQL CONNECTION mysql_conn
#   FOR TABLES (public.numbers);
# contains:self signed certificate in certificate chain
# > DROP CONNECTION mysql_conn;

# TODO: #25676
# # server: hostssl, client: verify-identity => OK
# > CREATE CONNECTION mysql_conn TO MYSQL (
#   HOST mysql,
#   USER hostssl,
#   SSL MODE verify_identity,
#   SSL CERTIFICATE AUTHORITY SECRET ssl_ca
#   );
# > CREATE SOURCE "mz_source"
#   FROM MYSQL CONNECTION mysql_conn
#   FOR TABLES (public.numbers);
# > SELECT * FROM "numbers";
# 1 1 one
# $ mysql-execute name=mysql
# INSERT INTO numbers VALUES (2, true, 'two');
# > SELECT * FROM "numbers";
# 1 1 one
# 2 1 two
# > DROP SOURCE "mz_source" CASCADE;
# > DROP CONNECTION mysql_conn;
# $ mysql-execute name=mysql
# DELETE FROM numbers WHERE number = 2;

# server: hostnossl, client: disable => OK
> CREATE CONNECTION mysql_conn TO MYSQL (
  HOST mysql,
  USER hostnossl,
  SSL MODE disabled
  );
> CREATE SOURCE "mz_source"
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.numbers);
> SELECT * FROM "numbers";
1 1 one
$ mysql-execute name=mysql
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 1 one
2 1 two
> DROP SOURCE "mz_source" CASCADE;
> DROP CONNECTION mysql_conn;
$ mysql-execute name=mysql
DELETE FROM numbers WHERE number = 2;

# TODO: #25676 (error not handled properly)
# # server: hostnossl, client: verify-ca => ERROR
# > CREATE CONNECTION mysql_conn TO MYSQL (
#   HOST mysql,
#   USER hostnossl,
#   SSL MODE verify_ca
#   );
# ! CREATE SOURCE "mz_source"
#   FROM MYSQL CONNECTION mysql_conn
#   FOR TABLES (public.numbers);
# contains:self signed certificate in certificate chain
# > DROP CONNECTION mysql_conn;

# server: certuser, client: require => OK
> CREATE CONNECTION mysql_conn TO MYSQL (
  HOST mysql,
  USER certuser,
  SSL MODE required,
  SSL CERTIFICATE SECRET ssl_cert,
  SSL KEY SECRET ssl_key,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca
  );
> CREATE SOURCE "mz_source"
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.numbers);
> SELECT * FROM "numbers";
1 1 one
$ mysql-execute name=mysql
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 1 one
2 1 two
> DROP SOURCE "mz_source" CASCADE;
> DROP CONNECTION mysql_conn;
$ mysql-execute name=mysql
DELETE FROM numbers WHERE number = 2;

# server: certuser, client: verify-ca => OK in MySQL
> CREATE CONNECTION mysql_conn TO MYSQL (
  HOST mysql,
  USER certuser,
  SSL MODE verify_ca,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca
  );
> CREATE SOURCE "mz_source"
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.numbers);
> SELECT * FROM "numbers";
1 1 one
$ mysql-execute name=mysql
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 1 one
2 1 two
> DROP SOURCE "mz_source" CASCADE;
> DROP CONNECTION mysql_conn;
$ mysql-execute name=mysql
DELETE FROM numbers WHERE number = 2;

# TODO: #25676 (error not handled properly)
# # server: certuser, client: verify-ca (wrong cert) => ERROR
# > CREATE CONNECTION mysql_conn TO MYSQL (
#   HOST mysql,
#   USER certuser,
#   SSL MODE verify_ca,
#   SSL CERTIFICATE SECRET ssl_wrong_cert,
#   SSL KEY SECRET ssl_wrong_key,
#   SSL CERTIFICATE AUTHORITY SECRET ssl_ca
#   );
# ! CREATE SOURCE "mz_source"
#   FROM MYSQL CONNECTION mysql_conn
# contains:db error: FATAL: certificate authentication failed for user "certuser"
# > DROP CONNECTION mysql_conn;

# server: certuser, client: verify-ca => OK
> CREATE CONNECTION mysql_conn TO MYSQL (
  HOST mysql,
  USER certuser,
  SSL MODE verify_ca,
  SSL CERTIFICATE SECRET ssl_cert,
  SSL KEY SECRET ssl_key,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca
  );
> CREATE SOURCE "mz_source"
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.numbers);
> SELECT * FROM "numbers";
1 1 one
$ mysql-execute name=mysql
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 1 one
2 1 two
> DROP SOURCE "mz_source" CASCADE;
> DROP CONNECTION mysql_conn;
$ mysql-execute name=mysql
DELETE FROM numbers WHERE number = 2;

# TODO: #25676 (error not handled properly)
# # server: certuser, client: verify-identity => OK
# > CREATE CONNECTION mysql_conn TO MYSQL (
#   HOST mysql,
#   USER certuser,
#   SSL MODE verify_identity,
#   SSL CERTIFICATE SECRET ssl_cert,
#   SSL KEY SECRET ssl_key,
#   SSL CERTIFICATE AUTHORITY SECRET ssl_ca
#   );
# > CREATE SOURCE "mz_source"
#   FROM MYSQL CONNECTION mysql_conn
#   FOR TABLES (public.numbers);
# > SELECT * FROM "numbers";
# 1 1 one
# $ mysql-execute name=mysql
# INSERT INTO numbers VALUES (2, true, 'two');
# > SELECT * FROM "numbers";
# 1 1 one
# 2 1 two
# > DROP SOURCE "mz_source" CASCADE;
# > DROP CONNECTION mysql_conn;
# $ mysql-execute name=mysql
# DELETE FROM numbers WHERE number = 2;

# missing sslcert
! CREATE CONNECTION mysql_conn TO MYSQL (
  HOST mysql,
  USER certuser,
  SSL MODE verify_full,
  SSL CERTIFICATE SECRET noexist,
  SSL KEY SECRET ssl_key,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca
  );
contains:unknown catalog item 'noexist'

# missing sslkey
! CREATE CONNECTION mysql_conn TO MYSQL (
  HOST mysql,
  USER certuser,
  SSL MODE verify_full,
  SSL CERTIFICATE SECRET ssl_cert,
  SSL KEY SECRET noexist,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca
  );
contains:unknown catalog item 'noexist'

# missing sslrootcert
! CREATE CONNECTION mysql_conn TO MYSQL (
  HOST mysql,
  USER certuser,
  SSL MODE verify_full,
  SSL CERTIFICATE SECRET ssl_cert,
  SSL KEY SECRET ssl_key,
  SSL CERTIFICATE AUTHORITY SECRET noexist
  );
contains:unknown catalog item 'noexist'

# require both sslcert and sslkey
! CREATE CONNECTION mysql_conn TO MYSQL (
  HOST mysql,
  USER certuser,
  SSL MODE verify_full,
  SSL CERTIFICATE SECRET ssl_cert
  );
contains:invalid CONNECTION: both SSL KEY and SSL CERTIFICATE are required

! CREATE CONNECTION mysql_conn TO MYSQL (
  HOST mysql,
  USER certuser,
  SSL MODE verify_full,
  SSL KEY SECRET ssl_cert
  );
contains:invalid CONNECTION: both SSL KEY and SSL CERTIFICATE are required
