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
#
# The important bit is that each user is named after the record type
# in `pg_hba.conf`. The record type indicates what kind of connection
# is allowed, e.g. `host` allows SSL and plaintext whereas `hostssl`
# only allows SSL.
#
# Check out https://www.postgresql.org/docs/13/auth-pg-hba-conf.html
# for more details.

> CREATE SECRET ssl_ca AS '${arg.ssl-ca}'
> CREATE SECRET ssl_cert AS '${arg.ssl-cert}'
> CREATE SECRET ssl_key AS '${arg.ssl-key}'
> CREATE SECRET ssl_wrong_cert AS '${arg.ssl-wrong-cert}'
> CREATE SECRET ssl_wrong_key AS '${arg.ssl-wrong-key}'

# Bootstrap users and data
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP USER IF EXISTS host;
CREATE USER host LOGIN SUPERUSER;

DROP USER IF EXISTS hostssl;
CREATE USER hostssl LOGIN SUPERUSER;

DROP USER IF EXISTS hostnossl;
CREATE USER hostnossl LOGIN SUPERUSER;

DROP USER IF EXISTS certuser;
CREATE USER certuser LOGIN SUPERUSER;

DROP TABLE IF EXISTS numbers;
CREATE TABLE numbers (number int PRIMARY KEY, is_prime bool, name text);
ALTER TABLE numbers REPLICA IDENTITY FULL;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

INSERT INTO numbers VALUES (1, true, 'one');

$ set-regex match=(\d{1,3}\.){3}\d{1,3} replacement=(HOST)

# server: host, client: disable => OK
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER host,
  SSL MODE disable,
  DATABASE postgres;
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source';
> CREATE VIEWS FROM SOURCE "mz_source" ("numbers")
> SELECT * FROM "numbers";
1 true one
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 true one
2 true two
> DROP VIEW "numbers";
> DROP SOURCE "mz_source";
> DROP CONNECTION pgconn;
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: host, client: prefer => unsupported
! CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER host,
  SSL MODE prefer,
  DATABASE postgres;
contains: invalid CONNECTION: unknown SSL MODE "prefer"

# server: host, client: require => OK
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER host,
  SSL MODE require,
  DATABASE postgres;
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source';
> CREATE VIEWS FROM SOURCE "mz_source" ("numbers")
> SELECT * FROM "numbers";
1 true one
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 true one
2 true two
> DROP VIEW "numbers";
> DROP SOURCE "mz_source";
> DROP CONNECTION pgconn;
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: hostssl, client: disable => ERROR
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER hostssl,
  SSL MODE disable,
  DATABASE postgres;
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source';
contains:db error: FATAL: no pg_hba.conf entry for host "(HOST)", user "hostssl", database "postgres", SSL off
> DROP CONNECTION pgconn;

# server: hostssl, client: require => OK
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER hostssl,
  SSL MODE require,
  DATABASE postgres;
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source';
> CREATE VIEWS FROM SOURCE "mz_source" ("numbers")
> SELECT * FROM "numbers";
1 true one
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 true one
2 true two
> DROP VIEW "numbers";
> DROP SOURCE "mz_source";
> DROP CONNECTION pgconn;
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: hostssl, client: verify-ca => ERROR
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER hostssl,
  SSL MODE verify_ca,
  DATABASE postgres;
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source';
contains:self signed certificate in certificate chain
> DROP CONNECTION pgconn;

# server: hostssl, client: verify-ca => OK
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER hostssl,
  SSL MODE verify_ca,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca,
  DATABASE postgres;
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source';
> CREATE VIEWS FROM SOURCE "mz_source" ("numbers")
> SELECT * FROM "numbers";
1 true one
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 true one
2 true two
> DROP VIEW "numbers";
> DROP SOURCE "mz_source";
> DROP CONNECTION pgconn;
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: hostssl, client: verify-full => ERROR
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER hostssl,
  SSL MODE verify_full,
  DATABASE postgres;
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source';
contains:self signed certificate in certificate chain
> DROP CONNECTION pgconn;

# server: hostssl, client: verify-full => OK
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER hostssl,
  SSL MODE verify_full,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca,
  DATABASE postgres;
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source';
> CREATE VIEWS FROM SOURCE "mz_source" ("numbers")
> SELECT * FROM "numbers";
1 true one
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 true one
2 true two
> DROP VIEW "numbers";
> DROP SOURCE "mz_source";
> DROP CONNECTION pgconn;
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: hostnossl, client: disable => OK
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER hostnossl,
  SSL MODE disable,
  DATABASE postgres;
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source';
> CREATE VIEWS FROM SOURCE "mz_source" ("numbers")
> SELECT * FROM "numbers";
1 true one
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 true one
2 true two
> DROP VIEW "numbers";
> DROP SOURCE "mz_source";
> DROP CONNECTION pgconn;
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: hostnossl, client: require => ERROR
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER hostnossl,
  SSL MODE require,
  DATABASE postgres;
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source';
contains:db error: FATAL: no pg_hba.conf entry for host "(HOST)", user "hostnossl", database "postgres", SSL on
> DROP CONNECTION pgconn;

# server: hostnossl, client: verify-ca => ERROR
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER hostnossl,
  SSL MODE verify_ca,
  DATABASE postgres;
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source';
contains:self signed certificate in certificate chain
> DROP CONNECTION pgconn;

# server: hostnossl, client: verify-full => ERROR
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER hostnossl,
  SSL MODE verify_full,
  DATABASE postgres;
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source';
contains:self signed certificate in certificate chain
> DROP CONNECTION pgconn;

# server: certuser, client: require => OK
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER certuser,
  SSL MODE require,
  SSL CERTIFICATE SECRET ssl_cert,
  SSL KEY SECRET ssl_key,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca,
  DATABASE postgres;
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source'
> CREATE VIEWS FROM SOURCE "mz_source" ("numbers")
> SELECT * FROM "numbers";
1 true one
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 true one
2 true two
> DROP VIEW "numbers";
> DROP SOURCE "mz_source";
> DROP CONNECTION pgconn;
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: certuser, client: verify-ca => ERROR
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER certuser,
  SSL MODE verify_ca,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca,
  DATABASE postgres;
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source'
contains:db error: FATAL: connection requires a valid client certificate
> DROP CONNECTION pgconn;

# server: certuser, client: verify-ca (wrong cert) => ERROR
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER certuser,
  SSL MODE verify_ca,
  SSL CERTIFICATE SECRET ssl_wrong_cert,
  SSL KEY SECRET ssl_wrong_key,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca,
  DATABASE postgres;
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source'
contains:db error: FATAL: certificate authentication failed for user "certuser"
> DROP CONNECTION pgconn;

# server: certuser, client: verify-ca => OK
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER certuser,
  SSL MODE verify_ca,
  SSL CERTIFICATE SECRET ssl_cert,
  SSL KEY SECRET ssl_key,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca,
  DATABASE postgres;
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source'
> CREATE VIEWS FROM SOURCE "mz_source" ("numbers")
> SELECT * FROM "numbers";
1 true one
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 true one
2 true two
> DROP VIEW "numbers";
> DROP SOURCE "mz_source";
> DROP CONNECTION pgconn;
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: certuser, client: verify-full => OK
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER certuser,
  SSL MODE verify_full,
  SSL CERTIFICATE SECRET ssl_cert,
  SSL KEY SECRET ssl_key,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca,
  DATABASE postgres;
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION pgconn
  PUBLICATION 'mz_source'
> CREATE VIEWS FROM SOURCE "mz_source" ("numbers")
> SELECT * FROM "numbers";
1 true one
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
INSERT INTO numbers VALUES (2, true, 'two');
> SELECT * FROM "numbers";
1 true one
2 true two
> DROP VIEW "numbers";
> DROP SOURCE "mz_source";
> DROP CONNECTION pgconn;
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# missing sslcert
! CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER certuser,
  SSL MODE verify_full,
  SSL CERTIFICATE SECRET noexist,
  SSL KEY SECRET ssl_key,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca,
  DATABASE postgres;
contains:unknown catalog item 'noexist'

# missing sslkey
! CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER certuser,
  SSL MODE verify_full,
  SSL CERTIFICATE SECRET ssl_cert,
  SSL KEY SECRET noexist,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca,
  DATABASE postgres;
contains:unknown catalog item 'noexist'

# missing sslrootcert
! CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER certuser,
  SSL MODE verify_full,
  SSL CERTIFICATE SECRET ssl_cert,
  SSL KEY SECRET ssl_key,
  SSL CERTIFICATE AUTHORITY SECRET noexist,
  DATABASE postgres;
contains:unknown catalog item 'noexist'

# require both sslcert and sslkey
! CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER certuser,
  SSL MODE verify_full,
  SSL CERTIFICATE SECRET ssl_cert,
  DATABASE postgres;
contains:invalid CONNECTION: both SSL KEY and SSL CERTIFICATE are required

! CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER certuser,
  SSL MODE verify_full,
  SSL KEY SECRET ssl_cert,
  DATABASE postgres;
contains:invalid CONNECTION: both SSL KEY and SSL CERTIFICATE are required
