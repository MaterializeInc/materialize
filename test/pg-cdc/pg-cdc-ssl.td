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
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=host sslmode=disable dbname=postgres'
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
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: host, client: prefer => OK
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=host sslmode=prefer dbname=postgres'
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
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: host, client: require => OK
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=host sslmode=require dbname=postgres'
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
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: hostssl, client: disable => ERROR
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=hostssl sslmode=disable dbname=postgres'
  PUBLICATION 'mz_source';
contains:db error: FATAL: no pg_hba.conf entry for host "(HOST)", user "hostssl", database "postgres", SSL off

# server: hostssl, client: prefer => OK
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=hostssl sslmode=prefer dbname=postgres'
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
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: hostssl, client: require => OK
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=hostssl sslmode=require dbname=postgres'
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
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: hostssl, client: verify-ca => ERROR
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=hostssl dbname=postgres sslmode=verify-ca'
  PUBLICATION 'mz_source';
contains:self signed certificate in certificate chain

# server: hostssl, client: verify-ca => OK
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=hostssl dbname=postgres sslmode=verify-ca sslrootcert=/share/secrets/ca.crt'
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
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: hostssl, client: verify-full => ERROR
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=hostssl dbname=postgres sslmode=verify-full'
  PUBLICATION 'mz_source';
contains:self signed certificate in certificate chain

# server: hostssl, client: verify-full => OK
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=hostssl dbname=postgres sslmode=verify-full sslrootcert=/share/secrets/ca.crt'
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
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: hostnossl, client: disable => OK
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=hostnossl sslmode=disable dbname=postgres'
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
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: hostnossl, client: prefer => OK
# todo(uce): rust-postgres does not fall back to no SSL
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=hostnossl sslmode=prefer dbname=postgres'
  PUBLICATION 'mz_source';
contains:db error: FATAL: no pg_hba.conf entry for host "(HOST)", user "hostnossl", database "postgres", SSL on

# server: hostnossl, client: require => ERROR
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=hostnossl sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';
contains:db error: FATAL: no pg_hba.conf entry for host "(HOST)", user "hostnossl", database "postgres", SSL on

# server: hostnossl, client: verify-ca => ERROR
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=hostnossl sslmode=verify-ca dbname=postgres'
  PUBLICATION 'mz_source';
contains:self signed certificate in certificate chain

# server: hostnossl, client: verify-full => ERROR
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=hostnossl sslmode=verify-full dbname=postgres'
  PUBLICATION 'mz_source';
contains:self signed certificate in certificate chain

# server: certuser, client: require => OK
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=certuser sslmode=require sslcert=/share/secrets/certuser.crt sslkey=/share/secrets/certuser.key sslrootcert=/share/secrets/ca.crt dbname=postgres'
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
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: certuser, client: verify-ca => ERROR
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=certuser dbname=postgres sslmode=verify-ca sslrootcert=/share/secrets/ca.crt'
  PUBLICATION 'mz_source'
contains:db error: FATAL: connection requires a valid client certificate

# server: certuser, client: verify-ca (wrong cert) => ERROR
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=certuser dbname=postgres sslmode=verify-ca sslcert=/share/secrets/postgres.crt sslkey=/share/secrets/postgres-world-readable.key sslrootcert=/share/secrets/ca.crt'
  PUBLICATION 'mz_source'
contains:db error: FATAL: certificate authentication failed for user "certuser"

# server: certuser, client: verify-ca => OK
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=certuser sslmode=verify-ca sslcert=/share/secrets/certuser.crt sslkey=/share/secrets/certuser.key sslrootcert=/share/secrets/ca.crt dbname=postgres'
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
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# server: certuser, client: verify-full => OK
> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=certuser sslmode=verify-ca sslcert=/share/secrets/certuser.crt sslkey=/share/secrets/certuser.key sslrootcert=/share/secrets/ca.crt dbname=postgres'
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
$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DELETE FROM numbers WHERE number = 2;

# missing sslcert
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=certuser sslmode=verify-ca sslcert=/share/secrets/missing sslkey=/share/secrets/certuser.key sslrootcert=/share/secrets/ca.crt dbname=postgres'
  PUBLICATION 'mz_source'
contains:invalid connection string: invalid value for option `sslcert`

# missing sslkey
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=certuser sslmode=verify-ca sslcert=/share/secrets/certuser.crt sslkey=/share/secrets/missing sslrootcert=/share/secrets/ca.crt dbname=postgres'
  PUBLICATION 'mz_source'
contains:invalid connection string: invalid value for option `sslkey`

# missing sslrootcert
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=certuser sslmode=verify-ca sslcert=/share/secrets/certuser.crt sslkey=/share/secrets/certuser.key sslrootcert=/share/secrets/missing dbname=postgres'
  PUBLICATION 'mz_source'
contains:invalid connection string: invalid value for option `sslrootcert`

# require both sslcert and sslkey
! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=certuser sslmode=verify-ca sslcert=/share/secrets/certuser.crt dbname=postgres'
  PUBLICATION 'mz_source'
contains:must provide both sslcert and sslkey, but only provided sslcert

! CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES CONNECTION 'host=postgres port=5432 user=certuser sslmode=verify-ca sslkey=/share/secrets/certuser.key dbname=postgres'
  PUBLICATION 'mz_source'
contains:must provide both sslcert and sslkey, but only provided sslkey
