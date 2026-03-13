# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test: ssl_root_cert PEM bundles with multiple CA certificates.
#
# Previously, only the first certificate in a PEM bundle was loaded into the
# OpenSSL trust store (X509::from_pem reads a single cert). If the relevant CA
# was not the first cert in the bundle, TLS verification would fail with
# "unknown ca". The fix uses X509::stack_from_pem to load all certs.

$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP TABLE IF EXISTS numbers;
CREATE TABLE numbers (number int PRIMARY KEY, is_prime bool, name text);
ALTER TABLE numbers REPLICA IDENTITY FULL;
DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;
INSERT INTO numbers VALUES (1, true, 'one');

> CREATE SECRET pgpass AS 'postgres'
> CREATE SECRET ssl_ca AS '${arg.ssl-ca}'

# Build a PEM bundle: unrelated CA first, real CA second.
# Before the fix, only the first (unrelated) CA was loaded, causing
# "self-signed certificate in certificate chain" errors.
> CREATE SECRET ssl_ca_bundle AS '${arg.ssl-ca-unrelated}${arg.ssl-ca}'

# Verify: single CA works (sanity check)
> CREATE CONNECTION pgconn_single TO POSTGRES (
  HOST postgres,
  USER postgres,
  PASSWORD SECRET pgpass,
  SSL MODE verify_ca,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca,
  DATABASE postgres
  )

> CREATE SOURCE mz_source_single
  FROM POSTGRES CONNECTION pgconn_single (PUBLICATION 'mz_source')

> CREATE TABLE numbers_single FROM SOURCE mz_source_single (REFERENCE numbers)

> SELECT * FROM numbers_single
1 true one

> DROP SOURCE mz_source_single CASCADE
> DROP CONNECTION pgconn_single

# Core regression test: PEM bundle with unrelated CA first, real CA second.
> CREATE CONNECTION pgconn_bundle TO POSTGRES (
  HOST postgres,
  USER postgres,
  PASSWORD SECRET pgpass,
  SSL MODE verify_ca,
  SSL CERTIFICATE AUTHORITY SECRET ssl_ca_bundle,
  DATABASE postgres
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres:5432
DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source_bundle
  FROM POSTGRES CONNECTION pgconn_bundle (PUBLICATION 'mz_source')

> CREATE TABLE numbers_bundle FROM SOURCE mz_source_bundle (REFERENCE numbers)

> SELECT * FROM numbers_bundle
1 true one

> DROP SOURCE mz_source_bundle CASCADE
> DROP CONNECTION pgconn_bundle
> DROP SECRET ssl_ca_bundle
> DROP SECRET ssl_ca
> DROP SECRET pgpass
