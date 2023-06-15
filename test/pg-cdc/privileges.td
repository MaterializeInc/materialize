# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# IMPORTANT: The Postgres server has a custom pg_hba.conf that only
# accepts connections from specific users. You will have to update
# pg_hba.conf if you modify the existing user names or add new ones.

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE USER priv WITH PASSWORD 'priv';
ALTER USER priv WITH replication;

CREATE SCHEMA other;
CREATE TABLE other.s (a int);
GRANT ALL PRIVILEGES ON SCHEMA other TO priv;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA other TO priv;

CREATE TABLE public.t (a int);
GRANT ALL PRIVILEGES ON SCHEMA public TO priv;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO priv;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

REVOKE CONNECT ON DATABASE postgres FROM public;

#
# CONNECT error
#

> CREATE SECRET pgpass AS 'priv'
! CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER priv,
    PASSWORD SECRET pgpass
  )
contains:permission denied for database "postgres"

#
# USAGE error
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
GRANT CONNECT ON DATABASE postgres TO public;
REVOKE USAGE ON SCHEMA other FROM priv;

> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER priv,
    PASSWORD SECRET pgpass
  )

! CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR SCHEMAS(public, other);
contains:failed to connect to PostgreSQL database
detail:user priv lacks USAGE privileges for schemas other

#
# SELECT errors
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
GRANT ALL PRIVILEGES ON SCHEMA other TO priv;
REVOKE SELECT ON TABLE other.s FROM priv;

! CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR SCHEMAS(public, other);
contains:failed to connect to PostgreSQL database
detail:user priv lacks SELECT privileges for tables other.s

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE "select" (a INT);
REVOKE SELECT ON public.select FROM priv;
CREATE TABLE """select""" (a INT);
REVOKE SELECT ON public."""select""" FROM priv;

! CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR SCHEMAS(public);
contains:failed to connect to PostgreSQL database
detail:user priv lacks SELECT privileges for tables public."""select""", public."select"
