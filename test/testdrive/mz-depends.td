# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test `mz_internal.mz_object_dependencies`.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_connection_validation_syntax = true

> CREATE SOURCE with_subsources FROM LOAD GENERATOR AUCTION FOR ALL TABLES;

> SELECT
  top_level_s.name as source,
  s.name AS subsource
  FROM mz_internal.mz_object_dependencies AS d
  JOIN mz_sources AS s ON s.id = d.referenced_object_id
  JOIN mz_sources AS top_level_s ON top_level_s.id = d.object_id
  WHERE top_level_s.name = 'with_subsources';
source          subsource
-------------------------
with_subsources accounts
with_subsources auctions
with_subsources bids
with_subsources organizations
with_subsources users
with_subsources with_subsources_progress

# make sure dropping works
> DROP SOURCE with_subsources CASCADE
> SELECT
  top_level_s.name as source,
  s.name AS subsource
  FROM mz_internal.mz_object_dependencies AS d
  JOIN mz_sources AS s ON s.id = d.referenced_object_id
  JOIN mz_sources AS top_level_s ON top_level_s.id = d.object_id
  WHERE top_level_s.name = 'with_subsources';
source          subsource
-------------------------

# Make sure other objects work as well.
# TODO(guswynn): determine if we need to test all object types exhaustively
> CREATE CONNECTION ssh_conn TO SSH TUNNEL (
    HOST 'unused',
    USER 'mz',
    PORT 22
  );
> CREATE CONNECTION pg_conn TO POSTGRES (
    HOST unused,
    DATABASE unused,
    USER unused,
    SSH TUNNEL ssh_conn
  ) WITH (VALIDATE = false);

> SELECT
  top_level_c.name as conn,
  c.name AS dep_conn
  FROM mz_internal.mz_object_dependencies AS d
  JOIN mz_connections AS c ON c.id = d.referenced_object_id
  JOIN mz_connections AS top_level_c ON top_level_c.id = d.object_id
conn     dep_conn
-----------------
pg_conn ssh_conn


# Assert that we actually are populating all the system objects.
> SELECT COUNT(*) > 200 FROM mz_internal.mz_object_dependencies WHERE object_id LIKE 's%'
true

# Ensure there are no duplicates
> WITH cte AS (
    SELECT * FROM mz_internal.mz_object_dependencies
    GROUP BY object_id, referenced_object_id
    HAVING count(*) > 1
  )
  SELECT count(*) FROM cte
0

# This isn't a full cycle check, but checks if 2 objects depend on each other
> SELECT
  COUNT(*)
  FROM mz_internal.mz_object_dependencies AS first
  JOIN mz_internal.mz_object_dependencies AS second
  ON first.object_id = second.referenced_object_id AND first.referenced_object_id = second.object_id;
0
