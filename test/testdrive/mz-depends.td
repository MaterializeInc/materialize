# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test `mz_internal.mz_object_dependencies`.

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
  );

> SELECT
  top_level_c.name as conn,
  c.name AS dep_conn
  FROM mz_internal.mz_object_dependencies AS d
  JOIN mz_connections AS c ON c.id = d.referenced_object_id
  JOIN mz_connections AS top_level_c ON top_level_c.id = d.object_id
conn     dep_conn
-----------------
pg_conn ssh_conn
