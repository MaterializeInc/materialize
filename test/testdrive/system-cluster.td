# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-connect name=mz_system url=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}

> SHOW CLUSTERS
mz_system
mz_introspection
default

! DROP CLUSTER mz_system CASCADE
contains:cluster name "mz_system" is reserved

! DROP CLUSTER mz_introspection CASCADE
contains:cluster name "mz_introspection" is reserved

! CREATE CLUSTER mz_joe REPLICAS (r1 (size '1'))
contains:cluster name "mz_joe" is reserved

> CREATE MATERIALIZED VIEW mv AS SELECT AVG(50)

> SET CLUSTER TO mz_introspection

> SHOW MATERIALIZED VIEWS
mv default

! CREATE MATERIALIZED VIEW mv1 AS SELECT MIN(1)
contains:system cluster 'mz_introspection' cannot be modified

> SET CLUSTER TO mz_system

! SHOW MATERIALIZED VIEWS
contains:system cluster 'mz_system' cannot execute user queries

! CREATE MATERIALIZED VIEW mv1 AS SELECT MIN(1)
contains:system cluster 'mz_system' cannot be modified

> SET CLUSTER TO default

> CREATE TABLE temp (a INT)

> INSERT INTO temp VALUES (1), (2)

$ postgres-execute connection=mz_system
SET CLUSTER TO mz_system
INSERT INTO temp SELECT * FROM temp

> SELECT * FROM temp
1
2
1
2

> SET CLUSTER TO mz_introspection

$ set-regex match=(\(\d+\s=\s|u\d+) replacement=

? EXPLAIN SHOW DATABASES
%0 =
| ReadExistingIndex mz_internal.mz_show_databases_ind
| Project (#0)

? EXPLAIN SHOW SCHEMAS
%0 =
| ReadExistingIndex mz_internal.mz_show_schemas_ind
| Filter ((#0) IS NULL OR uint8_to_double(#0)))
| Project (#3)

? EXPLAIN SHOW CONNECTIONS
%0 =
| ReadExistingIndex mz_internal.mz_show_connections_ind
| Filter uint8_to_double(#0))
| Project (#3, #4)

? EXPLAIN SHOW TABLES
%0 =
| ReadExistingIndex mz_internal.mz_show_tables_ind
| Filter uint8_to_double(#0))
| Project (#3)

? EXPLAIN SHOW SOURCES
%0 =
| ReadExistingIndex mz_internal.mz_show_sources_ind
| Filter uint8_to_double(#0))
| Project (#3, #4, #6)

? EXPLAIN SHOW VIEWS
%0 =
| ReadExistingIndex mz_internal.mz_show_views_ind
| Filter uint8_to_double(#0))
| Project (#3)

? EXPLAIN SHOW MATERIALIZED VIEWS
%0 =
| ReadExistingIndex mz_internal.mz_show_materialized_views_ind
| Filter uint8_to_double(#0))
| Project (#2, #3)

? EXPLAIN SHOW INDEXES
%0 =
| ReadExistingIndex mz_internal.mz_show_indexes_ind
| Filter uint8_to_double(#1))
| Project (#3..=#6)

? EXPLAIN SHOW SINKS
%0 =
| ReadExistingIndex mz_internal.mz_show_sinks_ind
| Filter uint8_to_double(#0))
| Project (#3, #4, #6)

? EXPLAIN SHOW TYPES
%0 =
| ReadExistingIndex mz_internal.mz_show_types_ind
| Filter uint8_to_double(#0))
| Project (#3)

? EXPLAIN SHOW OBJECTS
%0 =
| ReadExistingIndex mz_internal.mz_show_all_objects_ind
| Filter uint8_to_double(#0))
| Project (#3, #4)

> CREATE TABLE t (a INT)

? EXPLAIN SHOW COLUMNS IN t
%0 =
| ReadExistingIndex mz_internal.mz_show_columns_ind
| | Lookup value ("")
| Project (#1, #3, #4)

? EXPLAIN SHOW CLUSTERS
%0 =
| ReadExistingIndex mz_internal.mz_show_clusters_ind
| Project (#0)


? EXPLAIN SHOW CLUSTER REPLICAS
%0 =
| ReadExistingIndex mz_internal.mz_show_cluster_replicas_ind

? EXPLAIN SHOW SECRETS
%0 =
| ReadExistingIndex mz_internal.mz_show_secrets_ind
| Filter uint8_to_double(#0))
| Project (#2)
