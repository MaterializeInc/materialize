# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Smoke test to make sure queries that the Web Console uses work.

# The Web Console sets the cluster to mz_introspection for every request.
> SET CLUSTER = mz_introspection;

# ---- materialized.tsx

# Note(parkmycar): Here we wrap u.memory_percent in floor(...) and coalesce(...) to make the result
# comparison deterministic.

# useClusterReplicasWithUtilization(...)
> SELECT r.id, r.name as replica_name, r.cluster_id, r.size, coalesce(floor(u.memory_percent), 0)
  FROM mz_cluster_replicas r
  JOIN mz_internal.mz_cluster_replica_utilization u ON u.replica_id = r.id
  WHERE r.cluster_id = 'u1'
  ORDER BY r.id;
u1 r1 u1 4-4 0
u1 r1 u1 4-4 0
u1 r1 u1 4-4 0
u1 r1 u1 4-4 0

# useSinkErrors(...)
> SELECT MAX(extract(epoch from h.occurred_at) * 1000) as last_occurred, h.error, COUNT(h.occurred_at)
  FROM mz_internal.mz_sink_status_history h
  WHERE sink_id = 'u1'
  AND error IS NOT NULL
  AND h.occurred_at BETWEEN '2022-01-01T00:00:00.000Z' AND '2022-12-31T11:59:59.999Z'
  GROUP BY h.error
  ORDER BY last_occurred DESC
  LIMIT 10;

# useBucketedSinkErrors(...)
> SELECT COUNT(error) count, EXTRACT(epoch FROM date_bin(interval '15 seconds', occurred_at, '2022-01-01T00:00:00.000Z')) * 1000 as bin_start
  FROM mz_internal.mz_sink_status_history
  WHERE sink_id = 'u1'
  AND occurred_at BETWEEN '2022-01-01T00:00:00.000Z' AND '2022-12-31T11:59:59.999Z'
  GROUP BY bin_start
  ORDER BY bin_start DESC;

# useSinks(...)
> SELECT s.id, d.name as database_name, sc.name as schema_name, s.name, s.type, s.size, st.status, st.error
  FROM mz_sinks s
  INNER JOIN mz_schemas sc ON sc.id = s.schema_id
  INNER JOIN mz_databases d ON d.id = sc.database_id
  LEFT OUTER JOIN mz_internal.mz_sink_statuses st
  ON st.id = s.id
  WHERE s.id LIKE 'u%'
  AND CAST(d.id as text) = 'u2'
  AND CAST(sc.id as text) = 'u3';

# useMaterializedViews(...)
> SELECT id, name, definition
  FROM mz_materialized_views
  WHERE cluster_id = 'u1';

# useIndexes(...)
> SELECT i.id, i.name, r.name as relation_name, r.type
  FROM mz_indexes i
  INNER JOIN mz_relations r on r.id = i.on_id
  WHERE cluster_id = 'u1'
  AND i.id LIKE 'u%';

# useSecrets(...)
> SELECT s.id,  s.name,  events.occurred_at as created_at, d.name as database_name, sc.name as schema_name
  FROM mz_secrets s
  INNER JOIN mz_audit_events events ON events.details->>'id' = s.id AND event_type='create' AND object_type='secret'
  INNER JOIN mz_schemas sc ON sc.id = s.schema_id
  INNER JOIN mz_databases d ON d.id = sc.database_id AND CAST(d.id as text) = 'u2' AND CAST(sc.id as text) = 'u3'
  ORDER BY created_at DESC;

# ---- useAvailableClusterSizes.ts

> SHOW allowed_cluster_replica_sizes
""

# ---- useBucketedSourceErrors.ts

> SELECT COUNT(error) count, EXTRACT(epoch FROM date_bin(interval '15 seconds', occurred_at, '2023-06-01T00:00:00.000Z')) * 1000 as bin_start
  FROM mz_internal.mz_source_status_history
  JOIN mz_internal.mz_object_dependencies d ON source_id = d.referenced_object_id
  WHERE (d.object_id = 'u1' OR source_id = 'u1')
  AND occurred_at BETWEEN '2022-01-01T00:00:00.000Z' AND '2022-12-31T11:59:59.999Z'
  GROUP BY bin_start
  ORDER BY bin_start DESC

# ---- useClusters.ts

> SELECT c.id, c.name as cluster_name, r.id as replica_id, r.name as replica_name, r.size
  FROM mz_clusters c
  LEFT OUTER JOIN mz_cluster_replicas r ON c.id = r.cluster_id
  ORDER BY r.id
u1 default u1 r1 4-4
s1 mz_system u2 r1 1
s2 mz_introspection u3 r1 1

# ---- useClusterUtilization.ts

> SELECT r.id, u.cpu_percent, u.memory_percent
  FROM mz_cluster_replicas r
  JOIN mz_internal.mz_cluster_replica_utilization u ON u.replica_id = r.id
  WHERE r.cluster_id = 'u1'
  AND r.id = 'u2'

# ---- useConnections.ts

> SELECT connections.id, connections.name, schemas.name as schema_name, databases.name as database_name, connections.type, COUNT(sinks.id) AS num_sinks,  COUNT(sources.id) AS num_sources
  FROM mz_connections AS connections
  INNER JOIN mz_schemas schemas ON schemas.id = connections.schema_id
  INNER JOIN mz_databases databases ON databases.id = schemas.database_id
  LEFT JOIN mz_sinks AS sinks ON connections.id = sinks.connection_id
  LEFT JOIN mz_sources AS sources ON connections.id = sources.connection_id
  WHERE COALESCE(sources.type, '') <> 'subsource' AND CAST(databases.id as text) = 'u2' AND CAST(schemas.id as text) = 'u3'
  GROUP BY connections.id, connections.name, connections.type, schema_name, database_name

# ---- useDatabases.ts

# Note(parkmycar): Here we filter to a specific id to make results deterministic.

> SELECT id, name
  FROM mz_databases
  WHERE id = 'u2'
  ORDER BY name
u2 materialize

# ---- useMaxReplicasPerCluster.ts

> SHOW max_replicas_per_cluster;
5

# ---- useSchemas.tsx

> SELECT s.id, s.name, d.id as database_id, d.name as database_name
  FROM mz_schemas s
  JOIN mz_databases d
  ON s.database_id = d.id
  WHERE CAST(database_id as text) = 'u2'
  ORDER BY s.name;
u6 public u2 materialize

# ---- useSourceErrors.tsx

> SELECT MAX(extract(epoch from h.occurred_at) * 1000) as last_occurred, h.error, COUNT(h.occurred_at)
  FROM mz_internal.mz_source_status_history h
  JOIN mz_internal.mz_object_dependencies d ON h.source_id = d.referenced_object_id
  WHERE (d.object_id = 's390' OR source_id = 's390')
  AND error IS NOT NULL
  AND h.occurred_at BETWEEN '2022-01-01T00:00:00.000Z' AND '2022-12-31T11:59:59.999Z'
  GROUP BY h.error
  ORDER BY last_occurred DESC
  LIMIT 10


# ---- useSources.ts

> SELECT s.id, d.name as database_name, sc.name as schema_name, s.name, s.type, s.size, st.status, st.error
  FROM mz_sources s
  INNER JOIN mz_schemas sc ON sc.id = s.schema_id
  INNER JOIN mz_databases d ON d.id = sc.database_id
  LEFT OUTER JOIN mz_internal.mz_source_statuses st ON st.id = s.id
  WHERE s.id LIKE 'u%'
  AND s.type <> 'subsource'
  AND d.id = 'u2'
  AND sc.id = 'u3';

# ---- useSubsources.tsx

> SELECT id, name
  FROM mz_sources s
  JOIN mz_internal.mz_object_dependencies d ON s.id = d.referenced_object_id
  WHERE d.object_id = 's390';

# ---- NewClusterForm.tsx

> CREATE CLUSTER foo REPLICAS ( r1 (SIZE = '1') );
> SELECT id FROM mz_clusters WHERE name = 'foo';
u2

> DROP CLUSTER foo CASCADE;


# ---- Schema.tsx

> SHOW VIEWS;
> SHOW SOURCES;
> SHOW TABLES;

# ---- SecretsList.tsx

> CREATE SECRET my_secret AS 'abcd123';

# useDataflowStructure.ts
#
# Note(parkmycar): Currently doesn't work since we restrict querying non-system objects when the
# mz_introspection cluster is set, and here we're creating a temporary view that depends on another
# temporary (and thus non-system) view. There queries are used on a not yet released feature, and
# a fix is in progress.
#
# > CREATE TEMPORARY VIEW export_to_dataflow AS
#   SELECT export_id, id FROM mz_internal.mz_compute_exports AS mce JOIN mz_internal.mz_dataflows AS md ON
#   mce.dataflow_id = md.local_id;
#
# > CREATE TEMPORARY VIEW all_ops AS
#   SELECT e2d.export_id, mdod.id, mda.address, mdod.name, mdop.parent_id, coalesce(mas.records, 0) AS arrangement_records, coalesce(mse.elapsed_ns, 0) AS elapsed_ns
#   FROM export_to_dataflow AS e2d
#   JOIN mz_internal.mz_dataflow_operator_dataflows AS mdod ON e2d.id = mdod.dataflow_id
#   LEFT JOIN mz_internal.mz_scheduling_elapsed AS mse ON mdod.id = mse.id
#   LEFT JOIN mz_internal.mz_arrangement_sizes AS mas ON mdod.id = mas.operator_id
#   LEFT JOIN mz_internal.mz_dataflow_operator_parents AS mdop ON mdod.id = mdop.id
#   LEFT JOIN mz_internal.mz_dataflow_addresses AS mda ON mdod.id = mda.id;
#
# > BEGIN;
# > SELECT mdco.id, from_operator_id, from_operator_address, from_port, to_operator_id, to_operator_address, to_port, COALESCE(sum(sent), 0) AS sent
#   FROM mz_internal.mz_dataflow_channel_operators AS mdco
#   JOIN mz_internal.mz_dataflow_channels AS mdc ON mdc.id = mdco.id
#   LEFT JOIN mz_internal.mz_message_counts AS mmc ON mdco.id = mmc.channel_id
#   JOIN mz_internal.mz_compute_exports mce ON mce.dataflow_id = from_operator_address[1]
#   WHERE mce.export_id = 's421'
#   GROUP BY mdco.id, from_operator_id, from_operator_address, to_operator_id, to_operator_address, from_port, to_port;
# > SELECT id, address, name, parent_id, arrangement_records, elapsed_ns FROM all_ops WHERE export_id = 's421';
# > COMMIT;
