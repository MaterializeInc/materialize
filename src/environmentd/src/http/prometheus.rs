// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apach

use mz_catalog::memory::objects::{Cluster, ClusterReplica};

use super::sql::SqlRequest;

#[derive(Debug)]
pub(crate) struct PrometheusSqlQuery<'a> {
    pub(crate) metric_name: &'a str,
    pub(crate) help: &'a str,
    pub(crate) query: &'a str,
    pub(crate) value_column_name: &'a str,
    pub(crate) per_replica: bool,
}

impl<'a> PrometheusSqlQuery<'a> {
    pub(crate) fn to_sql_request(
        &self,
        cluster: Option<(&Cluster, &ClusterReplica)>,
    ) -> SqlRequest {
        SqlRequest::Simple {
            query: if let Some((cluster, replica)) = cluster {
                format!(
                    "SET auto_route_catalog_queries = false; SET CLUSTER = '{}'; SET CLUSTER_REPLICA = '{}'; {}",
                    cluster.name, replica.name, self.query
                )
            } else {
                format!(
                    "SET auto_route_catalog_queries = true; RESET CLUSTER; RESET CLUSTER_REPLICA; {}",
                    self.query
                )
            },
        }
    }
}

pub(crate) static FRONTIER_METRIC_QUERIES: &[PrometheusSqlQuery] = &[
    PrometheusSqlQuery {
        metric_name: "mz_write_frontier",
        help: "The global write frontiers of compute and storage collections.",
        query: "SELECT
                    object_id AS collection_id,
                    coalesce(write_frontier::text::uint8, 18446744073709551615::uint8) AS write_frontier
                FROM mz_internal.mz_frontiers
                WHERE object_id NOT LIKE 't%';",
        value_column_name: "write_frontier",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_read_frontier",
        help: "The global read frontiers of compute and storage collections.",
        query: "SELECT
                    object_id AS collection_id,
                    coalesce(read_frontier::text::uint8, 18446744073709551615::uint8) AS read_frontier
                FROM mz_internal.mz_frontiers
                WHERE object_id NOT LIKE 't%';",
        value_column_name: "read_frontier",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_replica_write_frontiers",
        help: "The per-replica write frontiers of compute and storage collections.",
        query: "SELECT
                    object_id AS collection_id,
                    coalesce(write_frontier::text::uint8, 18446744073709551615::uint8) AS write_frontier,
                    cluster_id AS instance_id,
                    replica_id AS replica_id
                FROM mz_catalog.mz_cluster_replica_frontiers
                JOIN mz_cluster_replicas ON (id = replica_id)
                WHERE object_id NOT LIKE 't%';",
        value_column_name: "write_frontier",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_replica_write_frontiers",
        help: "The per-replica write frontiers of compute and storage collections.",
        query: "SELECT
                    object_id AS collection_id,
                    coalesce(write_frontier::text::uint8, 18446744073709551615::uint8) AS write_frontier,
                    cluster_id AS instance_id,
                    replica_id AS replica_id
                FROM mz_catalog.mz_cluster_replica_frontiers
                JOIN mz_cluster_replicas ON (id = replica_id)
                WHERE object_id NOT LIKE 't%';",
        value_column_name: "write_frontier",
        per_replica: false,
    },
];

pub(crate) static USAGE_METRIC_QUERIES: &[PrometheusSqlQuery] = &[
    PrometheusSqlQuery {
        metric_name: "mz_compute_cluster_status",
        help: "Reports the name, ID, size, and availability zone of each cluster replica. Value is always 1.",
        query: "SELECT
                1 AS status,
                cr.cluster_id AS compute_cluster_id,
                cr.id AS compute_replica_id,
                c.name AS compute_cluster_name,
                cr.name AS compute_replica_name,
                COALESCE(cr.size, '') AS size,
                COALESCE(cr.availability_zone, '') AS availability_zone,
                mz_version() AS mz_version
          FROM
            mz_cluster_replicas AS cr
            JOIN mz_clusters AS c
              ON cr.cluster_id = c.id",
        value_column_name: "status",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_workload_clusters",
        help: "Reports the workload_class of each user cluster. Value is always 1.",
        query: "select
                    c.id as cluster_id,
                    c.name as cluster_name,
                    coalesce(wc.workload_class,'false') as workload_class,
                    1 as value
                from mz_clusters c
                join mz_internal.mz_cluster_workload_classes wc
                    on c.id = wc.id",
        value_column_name: "value",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_clusters_count",
        help: "Number of active clusters in the instance.",
        query: "select
                count(id) as clusters
            from mz_clusters
            where id like 'u%'",
        value_column_name: "clusters",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_cluster_reps_count",
        help: "Number of active cluster replicas in the instance, by replica size.",
        query: "select size
                , count(id) as replicas
            from mz_cluster_replicas
            where cluster_id like 'u%'
            group by size",
        value_column_name: "replicas",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_indexes_count",
        help: "Number of active indexes in the instance, by the type of relation on which the index is built.",
        query: "select
                o.type as relation_type
                , count(i.id) as indexes
            from mz_catalog.mz_indexes i
            join mz_catalog.mz_objects o
                on i.on_id = o.id
            where i.id like 'u%'
            group by relation_type",
        value_column_name: "indexes",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_sources_count",
        help: "Number of active sources in the instance, by type, envelope type, and size of source.",
        query: "SELECT
                type,
                COALESCE(envelope_type, '<none>') AS envelope_type,
                mz_cluster_replicas.size AS size,
                count(mz_sources.id) AS sources
            FROM
                mz_sources, mz_cluster_replicas
            WHERE mz_sources.id LIKE 'u%'
            AND mz_sources.type != 'subsource'
            AND mz_sources.cluster_id = mz_cluster_replicas.cluster_id
            GROUP BY
            type, envelope_type, mz_cluster_replicas.size",
        value_column_name: "sources",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_views_count",
        help: "Number of active views in the instance.",
        query: "select count(id) as views from mz_views where id like 'u%'",
        value_column_name: "views",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_mzd_views_count",
        help: "Number of active materialized views in the instance.",
        query: "select count(id) as mzd_views from mz_materialized_views where id like 'u%'",
        value_column_name: "mzd_views",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_secrets_count",
        help: "Number of active secrets in the instance.",
        query: "select count(id) as secrets from mz_secrets where id like 'u%'",
        value_column_name: "secrets",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_sinks_count",
        help: "Number of active sinks in the instance, by type, envelope type, and size.",
        query: "SELECT
                type,
                COALESCE(envelope_type, '<none>') AS envelope_type,
                mz_cluster_replicas.size AS size,
                count(mz_sinks.id) AS sinks
            FROM
                mz_sinks, mz_cluster_replicas
            WHERE mz_sinks.id LIKE 'u%'
            AND mz_sinks.cluster_id = mz_cluster_replicas.cluster_id
            GROUP BY type, envelope_type, mz_cluster_replicas.size",
        value_column_name: "sinks",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_connections_count",
        help: "Number of active connections in the instance, by type.",
        query: "select
                type
                , count(id) as connections
            from mz_connections
            where id like 'u%'
            group by type",
        value_column_name: "connections",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_tables_count",
        help: "Number of active tables in the instance.",
        query: "select count(id) as tables from mz_tables where id like 'u%'",
        value_column_name: "tables",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_catalog_items",
        help: "Mapping internal id for catalog item.",
        query: "SELECT
                concat(d.name, '.', s.name, '.', o.name) AS label,
                0 AS value
            FROM mz_objects o
            JOIN mz_schemas s ON (o.schema_id = s.id)
            JOIN mz_databases d ON (s.database_id = d.id)
            WHERE o.id LIKE 'u%'",
        value_column_name: "value",
        per_replica: false,
    },
    PrometheusSqlQuery {
        metric_name: "mz_object_id",
        help: "Mapping external name for catalog item.",
        query: "SELECT
                o.id AS label1,
                concat(d.name, '.', s.name, '.', o.name) AS label2,
                0 AS value
            FROM mz_objects o
            JOIN mz_schemas s ON (o.schema_id = s.id)
            JOIN mz_databases d ON (s.database_id = d.id)
            WHERE o.id LIKE 'u%'",
        value_column_name: "value",
        per_replica: false,
    },
];

pub(crate) static COMPUTE_METRIC_QUERIES: &[PrometheusSqlQuery] = &[
    PrometheusSqlQuery {
        metric_name: "mz_arrangement_count",
        help: "The number of arrangements in a dataflow.",
        query: "WITH
            arrangements AS (
                SELECT DISTINCT operator_id AS id
                FROM mz_internal.mz_arrangement_records_raw
            ),
            collections AS (
                SELECT
                    id,
                    regexp_replace(export_id, '^t.+', 'transient') as export_id
                FROM
                    mz_internal.mz_dataflow_addresses,
                    mz_internal.mz_compute_exports
                WHERE address[1] = dataflow_id
            )
        SELECT
            COALESCE(export_id, 'none') AS collection_id,
            count(*) as count
        FROM arrangements
        LEFT JOIN collections USING (id)
        GROUP BY export_id",
        value_column_name: "count",
        per_replica: true,
    },
    PrometheusSqlQuery {
        metric_name: "mz_arrangement_record_count",
        help: "The number of records in all arrangements in a dataflow.",
        query: "WITH
            collections AS (
                SELECT
                    id,
                    regexp_replace(export_id, '^t.+', 'transient') as export_id
                FROM
                    mz_internal.mz_dataflow_addresses,
                    mz_internal.mz_compute_exports
                WHERE address[1] = dataflow_id
            )
        SELECT
            worker_id,
            COALESCE(export_id, 'none') AS collection_id,
            count(*) as count
        FROM mz_internal.mz_arrangement_records_raw
        LEFT JOIN collections ON (operator_id = id)
        GROUP BY worker_id, export_id",
        value_column_name: "count",
        per_replica: true,
    },
    PrometheusSqlQuery {
        metric_name: "mz_arrangement_batch_count",
        help: "The number of batches in all arrangements in a dataflow.",
        query: "WITH
            collections AS (
                SELECT
                    id,
                    regexp_replace(export_id, '^t.+', 'transient') as export_id
                FROM
                    mz_internal.mz_dataflow_addresses,
                    mz_internal.mz_compute_exports
                WHERE address[1] = dataflow_id
            )
        SELECT
            worker_id,
            COALESCE(export_id, 'none') AS collection_id,
            count(*) as count
        FROM mz_internal.mz_arrangement_batches_raw
        LEFT JOIN collections ON (operator_id = id)
        GROUP BY worker_id, export_id",
        value_column_name: "count",
        per_replica: true,
    },
    PrometheusSqlQuery {
        metric_name: "mz_arrangement_size_bytes",
        help: "The size of all arrangements in a dataflow.",
        query: "WITH
            collections AS (
                SELECT
                    id,
                    regexp_replace(export_id, '^t.+', 'transient') as export_id
                FROM
                    mz_internal.mz_dataflow_addresses,
                    mz_internal.mz_compute_exports
                WHERE address[1] = dataflow_id
            )
        SELECT
            worker_id,
            COALESCE(export_id, 'none') AS collection_id,
            count(*) as count
        FROM mz_internal.mz_arrangement_heap_size_raw
        LEFT JOIN collections ON (operator_id = id)
        GROUP BY worker_id, export_id",
        value_column_name: "count",
        per_replica: true,
    },
    PrometheusSqlQuery {
        metric_name: "mz_arrangement_capacity_bytes",
        help: "The capacity of all arrangements in all dataflows.",
        query: "SELECT
            worker_id,
            count(*) as count
        FROM mz_internal.mz_arrangement_heap_capacity_raw
        GROUP BY worker_id",
        value_column_name: "count",
        per_replica: true,
    },
    PrometheusSqlQuery {
        metric_name: "mz_arrangement_allocation_count",
        help: "The number of allocations in all arrangements in all dataflows.",
        query: "SELECT
            worker_id,
            count(*) as count
        FROM mz_internal.mz_arrangement_heap_allocations_raw
        GROUP BY worker_id",
        value_column_name: "count",
        per_replica: true,
    },
    PrometheusSqlQuery {
        metric_name: "mz_compute_replica_park_duration_seconds_total",
        help: "The total time workers were parked since restart.",
        query: "SELECT
            worker_id,
            sum(slept_for_ns * count)::float8 / 1000000000 AS duration_s
        FROM mz_internal.mz_scheduling_parks_histogram_per_worker
        GROUP BY worker_id",
        value_column_name: "duration_s",
        per_replica: true,
    },
    PrometheusSqlQuery {
        metric_name: "mz_compute_replica_peek_count",
        help: "The number of pending peeks.",
        query: "SELECT worker_id, count(*) as count
        FROM mz_internal.mz_active_peeks_per_worker
        GROUP BY worker_id",
        value_column_name: "count",
        per_replica: true,
    },
    PrometheusSqlQuery {
        metric_name: "mz_dataflow_elapsed_seconds_total",
        help: "The total time spent computing a dataflow.",
        query: "SELECT
            worker_id,
            regexp_replace(export_id, '^t.+', 'transient') AS collection_id,
            sum(elapsed_ns)::float8 / 1000000000 AS elapsed_s
        FROM
            mz_internal.mz_scheduling_elapsed_per_worker AS s,
            mz_internal.mz_dataflow_operators AS o,
            mz_internal.mz_dataflow_addresses AS a,
            mz_internal.mz_compute_exports AS e
        WHERE
            o.id = s.id AND
            o.id = a.id AND
            list_length(a.address) = 1 AND
            e.dataflow_id = a.address[1]
        GROUP BY worker_id, collection_id",
        value_column_name: "elapsed_s",
        per_replica: true,
    },
    PrometheusSqlQuery {
        metric_name: "mz_dataflow_error_count",
        help: "The number of errors in a dataflow",
        query: "SELECT
            regexp_replace(export_id, '^t.+', 'transient') AS collection_id,
            count::uint8 as count
        FROM mz_internal.mz_compute_error_counts",
        value_column_name: "count",
        per_replica: true,
    },
];

pub(crate) static STORAGE_METRIC_QUERIES: &[PrometheusSqlQuery] = &[
    PrometheusSqlQuery {
        metric_name: "mz_storage_objects",
        help: "Nicely labeled information about existing sources and sinks.",
        value_column_name: "value",
        per_replica: false,
        query: "
        WITH
            -- All user, non- progress or subsource sources.
            top_level_sources AS (
                SELECT id, connection_id, type, envelope_type, cluster_id
                FROM mz_sources
                WHERE id LIKE 'u%' AND type NOT IN ('progress', 'subsource')
            ),
            -- Sources enriched with the type of the core connection.
            source_and_conns AS (
                SELECT top_level_sources.*, mc.type AS connection_type
                FROM top_level_sources
                LEFT OUTER JOIN mz_connections mc ON mc.id = top_level_sources.connection_id
            ),

            -- All user sinks.
            top_level_sinks AS (
                SELECT id, connection_id, type, envelope_type, cluster_id
                FROM mz_sinks
                WHERE id LIKE 'u%'
            ),
            -- Sinks enriched with the type of the core connection.
            sink_and_conns AS (
                SELECT top_level_sinks.*, mc.type AS connection_type
                FROM top_level_sinks
                LEFT OUTER JOIN mz_connections mc ON mc.id = top_level_sinks.connection_id
            ),

            -- All objects we care about
            object_and_conns AS (
                SELECT * FROM source_and_conns
                UNION ALL
                SELECT * FROM sink_and_conns
            ),

            -- The networking the object connection uses, if any.
            networking AS (
                SELECT object_and_conns.id, mc.id AS networking_id, mc.type AS networking_type
                FROM object_and_conns
                JOIN mz_internal.mz_object_dependencies mod ON object_and_conns.connection_id = mod.object_id
                JOIN mz_connections mc ON mc.id = mod.referenced_object_id
                -- Not required but made explicit
                WHERE object_and_conns.connection_id IS NOT NULL
            ),
            -- The connection the format of the object uses, if any. This uses `mz_object_dependencies`
            -- and a filter to find non-core objects the object depends on.
            format_conns AS (
                SELECT object_and_conns.id, mc.id AS format_connection_id, mc.type AS format_connection
                FROM mz_internal.mz_object_dependencies mod
                JOIN object_and_conns ON mod.object_id = object_and_conns.id
                JOIN mz_connections mc ON mc.id = mod.referenced_object_id
                WHERE mc.id NOT IN (SELECT connection_id FROM top_level_sources UNION ALL SELECT connection_id FROM top_level_sinks)
                -- Not required but made explicit
                AND object_and_conns.connection_id IS NOT NULL
            ),
            -- The networking used by `format_conns`, if any.
            format_conn_deps AS (
                SELECT format_conns.id, mc.id AS format_connection_networking_id, mc.type AS format_connection_networking
                FROM format_conns
                JOIN mz_internal.mz_object_dependencies mod ON mod.object_id = format_conns.format_connection_id
                JOIN mz_connections mc
                ON mc.id = mod.referenced_object_id
            ),

            -- When aggregating values that are known to be the same, we just use `MAX` for simplicity.

            -- source_and_conns LEFT JOINed with the networking and format connection information.
            -- Missing networking/format connections are coalesced to `none`, and aggregated with a comma.
            -- This is because sources can have multiple type of networking (i.e. different kafka brokers with
            -- different configuration), and multiple connections for formats (for key and value formats).
            --
            -- The actual format type is not yet included, as it depends on https://github.com/MaterializeInc/materialize/pull/23880
            sources AS (
                SELECT
                -- Whether its a source or sink
                'source' AS type,
                1 AS value,
                source_and_conns.id AS id,
                -- What type of source/sink it is
                MAX(type) AS object_type,
                COALESCE(MAX(connection_type), 'none') AS connection_type,
                COALESCE(MAX(envelope_type), 'none') AS envelope_type,
                STRING_AGG(
                    DISTINCT COALESCE(networking_type, 'none'),
                    ','
                    ORDER BY COALESCE(networking_type, 'none') ASC
                ) AS networking_type,
                STRING_AGG(
                    DISTINCT COALESCE(format_connection, 'none'),
                    ','
                    ORDER BY COALESCE(format_connection, 'none') ASC
                ) AS format_connection,
                STRING_AGG(
                    DISTINCT COALESCE(format_connection_networking, 'none'),
                    ','
                    ORDER BY COALESCE(format_connection_networking, 'none') ASC
                ) AS format_connection_networking,
                MAX(cluster_id) AS cluster_id
                FROM source_and_conns
                LEFT OUTER JOIN networking ON networking.id = source_and_conns.id
                LEFT OUTER JOIN format_conns ON format_conns.id = source_and_conns.id
                LEFT OUTER JOIN format_conn_deps ON format_conn_deps.id = source_and_conns.id
                GROUP BY source_and_conns.id
            ),

            sinks AS (
                SELECT
                'sink' AS type,
                1 AS value,
                sink_and_conns.id AS id,
                MAX(type) AS object_type,
                COALESCE(MAX(connection_type), 'none') AS connection_type,
                COALESCE(MAX(envelope_type), 'none') AS envelope_type,
                STRING_AGG(
                    DISTINCT COALESCE(networking_type, 'none'),
                    ','
                    ORDER BY COALESCE(networking_type, 'none') ASC
                ) AS networking_type,
                -- Sinks can only have 1 format connection but we aggregate
                -- for consistency.
                STRING_AGG(
                    DISTINCT COALESCE(format_connection, 'none'),
                    ','
                    ORDER BY COALESCE(format_connection, 'none') ASC
                ) AS format_connection,
                STRING_AGG(
                    DISTINCT COALESCE(format_connection_networking, 'none'),
                    ','
                    ORDER BY COALESCE(format_connection_networking, 'none') ASC
                ) AS format_connection_networking,
                MAX(cluster_id) AS cluster_id
                FROM sink_and_conns
                LEFT OUTER JOIN networking ON networking.id = sink_and_conns.id
                LEFT OUTER JOIN format_conns ON format_conns.id = sink_and_conns.id
                LEFT OUTER JOIN format_conn_deps ON format_conn_deps.id = sink_and_conns.id
                GROUP BY sink_and_conns.id
            ),

            -- everything without replicas
            together AS (
                SELECT * FROM sources
                UNION ALL
                SELECT * from sinks
            ),

            with_cluster_replicas AS (
                SELECT
                -- `together.*` doesn't work because we need to aggregate the columns :(
                MAX(together.id) AS id,
                MAX(together.type) AS type,
                MAX(together.object_type) AS object_type,
                -- We just report 1 to the gauge. The `replica_id` labels aggregates the 0-many replicas associated
                -- with this object.
                MAX(together.value) AS value,
                MAX(together.connection_type) AS connection_type,
                MAX(together.envelope_type) AS envelope_type,
                MAX(together.networking_type) AS networking_type,
                MAX(together.format_connection) AS format_connection,
                MAX(together.format_connection_networking) AS format_connection_networking,
                MAX(together.cluster_id) AS cluster_id,
                mcr.id AS replica_id,
                -- Coalesce to 0 when there is no replica. This ensures `with_pod` below will generate
                -- 1 row for the object. Also, -1 because `generate_series` is inclusive.
                COALESCE((mcrs.processes - 1)::int, 0) AS processes
                FROM together
                LEFT OUTER JOIN mz_cluster_replicas mcr ON together.cluster_id = mcr.cluster_id
                LEFT OUTER JOIN mz_catalog.mz_cluster_replica_sizes mcrs ON mcr.size = mcrs.size
                GROUP BY together.id, mcr.id, mcrs.processes
            ),

            with_pod AS (
                SELECT
                id,
                type,
                object_type,
                value,
                connection_type,
                envelope_type,
                networking_type,
                format_connection,
                format_connection_networking,
                cluster_id,
                COALESCE(replica_id, 'none') AS replica_id,
                -- When `replica_id` is NULL`, this coalesces to `none`.
                COALESCE('cluster-' || cluster_id || '-replica-' || replica_id || '-' || generated, 'none') AS synthesized_pod
                FROM with_cluster_replicas, generate_series(0, processes) generated
            )

        SELECT * FROM with_pod;
        ",
    },
];
