// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Materialize Catalog Ontology — static data for `mz_ontology` system tables.
//!
//! A set of tables that describe the Materialize system catalog — what entities
//! exist, how they relate, what columns mean, and what semantic ID type each
//! column uses.
//!
//! # Schema
//!
//! Four tables in the `mz_ontology` schema:
//!
//! - **`mz_ontology_entity_types`** `(name, relation, properties, description)` —
//!   what kinds of things exist. `properties` jsonb has `{"primary_key": ["id"]}`.
//! - **`mz_ontology_semantic_types`** `(name, sql_type, description)` — typed ID
//!   domains and other semantic column types (CatalogItemId, GlobalId, ByteCount, etc.)
//! - **`mz_ontology_properties`** `(entity_type, column_name, semantic_type, description)` —
//!   maps every column to its semantic type and describes what it means.
//! - **`mz_ontology_link_types`** `(name, source_entity, target_entity, properties, description)` —
//!   named relationships. `properties` jsonb has freeform keywords:
//!   - `"kind": "foreign_key"` — column-level join with source_column, target_column, cardinality
//!   - `"kind": "measures"` — a measurement/metric relationship
//!   - `"kind": "depends_on"` — a dependency relationship
//!   - `"kind": "maps_to"` — an ID mapping (e.g., CatalogItemId ↔ GlobalId)
//!   - `"kind": "union"` — a UNION view includes another entity type
//!
//! For usage guide, example queries, and LLM instructions, see
//! `doc/developer/mz_ontology.md`.

use mz_catalog::builtin::{
    BuiltinTable, MZ_ONTOLOGY_ENTITY_TYPES, MZ_ONTOLOGY_LINK_TYPES, MZ_ONTOLOGY_PROPERTIES,
    MZ_ONTOLOGY_SEMANTIC_TYPES,
};
use mz_repr::adt::jsonb::JsonbPacker;
use mz_repr::{Datum, Diff, Row};

use crate::catalog::BuiltinTableUpdate;

/// Pack all ontology table updates (entity_types, semantic_types, properties, link_types).
pub fn pack_ontology_updates() -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
    let mut updates = Vec::new();
    updates.extend(pack_entity_types());
    updates.extend(pack_semantic_types());
    updates.extend(pack_properties());
    updates.extend(pack_link_types());
    updates
}

fn entity_type_row(
    name: &str,
    relation: &str,
    properties: Option<&str>,
    description: &str,
) -> BuiltinTableUpdate<&'static BuiltinTable> {
    let mut row = Row::default();
    let mut packer = row.packer();
    packer.push(Datum::String(name));
    packer.push(Datum::String(relation));
    match properties {
        Some(j) => JsonbPacker::new(&mut packer)
            .pack_str(j)
            .expect("valid json"),
        None => packer.push(Datum::Null),
    }
    packer.push(Datum::String(description));
    BuiltinTableUpdate::row(&MZ_ONTOLOGY_ENTITY_TYPES, row, Diff::ONE)
}

fn semantic_type_row(
    name: &str,
    sql_type: &str,
    description: &str,
) -> BuiltinTableUpdate<&'static BuiltinTable> {
    let row = Row::pack_slice(&[
        Datum::String(name),
        Datum::String(sql_type),
        Datum::String(description),
    ]);
    BuiltinTableUpdate::row(&MZ_ONTOLOGY_SEMANTIC_TYPES, row, Diff::ONE)
}

fn property_row(
    entity_type: &str,
    column_name: &str,
    semantic_type: Option<&str>,
    description: &str,
) -> BuiltinTableUpdate<&'static BuiltinTable> {
    let row = Row::pack_slice(&[
        Datum::String(entity_type),
        Datum::String(column_name),
        match semantic_type {
            Some(s) => Datum::String(s),
            None => Datum::Null,
        },
        Datum::String(description),
    ]);
    BuiltinTableUpdate::row(&MZ_ONTOLOGY_PROPERTIES, row, Diff::ONE)
}

fn link_type_row(
    name: &str,
    source_entity: &str,
    target_entity: &str,
    properties: Option<&str>,
    description: &str,
) -> BuiltinTableUpdate<&'static BuiltinTable> {
    let mut row = Row::default();
    let mut packer = row.packer();
    packer.push(Datum::String(name));
    packer.push(Datum::String(source_entity));
    packer.push(Datum::String(target_entity));
    match properties {
        Some(j) => JsonbPacker::new(&mut packer)
            .pack_str(j)
            .expect("valid json"),
        None => packer.push(Datum::Null),
    }
    packer.push(Datum::String(description));
    BuiltinTableUpdate::row(&MZ_ONTOLOGY_LINK_TYPES, row, Diff::ONE)
}

/// Raw entity type data: (name, relation, properties_json, description).
///
/// Extracted so the validation test can check that each `relation` corresponds
/// to an actual builtin table/view and that ontology properties reference valid
/// columns.
fn entity_type_data() -> Vec<(
    &'static str,
    &'static str,
    Option<&'static str>,
    &'static str,
)> {
    vec![
        // Core object types
        (
            "database",
            "mz_catalog.mz_databases",
            Some(r#"{"primary_key": ["id"]}"#),
            "A top-level namespace that contains schemas",
        ),
        (
            "schema",
            "mz_catalog.mz_schemas",
            Some(r#"{"primary_key": ["id"]}"#),
            "A namespace within a database that contains objects",
        ),
        (
            "role",
            "mz_catalog.mz_roles",
            Some(r#"{"primary_key": ["id"]}"#),
            "A user or role for authentication and access control",
        ),
        (
            "cluster",
            "mz_catalog.mz_clusters",
            Some(r#"{"primary_key": ["id"]}"#),
            "A compute cluster that runs dataflows for sources, sinks, MVs, and indexes",
        ),
        (
            "replica",
            "mz_catalog.mz_cluster_replicas",
            Some(r#"{"primary_key": ["id"]}"#),
            "A physical replica of a cluster providing fault tolerance",
        ),
        // Data object types
        (
            "table",
            "mz_catalog.mz_tables",
            Some(r#"{"primary_key": ["id"]}"#),
            "A user-writable table that can be inserted into and updated",
        ),
        (
            "source",
            "mz_catalog.mz_sources",
            Some(r#"{"primary_key": ["id"]}"#),
            "An external data source ingested into Materialize (e.g., Kafka, Postgres)",
        ),
        (
            "view",
            "mz_catalog.mz_views",
            Some(r#"{"primary_key": ["id"]}"#),
            "A non-materialized view defined by a SQL query",
        ),
        (
            "mv",
            "mz_catalog.mz_materialized_views",
            Some(r#"{"primary_key": ["id"]}"#),
            "A materialized view maintained incrementally on a cluster",
        ),
        (
            "index",
            "mz_catalog.mz_indexes",
            Some(r#"{"primary_key": ["id"]}"#),
            "An in-memory index on a relation for fast lookups",
        ),
        (
            "sink",
            "mz_catalog.mz_sinks",
            Some(r#"{"primary_key": ["id"]}"#),
            "An export of data from Materialize to an external system",
        ),
        // Infrastructure types
        (
            "connection",
            "mz_catalog.mz_connections",
            Some(r#"{"primary_key": ["id"]}"#),
            "A reusable connection configuration to an external system",
        ),
        (
            "secret",
            "mz_catalog.mz_secrets",
            Some(r#"{"primary_key": ["id"]}"#),
            "An encrypted secret value used by connections",
        ),
        // Type system
        (
            "type",
            "mz_catalog.mz_types",
            Some(r#"{"primary_key": ["id"]}"#),
            "A named data type (base, array, list, map, or pseudo)",
        ),
        (
            "array_type",
            "mz_catalog.mz_array_types",
            Some(r#"{"primary_key": ["id"]}"#),
            "An array type with its element type",
        ),
        (
            "list_type",
            "mz_catalog.mz_list_types",
            Some(r#"{"primary_key": ["id"]}"#),
            "A list type with its element type",
        ),
        (
            "map_type",
            "mz_catalog.mz_map_types",
            Some(r#"{"primary_key": ["id"]}"#),
            "A map type with its key and value types",
        ),
        (
            "base_type",
            "mz_catalog.mz_base_types",
            Some(r#"{"primary_key": ["id"]}"#),
            "A primitive/base data type",
        ),
        (
            "pseudo_type",
            "mz_catalog.mz_pseudo_types",
            Some(r#"{"primary_key": ["id"]}"#),
            "A pseudo-type used in function signatures",
        ),
        (
            "function",
            "mz_catalog.mz_functions",
            Some(r#"{"primary_key": ["id"]}"#),
            "A built-in or user-defined function",
        ),
        (
            "operator",
            "mz_catalog.mz_operators",
            None,
            "A built-in SQL operator",
        ),
        // Composite / union views
        (
            "relation",
            "mz_catalog.mz_relations",
            Some(r#"{"primary_key": ["id"]}"#),
            "Union of all relation types: tables, sources, views, MVs (convenience view)",
        ),
        (
            "object",
            "mz_catalog.mz_objects",
            Some(r#"{"primary_key": ["id"]}"#),
            "Union of all object types: relations, indexes, connections, etc. (convenience view)",
        ),
        // Columns and index columns
        (
            "column",
            "mz_catalog.mz_columns",
            None,
            "A column of a relation, with its name, position, type, and nullability",
        ),
        (
            "index_column",
            "mz_catalog.mz_index_columns",
            None,
            "A column or expression in an index, with its position",
        ),
        // Access control
        (
            "role_member",
            "mz_catalog.mz_role_members",
            None,
            "A membership grant: one role is a member of another role",
        ),
        (
            "role_parameter",
            "mz_catalog.mz_role_parameters",
            None,
            "A session parameter default set for a role",
        ),
        (
            "default_privilege",
            "mz_catalog.mz_default_privileges",
            None,
            "A default privilege rule applied to newly created objects",
        ),
        (
            "system_privilege",
            "mz_catalog.mz_system_privileges",
            None,
            "A system-level privilege grant",
        ),
        // Connection details
        (
            "kafka_connection",
            "mz_catalog.mz_kafka_connections",
            None,
            "Kafka-specific connection configuration (brokers, progress topic)",
        ),
        (
            "ssh_tunnel",
            "mz_catalog.mz_ssh_tunnel_connections",
            None,
            "SSH tunnel connection with public keys",
        ),
        (
            "aws_privatelink",
            "mz_catalog.mz_aws_privatelink_connections",
            None,
            "AWS PrivateLink connection configuration",
        ),
        // Source/sink details
        (
            "kafka_source",
            "mz_catalog.mz_kafka_sources",
            None,
            "Kafka-specific source configuration (topic, group ID)",
        ),
        (
            "kafka_sink",
            "mz_catalog.mz_kafka_sinks",
            Some(r#"{"primary_key": ["id"]}"#),
            "Kafka-specific sink configuration (topic)",
        ),
        (
            "iceberg_sink",
            "mz_catalog.mz_iceberg_sinks",
            None,
            "Iceberg-specific sink configuration (namespace, table)",
        ),
        // Operational
        (
            "audit_event",
            "mz_catalog.mz_audit_events",
            Some(r#"{"primary_key": ["id"]}"#),
            "An audit log entry recording a DDL operation",
        ),
        (
            "storage_usage",
            "mz_catalog.mz_storage_usage",
            Some(r#"{"primary_key": ["object_id", "collection_timestamp"]}"#),
            "Historical storage usage per object over time",
        ),
        (
            "recent_storage",
            "mz_catalog.mz_recent_storage_usage",
            Some(r#"{"primary_key": ["object_id"]}"#),
            "Most recent storage usage snapshot per object",
        ),
        // Cluster sizing
        (
            "replica_size",
            "mz_catalog.mz_cluster_replica_sizes",
            Some(r#"{"primary_key": ["size"]}"#),
            "Available cluster replica sizes with CPU, memory, and credit cost",
        ),
        // Misc
        (
            "egress_ip",
            "mz_catalog.mz_egress_ips",
            None,
            "IP addresses used for outbound connections from Materialize",
        ),
        // mz_internal entities
        (
            "object_dependency",
            "mz_internal.mz_object_dependencies",
            None,
            "A dependency edge: one object depends on another",
        ),
        (
            "object_global_id",
            "mz_internal.mz_object_global_ids",
            None,
            "Mapping between CatalogItemId (SQL layer) and GlobalId (runtime layer)",
        ),
        (
            "comment",
            "mz_internal.mz_comments",
            None,
            "A COMMENT ON annotation for a catalog object or column",
        ),
        (
            "object_history",
            "mz_internal.mz_object_history",
            None,
            "Historical record of object creation and drops",
        ),
        (
            "object_lifetime",
            "mz_internal.mz_object_lifetimes",
            None,
            "Computed lifetime span (created_at to dropped_at) for objects",
        ),
        (
            "object_fqn",
            "mz_internal.mz_object_fully_qualified_names",
            None,
            "Fully qualified name (database.schema.name) for objects",
        ),
        (
            "continual_task",
            "mz_internal.mz_continual_tasks",
            None,
            "A continual task definition",
        ),
        (
            "replacement",
            "mz_internal.mz_replacements",
            None,
            "A record of an object replacement (ALTER ... SWAP)",
        ),
        (
            "aggregate",
            "mz_internal.mz_aggregates",
            None,
            "Aggregate function metadata",
        ),
        (
            "history_retention",
            "mz_internal.mz_history_retention_strategies",
            None,
            "History retention strategy for an object",
        ),
        (
            "license_key",
            "mz_internal.mz_license_keys",
            None,
            "License key metadata",
        ),
        // Cluster & replica status/metrics
        (
            "replica_status_history",
            "mz_internal.mz_cluster_replica_status_history",
            None,
            "Historical replica status events (ready, not-ready, etc.)",
        ),
        (
            "replica_status",
            "mz_internal.mz_cluster_replica_statuses",
            None,
            "Current status of each replica",
        ),
        (
            "replica_metrics",
            "mz_internal.mz_cluster_replica_metrics",
            None,
            "CPU and memory metrics per replica",
        ),
        (
            "replica_utilization",
            "mz_internal.mz_cluster_replica_utilization",
            None,
            "Computed utilization metrics per replica",
        ),
        (
            "replica_history",
            "mz_internal.mz_cluster_replica_history",
            None,
            "Historical record of replica creation/drops",
        ),
        (
            "replica_name_history",
            "mz_internal.mz_cluster_replica_name_history",
            None,
            "Historical replica names",
        ),
        (
            "cluster_schedule",
            "mz_internal.mz_cluster_schedules",
            None,
            "Cluster scheduling configuration",
        ),
        (
            "cluster_deployment",
            "mz_internal.mz_cluster_deployment_lineage",
            None,
            "Cluster deployment lineage information",
        ),
        // Compute
        (
            "compute_dependency",
            "mz_internal.mz_compute_dependencies",
            None,
            "Dependency edges within compute dataflows",
        ),
        (
            "compute_hydration_time",
            "mz_internal.mz_compute_hydration_times",
            None,
            "Time to hydrate compute objects",
        ),
        (
            "compute_hydration_status",
            "mz_internal.mz_compute_operator_hydration_statuses",
            None,
            "Hydration status per compute operator",
        ),
        (
            "hydration_status",
            "mz_internal.mz_hydration_statuses",
            None,
            "Overall hydration status per object",
        ),
        (
            "compute_hydration_status_view",
            "mz_internal.mz_compute_hydration_statuses",
            None,
            "Computed hydration status per compute object",
        ),
        // Frontiers & lag
        (
            "frontier",
            "mz_internal.mz_frontiers",
            None,
            "Current read/write frontiers per object (source)",
        ),
        (
            "global_frontier",
            "mz_internal.mz_global_frontiers",
            None,
            "Aggregated frontiers across all replicas",
        ),
        (
            "wallclock_lag_history",
            "mz_internal.mz_wallclock_lag_history",
            None,
            "Historical wallclock lag per object (source)",
        ),
        (
            "wallclock_global_lag",
            "mz_internal.mz_wallclock_global_lag",
            None,
            "Current wallclock lag aggregated across replicas",
        ),
        (
            "wallclock_global_lag_history",
            "mz_internal.mz_wallclock_global_lag_history",
            None,
            "Historical global wallclock lag",
        ),
        (
            "materialization_lag",
            "mz_internal.mz_materialization_lag",
            None,
            "Lag between a materialization and its inputs",
        ),
        (
            "materialization_dep",
            "mz_internal.mz_materialization_dependencies",
            None,
            "Dependencies between materializations",
        ),
        // Storage
        (
            "source_statistics",
            "mz_internal.mz_source_statistics",
            None,
            "Aggregated source ingestion statistics",
        ),
        (
            "source_status",
            "mz_internal.mz_source_statuses",
            None,
            "Current source status (running, stalled, etc.)",
        ),
        (
            "source_status_history",
            "mz_internal.mz_source_status_history",
            None,
            "Historical source status events",
        ),
        (
            "sink_statistics",
            "mz_internal.mz_sink_statistics",
            None,
            "Aggregated sink export statistics",
        ),
        (
            "sink_status",
            "mz_internal.mz_sink_statuses",
            None,
            "Current sink status",
        ),
        (
            "sink_status_history",
            "mz_internal.mz_sink_status_history",
            None,
            "Historical sink status events",
        ),
        (
            "storage_shard",
            "mz_internal.mz_storage_shards",
            None,
            "Persist shards used by storage objects",
        ),
        (
            "storage_usage_by_shard",
            "mz_internal.mz_storage_usage_by_shard",
            None,
            "Storage usage broken down by shard",
        ),
        // Source-type-specific
        (
            "kafka_source_table",
            "mz_internal.mz_kafka_source_tables",
            None,
            "Kafka source table-level details",
        ),
        (
            "postgres_source_table",
            "mz_internal.mz_postgres_source_tables",
            None,
            "Postgres source table-level details",
        ),
        (
            "postgres_source",
            "mz_internal.mz_postgres_sources",
            None,
            "Postgres source-level details",
        ),
        (
            "mysql_source_table",
            "mz_internal.mz_mysql_source_tables",
            None,
            "MySQL source table-level details",
        ),
        (
            "sql_server_source_table",
            "mz_internal.mz_sql_server_source_tables",
            None,
            "SQL Server source table-level details",
        ),
        (
            "source_reference",
            "mz_internal.mz_source_references",
            None,
            "External references tracked by sources",
        ),
        (
            "webhook_source",
            "mz_internal.mz_webhook_sources",
            None,
            "Webhook source configuration",
        ),
        // Session & query activity
        (
            "session_history",
            "mz_internal.mz_session_history",
            None,
            "Historical session connection events",
        ),
        (
            "session",
            "mz_internal.mz_sessions",
            None,
            "Currently active sessions",
        ),
        (
            "sql_text",
            "mz_internal.mz_sql_text",
            None,
            "Raw SQL text of executed statements",
        ),
        (
            "recent_sql_text",
            "mz_internal.mz_recent_sql_text",
            None,
            "Recent SQL text (indexed, last 3 days)",
        ),
        (
            "activity_log",
            "mz_internal.mz_recent_activity_log",
            None,
            "Recent query activity with execution stats",
        ),
        (
            "subscription",
            "mz_internal.mz_subscriptions",
            None,
            "Active SUBSCRIBE operations",
        ),
        (
            "statement_lifecycle",
            "mz_internal.mz_statement_lifecycle_history",
            None,
            "Statement lifecycle events (parse, bind, execute)",
        ),
        // Network policies
        (
            "network_policy",
            "mz_internal.mz_network_policies",
            None,
            "Network access policies",
        ),
        (
            "network_policy_rule",
            "mz_internal.mz_network_policy_rules",
            None,
            "Individual rules within a network policy",
        ),
        // AWS
        (
            "aws_connection",
            "mz_internal.mz_aws_connections",
            None,
            "AWS connection configuration details",
        ),
        (
            "privatelink_status",
            "mz_internal.mz_aws_privatelink_connection_statuses",
            None,
            "PrivateLink connection health status",
        ),
        // mz_introspection entities
        (
            "arrangement_size",
            "mz_introspection.mz_arrangement_sizes",
            None,
            "Aggregated arrangement sizes (records, batches, bytes)",
        ),
        (
            "arrangement_sharing",
            "mz_introspection.mz_arrangement_sharing",
            None,
            "Arrangement sharing between operators",
        ),
        (
            "group_size_advice",
            "mz_introspection.mz_expected_group_size_advice",
            None,
            "Advice on expected group sizes for reduce operators",
        ),
        (
            "dataflow",
            "mz_introspection.mz_dataflows",
            None,
            "Dataflow instances",
        ),
        (
            "dataflow_operator",
            "mz_introspection.mz_dataflow_operators",
            None,
            "Operators within dataflows",
        ),
        (
            "dataflow_address",
            "mz_introspection.mz_dataflow_addresses",
            None,
            "Address (scope path) of dataflow operators",
        ),
        (
            "dataflow_channel",
            "mz_introspection.mz_dataflow_channels",
            None,
            "Communication channels between operators",
        ),
        (
            "dataflow_operator_dataflow",
            "mz_introspection.mz_dataflow_operator_dataflows",
            None,
            "Mapping of operators to their parent dataflow",
        ),
        (
            "compute_export",
            "mz_introspection.mz_compute_exports",
            None,
            "Compute exports (maintained collections)",
        ),
        (
            "compute_frontier",
            "mz_introspection.mz_compute_frontiers",
            None,
            "Per-replica compute frontiers",
        ),
        (
            "compute_import_frontier",
            "mz_introspection.mz_compute_import_frontiers",
            None,
            "Import frontiers for compute dependencies",
        ),
        (
            "compute_error_count",
            "mz_introspection.mz_compute_error_counts",
            None,
            "Error counts per compute collection",
        ),
        (
            "lir_mapping",
            "mz_introspection.mz_lir_mapping",
            None,
            "LIR (low-level IR) to dataflow operator mapping",
        ),
        (
            "records_per_dataflow",
            "mz_introspection.mz_records_per_dataflow",
            None,
            "Record counts aggregated per dataflow",
        ),
        (
            "mappable_object",
            "mz_introspection.mz_mappable_objects",
            None,
            "Objects that can be mapped to dataflow operators",
        ),
        (
            "scheduling_elapsed",
            "mz_introspection.mz_scheduling_elapsed",
            None,
            "CPU time spent per operator",
        ),
        (
            "scheduling_parks",
            "mz_introspection.mz_scheduling_parks_histogram",
            None,
            "Histogram of operator park durations",
        ),
        (
            "peek_duration",
            "mz_introspection.mz_peek_durations_histogram",
            None,
            "Histogram of SELECT query durations",
        ),
        (
            "active_peek",
            "mz_introspection.mz_active_peeks",
            None,
            "Currently executing SELECT queries",
        ),
        (
            "message_count",
            "mz_introspection.mz_message_counts",
            None,
            "Inter-worker message counts",
        ),
        (
            "transitive_dependency",
            "mz_internal.mz_object_transitive_dependencies",
            Some(r#"{"primary_key": ["object_id", "referenced_object_id"]}"#),
            "Transitive closure of object dependencies — all direct and indirect dependencies",
        ),
    ]
}

fn pack_entity_types() -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
    entity_type_data()
        .into_iter()
        .map(|(name, relation, properties, description)| {
            entity_type_row(name, relation, properties, description)
        })
        .collect()
}

fn pack_semantic_types() -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
    vec![
        semantic_type_row(
            "CatalogItemId",
            "text",
            "SQL-layer identifier for a catalog object (table, view, MV, source, sink, index, connection, secret, type, function). Format: s{n}/u{n}. This is the ID users see in mz_objects.id, mz_tables.id, etc. Rust: CatalogItemId in src/repr/src/catalog_item_id.rs",
        ),
        semantic_type_row(
            "GlobalId",
            "text",
            "Runtime identifier used by compute and storage layers. A single CatalogItemId can map to multiple GlobalIds over time (e.g., after ALTER). Format: s{n}/u{n}/si{n}. Appears in mz_internal introspection tables. Mapped via mz_object_global_ids. Rust: GlobalId in src/repr/src/global_id.rs",
        ),
        semantic_type_row(
            "ClusterId",
            "text",
            "Identifies a compute cluster. Format: s{n}/u{n}. Rust: ClusterId (alias for StorageInstanceId) in src/controller-types/src/lib.rs",
        ),
        semantic_type_row(
            "ReplicaId",
            "text",
            "Identifies a cluster replica. Format: s{n}/u{n}. Rust: ReplicaId in src/cluster-client/src/lib.rs",
        ),
        semantic_type_row(
            "SchemaId",
            "text",
            "Identifies a schema. Format: s{n}/u{n}. Rust: SchemaId in src/sql/src/names.rs",
        ),
        semantic_type_row(
            "DatabaseId",
            "text",
            "Identifies a database. Format: s{n}/u{n}. Rust: DatabaseId in src/sql/src/names.rs",
        ),
        semantic_type_row(
            "RoleId",
            "text",
            "Identifies a role or user. Format: s{n} (system), g{n} (predefined), u{n} (user), p (public). Rust: RoleId in src/repr/src/role_id.rs",
        ),
        semantic_type_row(
            "NetworkPolicyId",
            "text",
            "Identifies a network policy. Format: s{n}/u{n}. Rust: NetworkPolicyId in src/repr/src/network_policy_id.rs",
        ),
        semantic_type_row(
            "ShardId",
            "text",
            "Identifies a persist shard (durable storage unit). Format: s{uuid}. Rust: ShardId in src/persist-types/src/lib.rs",
        ),
        semantic_type_row(
            "OID",
            "oid",
            "Postgres-compatible object identifier. Numeric. Used for compatibility with PostgreSQL tooling and drivers. Not the primary ID in Materialize.",
        ),
        semantic_type_row(
            "ObjectType",
            "text",
            "The type of a catalog object: table, source, view, materialized-view, sink, index, connection, secret, type, function. Used in mz_objects.type, mz_audit_events.object_type",
        ),
        semantic_type_row(
            "ConnectionType",
            "text",
            "The type of a connection: kafka, postgres, aws, ssh-tunnel, etc. Used in mz_connections.type",
        ),
        semantic_type_row(
            "SourceType",
            "text",
            "The type of a source: kafka, postgres, load-generator, etc. Used in mz_sources.type",
        ),
        semantic_type_row(
            "MzTimestamp",
            "mz_timestamp",
            "Internal logical timestamp used by Materialize for consistency. A uint64. Different from wall clock timestamps. Used in frontier tracking.",
        ),
        semantic_type_row(
            "WallclockTimestamp",
            "timestamp with time zone",
            "A wall clock timestamp. Used for audit events, storage usage timestamps, etc.",
        ),
        semantic_type_row(
            "ByteCount",
            "uint8",
            "A count of bytes. Used for storage sizes, memory usage, arrangement sizes.",
        ),
        semantic_type_row(
            "RecordCount",
            "uint8",
            "A count of records/rows. Used for statistics and arrangement info.",
        ),
        semantic_type_row(
            "CreditRate",
            "numeric",
            "Credits consumed per hour. Used in mz_cluster_replica_sizes.credits_per_hour.",
        ),
        semantic_type_row(
            "SqlDefinition",
            "text",
            "A SQL CREATE statement. Used in create_sql columns across object types.",
        ),
        semantic_type_row(
            "RedactedSqlDefinition",
            "text",
            "A SQL CREATE statement with sensitive values redacted. Used in redacted_create_sql columns.",
        ),
    ]
}

/// Raw property data: (entity_type, column_name, semantic_type, description).
///
/// Extracted so the validation test can check that each `column_name` exists
/// in the actual builtin table/view referenced by the entity type.
fn property_data() -> Vec<(
    &'static str,
    &'static str,
    Option<&'static str>,
    &'static str,
)> {
    vec![
        // mz_databases
        (
            "database",
            "id",
            Some("DatabaseId"),
            "Unique database identifier. PK.",
        ),
        ("database", "oid", Some("OID"), "PostgreSQL-compatible OID."),
        ("database", "name", None, "The name of the database."),
        (
            "database",
            "owner_id",
            Some("RoleId"),
            "Owner of the database. FK -> mz_roles.id.",
        ),
        (
            "database",
            "privileges",
            None,
            "ACL privileges on the database.",
        ),
        // mz_schemas
        (
            "schema",
            "id",
            Some("SchemaId"),
            "Unique schema identifier. PK.",
        ),
        ("schema", "oid", Some("OID"), "PostgreSQL-compatible OID."),
        (
            "schema",
            "database_id",
            Some("DatabaseId"),
            "Parent database. FK -> mz_databases.id. NULL for system schemas.",
        ),
        ("schema", "name", None, "The name of the schema."),
        (
            "schema",
            "owner_id",
            Some("RoleId"),
            "Owner of the schema. FK -> mz_roles.id.",
        ),
        (
            "schema",
            "privileges",
            None,
            "ACL privileges on the schema.",
        ),
        // mz_roles
        ("role", "id", Some("RoleId"), "Unique role identifier. PK."),
        ("role", "oid", Some("OID"), "PostgreSQL-compatible OID."),
        ("role", "name", None, "The name of the role."),
        (
            "role",
            "inherit",
            None,
            "Whether the role inherits privileges.",
        ),
        ("role", "rolcanlogin", None, "Whether the role can log in."),
        ("role", "rolsuper", None, "Whether the role is a superuser."),
        // mz_clusters
        (
            "cluster",
            "id",
            Some("ClusterId"),
            "Unique cluster identifier. PK.",
        ),
        ("cluster", "name", None, "The name of the cluster."),
        (
            "cluster",
            "owner_id",
            Some("RoleId"),
            "Owner of the cluster. FK -> mz_roles.id.",
        ),
        (
            "cluster",
            "privileges",
            None,
            "ACL privileges on the cluster.",
        ),
        (
            "cluster",
            "managed",
            None,
            "Whether this is a managed cluster.",
        ),
        (
            "cluster",
            "size",
            None,
            "Replica size for managed clusters. NULL for unmanaged.",
        ),
        (
            "cluster",
            "replication_factor",
            None,
            "Number of replicas for managed clusters. NULL for unmanaged.",
        ),
        (
            "cluster",
            "disk",
            None,
            "Whether replicas have disk. NULL for unmanaged.",
        ),
        (
            "cluster",
            "availability_zones",
            None,
            "Availability zones for managed clusters.",
        ),
        (
            "cluster",
            "introspection_debugging",
            None,
            "Whether introspection debugging is enabled.",
        ),
        (
            "cluster",
            "introspection_interval",
            None,
            "Interval for introspection data collection.",
        ),
        // mz_cluster_replicas
        (
            "replica",
            "id",
            Some("ReplicaId"),
            "Unique replica identifier.",
        ),
        ("replica", "name", None, "The name of the replica."),
        (
            "replica",
            "cluster_id",
            Some("ClusterId"),
            "Parent cluster. FK -> mz_clusters.id.",
        ),
        ("replica", "size", None, "Replica size (e.g., 25cc, 100cc)."),
        (
            "replica",
            "availability_zone",
            None,
            "The AZ where the replica runs.",
        ),
        (
            "replica",
            "owner_id",
            Some("RoleId"),
            "Owner of the replica. FK -> mz_roles.id.",
        ),
        (
            "replica",
            "disk",
            None,
            "Whether the replica has local disk.",
        ),
        // mz_tables
        (
            "table",
            "id",
            Some("CatalogItemId"),
            "Unique table identifier. PK.",
        ),
        ("table", "oid", Some("OID"), "PostgreSQL-compatible OID."),
        (
            "table",
            "schema_id",
            Some("SchemaId"),
            "Parent schema. FK -> mz_schemas.id.",
        ),
        ("table", "name", None, "The name of the table."),
        (
            "table",
            "owner_id",
            Some("RoleId"),
            "Owner. FK -> mz_roles.id.",
        ),
        ("table", "privileges", None, "ACL privileges."),
        (
            "table",
            "create_sql",
            Some("SqlDefinition"),
            "CREATE TABLE statement.",
        ),
        (
            "table",
            "redacted_create_sql",
            Some("RedactedSqlDefinition"),
            "Redacted CREATE TABLE statement.",
        ),
        (
            "table",
            "source_id",
            Some("CatalogItemId"),
            "Source that created this table (e.g., Postgres replication). FK -> mz_sources.id. NULL for user-created tables.",
        ),
        // mz_sources
        (
            "source",
            "id",
            Some("CatalogItemId"),
            "Unique source identifier. PK.",
        ),
        ("source", "oid", Some("OID"), "PostgreSQL-compatible OID."),
        (
            "source",
            "schema_id",
            Some("SchemaId"),
            "Parent schema. FK -> mz_schemas.id.",
        ),
        ("source", "name", None, "The name of the source."),
        (
            "source",
            "type",
            Some("SourceType"),
            "Source type: kafka, postgres, mysql, load-generator, progress, subsource.",
        ),
        (
            "source",
            "connection_id",
            Some("CatalogItemId"),
            "Connection used by this source. FK -> mz_connections.id.",
        ),
        ("source", "size", None, "Deprecated. Source size."),
        (
            "source",
            "envelope_type",
            None,
            "Kafka envelope: none, upsert, debezium. NULL for non-Kafka.",
        ),
        (
            "source",
            "key_format",
            None,
            "Kafka key format: avro, json, bytes, text. NULL for non-Kafka.",
        ),
        (
            "source",
            "value_format",
            None,
            "Kafka value format: avro, json, bytes, text. NULL for non-Kafka.",
        ),
        (
            "source",
            "cluster_id",
            Some("ClusterId"),
            "Cluster running this source. FK -> mz_clusters.id. NULL for subsources.",
        ),
        (
            "source",
            "owner_id",
            Some("RoleId"),
            "Owner. FK -> mz_roles.id.",
        ),
        ("source", "privileges", None, "ACL privileges."),
        (
            "source",
            "create_sql",
            Some("SqlDefinition"),
            "CREATE SOURCE statement.",
        ),
        (
            "source",
            "redacted_create_sql",
            Some("RedactedSqlDefinition"),
            "Redacted CREATE SOURCE statement.",
        ),
        // mz_views
        (
            "view",
            "id",
            Some("CatalogItemId"),
            "Unique view identifier. PK.",
        ),
        ("view", "oid", Some("OID"), "PostgreSQL-compatible OID."),
        (
            "view",
            "schema_id",
            Some("SchemaId"),
            "Parent schema. FK -> mz_schemas.id.",
        ),
        ("view", "name", None, "The name of the view."),
        (
            "view",
            "definition",
            Some("SqlDefinition"),
            "The SELECT query defining the view.",
        ),
        (
            "view",
            "owner_id",
            Some("RoleId"),
            "Owner. FK -> mz_roles.id.",
        ),
        ("view", "privileges", None, "ACL privileges."),
        (
            "view",
            "create_sql",
            Some("SqlDefinition"),
            "CREATE VIEW statement.",
        ),
        (
            "view",
            "redacted_create_sql",
            Some("RedactedSqlDefinition"),
            "Redacted CREATE VIEW statement.",
        ),
        // mz_materialized_views
        (
            "mv",
            "id",
            Some("CatalogItemId"),
            "Unique MV identifier. PK.",
        ),
        ("mv", "oid", Some("OID"), "PostgreSQL-compatible OID."),
        (
            "mv",
            "schema_id",
            Some("SchemaId"),
            "Parent schema. FK -> mz_schemas.id.",
        ),
        ("mv", "name", None, "The name of the materialized view."),
        (
            "mv",
            "cluster_id",
            Some("ClusterId"),
            "Cluster maintaining this MV. FK -> mz_clusters.id.",
        ),
        (
            "mv",
            "definition",
            Some("SqlDefinition"),
            "The SELECT query defining the MV.",
        ),
        (
            "mv",
            "owner_id",
            Some("RoleId"),
            "Owner. FK -> mz_roles.id.",
        ),
        ("mv", "privileges", None, "ACL privileges."),
        (
            "mv",
            "create_sql",
            Some("SqlDefinition"),
            "CREATE MATERIALIZED VIEW statement.",
        ),
        (
            "mv",
            "redacted_create_sql",
            Some("RedactedSqlDefinition"),
            "Redacted CREATE MATERIALIZED VIEW statement.",
        ),
        // mz_indexes
        (
            "index",
            "id",
            Some("CatalogItemId"),
            "Unique index identifier. PK.",
        ),
        ("index", "oid", Some("OID"), "PostgreSQL-compatible OID."),
        ("index", "name", None, "The name of the index."),
        (
            "index",
            "on_id",
            Some("CatalogItemId"),
            "The relation this index is built on. FK -> any relation.",
        ),
        (
            "index",
            "cluster_id",
            Some("ClusterId"),
            "Cluster maintaining this index. FK -> mz_clusters.id.",
        ),
        (
            "index",
            "owner_id",
            Some("RoleId"),
            "Owner. FK -> mz_roles.id.",
        ),
        (
            "index",
            "create_sql",
            Some("SqlDefinition"),
            "CREATE INDEX statement.",
        ),
        (
            "index",
            "redacted_create_sql",
            Some("RedactedSqlDefinition"),
            "Redacted CREATE INDEX statement.",
        ),
        // mz_sinks
        (
            "sink",
            "id",
            Some("CatalogItemId"),
            "Unique sink identifier. PK.",
        ),
        ("sink", "oid", Some("OID"), "PostgreSQL-compatible OID."),
        (
            "sink",
            "schema_id",
            Some("SchemaId"),
            "Parent schema. FK -> mz_schemas.id.",
        ),
        ("sink", "name", None, "The name of the sink."),
        ("sink", "type", None, "Sink type: kafka."),
        (
            "sink",
            "connection_id",
            Some("CatalogItemId"),
            "Connection used by this sink. FK -> mz_connections.id.",
        ),
        ("sink", "size", None, "Sink size."),
        (
            "sink",
            "envelope_type",
            None,
            "Sink envelope: upsert, debezium.",
        ),
        ("sink", "format", None, "Deprecated. Kafka message format."),
        ("sink", "key_format", None, "Kafka key format."),
        ("sink", "value_format", None, "Kafka value format."),
        (
            "sink",
            "cluster_id",
            Some("ClusterId"),
            "Cluster running this sink. FK -> mz_clusters.id.",
        ),
        (
            "sink",
            "owner_id",
            Some("RoleId"),
            "Owner. FK -> mz_roles.id.",
        ),
        (
            "sink",
            "create_sql",
            Some("SqlDefinition"),
            "CREATE SINK statement.",
        ),
        (
            "sink",
            "redacted_create_sql",
            Some("RedactedSqlDefinition"),
            "Redacted CREATE SINK statement.",
        ),
        // mz_connections
        (
            "connection",
            "id",
            Some("CatalogItemId"),
            "Unique connection identifier. PK.",
        ),
        (
            "connection",
            "oid",
            Some("OID"),
            "PostgreSQL-compatible OID.",
        ),
        (
            "connection",
            "schema_id",
            Some("SchemaId"),
            "Parent schema. FK -> mz_schemas.id.",
        ),
        ("connection", "name", None, "The name of the connection."),
        (
            "connection",
            "type",
            Some("ConnectionType"),
            "Connection type: kafka, postgres, ssh-tunnel, etc.",
        ),
        (
            "connection",
            "owner_id",
            Some("RoleId"),
            "Owner. FK -> mz_roles.id.",
        ),
        ("connection", "privileges", None, "ACL privileges."),
        (
            "connection",
            "create_sql",
            Some("SqlDefinition"),
            "CREATE CONNECTION statement.",
        ),
        (
            "connection",
            "redacted_create_sql",
            Some("RedactedSqlDefinition"),
            "Redacted CREATE CONNECTION statement.",
        ),
        // mz_secrets
        (
            "secret",
            "id",
            Some("CatalogItemId"),
            "Unique secret identifier.",
        ),
        ("secret", "oid", Some("OID"), "PostgreSQL-compatible OID."),
        (
            "secret",
            "schema_id",
            Some("SchemaId"),
            "Parent schema. FK -> mz_schemas.id.",
        ),
        ("secret", "name", None, "The name of the secret."),
        (
            "secret",
            "owner_id",
            Some("RoleId"),
            "Owner. FK -> mz_roles.id.",
        ),
        ("secret", "privileges", None, "ACL privileges."),
        // mz_types
        (
            "type",
            "id",
            Some("CatalogItemId"),
            "Unique type identifier. PK.",
        ),
        ("type", "oid", Some("OID"), "PostgreSQL-compatible OID."),
        (
            "type",
            "schema_id",
            Some("SchemaId"),
            "Parent schema. FK -> mz_schemas.id.",
        ),
        ("type", "name", None, "The name of the type."),
        ("type", "category", None, "Type category."),
        (
            "type",
            "owner_id",
            Some("RoleId"),
            "Owner. FK -> mz_roles.id.",
        ),
        ("type", "privileges", None, "ACL privileges."),
        (
            "type",
            "create_sql",
            Some("SqlDefinition"),
            "CREATE TYPE statement.",
        ),
        (
            "type",
            "redacted_create_sql",
            Some("RedactedSqlDefinition"),
            "Redacted CREATE TYPE statement.",
        ),
        // Type subtypes
        (
            "array_type",
            "id",
            Some("CatalogItemId"),
            "The type ID. FK -> mz_types.id.",
        ),
        (
            "array_type",
            "element_id",
            Some("CatalogItemId"),
            "Element type. FK -> mz_types.id.",
        ),
        (
            "list_type",
            "id",
            Some("CatalogItemId"),
            "The type ID. FK -> mz_types.id.",
        ),
        (
            "list_type",
            "element_id",
            Some("CatalogItemId"),
            "Element type. FK -> mz_types.id.",
        ),
        (
            "list_type",
            "element_modifiers",
            None,
            "Element type modifiers.",
        ),
        (
            "map_type",
            "id",
            Some("CatalogItemId"),
            "The type ID. FK -> mz_types.id.",
        ),
        (
            "map_type",
            "key_id",
            Some("CatalogItemId"),
            "Key type. FK -> mz_types.id.",
        ),
        (
            "map_type",
            "value_id",
            Some("CatalogItemId"),
            "Value type. FK -> mz_types.id.",
        ),
        ("map_type", "key_modifiers", None, "Key type modifiers."),
        ("map_type", "value_modifiers", None, "Value type modifiers."),
        (
            "base_type",
            "id",
            Some("CatalogItemId"),
            "The type ID. FK -> mz_types.id.",
        ),
        (
            "pseudo_type",
            "id",
            Some("CatalogItemId"),
            "The type ID. FK -> mz_types.id.",
        ),
        // mz_functions
        (
            "function",
            "id",
            Some("CatalogItemId"),
            "Unique function identifier.",
        ),
        ("function", "oid", Some("OID"), "PostgreSQL-compatible OID."),
        (
            "function",
            "schema_id",
            Some("SchemaId"),
            "Parent schema. FK -> mz_schemas.id.",
        ),
        ("function", "name", None, "The name of the function."),
        (
            "function",
            "argument_type_ids",
            None,
            "Array of argument type IDs. Each refers to mz_types.id.",
        ),
        (
            "function",
            "variadic_argument_type_id",
            Some("CatalogItemId"),
            "Variadic argument type. FK -> mz_types.id.",
        ),
        (
            "function",
            "return_type_id",
            Some("CatalogItemId"),
            "Return type. FK -> mz_types.id. NULL for void.",
        ),
        (
            "function",
            "returns_set",
            None,
            "Whether this is a table function.",
        ),
        (
            "function",
            "owner_id",
            Some("RoleId"),
            "Owner. FK -> mz_roles.id.",
        ),
        // mz_operators
        ("operator", "oid", Some("OID"), "PostgreSQL-compatible OID."),
        ("operator", "name", None, "Operator name."),
        (
            "operator",
            "argument_type_ids",
            None,
            "Array of argument type IDs.",
        ),
        (
            "operator",
            "return_type_id",
            Some("CatalogItemId"),
            "Return type. FK -> mz_types.id.",
        ),
        // mz_columns
        (
            "column",
            "id",
            Some("CatalogItemId"),
            "The relation this column belongs to. FK -> any relation.",
        ),
        ("column", "name", None, "Column name."),
        (
            "column",
            "position",
            None,
            "1-indexed position in the relation.",
        ),
        (
            "column",
            "nullable",
            None,
            "Whether the column can be NULL.",
        ),
        ("column", "type", None, "SQL data type name."),
        ("column", "default", None, "Default expression."),
        (
            "column",
            "type_oid",
            Some("OID"),
            "OID of the column type. FK -> mz_types.oid.",
        ),
        ("column", "type_mod", None, "Packed type modifier."),
        // mz_index_columns
        (
            "index_column",
            "index_id",
            Some("CatalogItemId"),
            "Parent index. FK -> mz_indexes.id.",
        ),
        (
            "index_column",
            "index_position",
            None,
            "1-indexed position within the index.",
        ),
        (
            "index_column",
            "on_position",
            None,
            "Position in the indexed relation. NULL for expression columns.",
        ),
        (
            "index_column",
            "on_expression",
            None,
            "SQL expression for computed index columns.",
        ),
        (
            "index_column",
            "nullable",
            None,
            "Whether this index column can be NULL.",
        ),
        // mz_role_members
        (
            "role_member",
            "role_id",
            Some("RoleId"),
            "The role being granted membership in. FK -> mz_roles.id.",
        ),
        (
            "role_member",
            "member",
            Some("RoleId"),
            "The role that is a member. FK -> mz_roles.id.",
        ),
        (
            "role_member",
            "grantor",
            Some("RoleId"),
            "The role that granted membership. FK -> mz_roles.id.",
        ),
        // mz_role_parameters
        (
            "role_parameter",
            "role_id",
            Some("RoleId"),
            "The role. FK -> mz_roles.id.",
        ),
        (
            "role_parameter",
            "parameter_name",
            None,
            "Configuration parameter name.",
        ),
        (
            "role_parameter",
            "parameter_value",
            None,
            "Default value for this role.",
        ),
        // mz_default_privileges
        (
            "default_privilege",
            "role_id",
            Some("RoleId"),
            "Creator role. FK -> mz_roles.id. 'p' = PUBLIC.",
        ),
        (
            "default_privilege",
            "database_id",
            Some("DatabaseId"),
            "Scope database. FK -> mz_databases.id. NULL = global.",
        ),
        (
            "default_privilege",
            "schema_id",
            Some("SchemaId"),
            "Scope schema. FK -> mz_schemas.id. NULL = database-wide.",
        ),
        (
            "default_privilege",
            "object_type",
            Some("ObjectType"),
            "Type of object this rule applies to.",
        ),
        (
            "default_privilege",
            "grantee",
            Some("RoleId"),
            "Role receiving privileges. 'p' = PUBLIC.",
        ),
        (
            "default_privilege",
            "privileges",
            None,
            "Set of privileges to grant.",
        ),
        // mz_system_privileges
        (
            "system_privilege",
            "privileges",
            None,
            "System-level privilege grant.",
        ),
        // Connection detail tables
        (
            "kafka_connection",
            "id",
            Some("CatalogItemId"),
            "Connection ID. FK -> mz_connections.id.",
        ),
        (
            "kafka_connection",
            "brokers",
            None,
            "Kafka broker addresses.",
        ),
        (
            "kafka_connection",
            "sink_progress_topic",
            None,
            "Kafka topic for sink progress tracking.",
        ),
        (
            "ssh_tunnel",
            "id",
            Some("CatalogItemId"),
            "Connection ID. FK -> mz_connections.id.",
        ),
        ("ssh_tunnel", "public_key_1", None, "First SSH public key."),
        ("ssh_tunnel", "public_key_2", None, "Second SSH public key."),
        (
            "aws_privatelink",
            "id",
            Some("CatalogItemId"),
            "Connection ID. FK -> mz_connections.id.",
        ),
        (
            "aws_privatelink",
            "principal",
            None,
            "AWS Principal for VPC endpoint.",
        ),
        // Source/sink detail tables
        (
            "kafka_source",
            "id",
            Some("CatalogItemId"),
            "Source ID. FK -> mz_sources.id.",
        ),
        (
            "kafka_source",
            "group_id_prefix",
            None,
            "Kafka consumer group ID prefix.",
        ),
        ("kafka_source", "topic", None, "Kafka topic name."),
        (
            "kafka_sink",
            "id",
            Some("CatalogItemId"),
            "Sink ID. FK -> mz_sinks.id.",
        ),
        ("kafka_sink", "topic", None, "Kafka topic name."),
        (
            "iceberg_sink",
            "id",
            Some("CatalogItemId"),
            "Sink ID. FK -> mz_sinks.id.",
        ),
        (
            "iceberg_sink",
            "namespace",
            None,
            "Iceberg table namespace.",
        ),
        ("iceberg_sink", "table", None, "Iceberg table name."),
        // mz_audit_events
        (
            "audit_event",
            "id",
            None,
            "Monotonically increasing event ID. PK.",
        ),
        (
            "audit_event",
            "event_type",
            None,
            "Event type: create, drop, alter.",
        ),
        (
            "audit_event",
            "object_type",
            Some("ObjectType"),
            "Type of affected object.",
        ),
        (
            "audit_event",
            "details",
            None,
            "JSON details varying by event/object type.",
        ),
        (
            "audit_event",
            "user",
            None,
            "User who triggered the event. NULL if system.",
        ),
        (
            "audit_event",
            "occurred_at",
            Some("WallclockTimestamp"),
            "When the event occurred.",
        ),
        // mz_cluster_replica_sizes
        (
            "replica_size",
            "size",
            None,
            "Human-readable size name (e.g., 25cc).",
        ),
        ("replica_size", "processes", None, "Number of processes."),
        (
            "replica_size",
            "workers",
            None,
            "Timely Dataflow workers per process.",
        ),
        (
            "replica_size",
            "cpu_nano_cores",
            None,
            "CPU allocation per process in nano-cores.",
        ),
        (
            "replica_size",
            "memory_bytes",
            Some("ByteCount"),
            "RAM allocation per process.",
        ),
        (
            "replica_size",
            "disk_bytes",
            Some("ByteCount"),
            "Disk allocation per process.",
        ),
        (
            "replica_size",
            "credits_per_hour",
            Some("CreditRate"),
            "Compute credits consumed per hour.",
        ),
        // mz_egress_ips
        ("egress_ip", "egress_ip", None, "Start of the IP range."),
        ("egress_ip", "prefix_length", None, "CIDR prefix length."),
        ("egress_ip", "cidr", None, "CIDR notation."),
        // Union views
        ("relation", "id", Some("CatalogItemId"), "Relation ID."),
        ("relation", "oid", Some("OID"), "PostgreSQL-compatible OID."),
        (
            "relation",
            "schema_id",
            Some("SchemaId"),
            "Parent schema. FK -> mz_schemas.id.",
        ),
        ("relation", "name", None, "Relation name."),
        (
            "relation",
            "type",
            Some("ObjectType"),
            "Relation type: table, source, view, materialized-view.",
        ),
        (
            "relation",
            "owner_id",
            Some("RoleId"),
            "Owner. FK -> mz_roles.id.",
        ),
        (
            "relation",
            "cluster_id",
            Some("ClusterId"),
            "Cluster. FK -> mz_clusters.id. NULL for views/tables.",
        ),
        ("relation", "privileges", None, "ACL privileges."),
        ("object", "id", Some("CatalogItemId"), "Object ID."),
        ("object", "oid", Some("OID"), "PostgreSQL-compatible OID."),
        (
            "object",
            "schema_id",
            Some("SchemaId"),
            "Parent schema. FK -> mz_schemas.id.",
        ),
        ("object", "name", None, "Object name."),
        (
            "object",
            "type",
            Some("ObjectType"),
            "Object type: table, source, view, materialized-view, sink, index, connection, secret, type, function.",
        ),
        (
            "object",
            "owner_id",
            Some("RoleId"),
            "Owner. FK -> mz_roles.id.",
        ),
        (
            "object",
            "cluster_id",
            Some("ClusterId"),
            "Cluster. FK -> mz_clusters.id. NULL for non-cluster objects.",
        ),
        ("object", "privileges", None, "ACL privileges."),
        // Storage usage views
        (
            "storage_usage",
            "object_id",
            Some("CatalogItemId"),
            "Object being measured. FK -> mz_objects.id.",
        ),
        (
            "storage_usage",
            "size_bytes",
            Some("ByteCount"),
            "Storage bytes used.",
        ),
        (
            "storage_usage",
            "collection_timestamp",
            Some("WallclockTimestamp"),
            "When usage was assessed.",
        ),
        (
            "recent_storage",
            "object_id",
            Some("CatalogItemId"),
            "Object being measured. FK -> mz_objects.id.",
        ),
        (
            "recent_storage",
            "size_bytes",
            Some("ByteCount"),
            "Most recent storage bytes used.",
        ),
        // mz_internal properties
        (
            "object_dependency",
            "object_id",
            Some("CatalogItemId"),
            "The dependent object.",
        ),
        (
            "object_dependency",
            "referenced_object_id",
            Some("CatalogItemId"),
            "The object being depended on.",
        ),
        (
            "object_global_id",
            "id",
            Some("CatalogItemId"),
            "SQL-layer catalog item ID.",
        ),
        (
            "object_global_id",
            "global_id",
            Some("GlobalId"),
            "Runtime global ID.",
        ),
        (
            "comment",
            "id",
            Some("CatalogItemId"),
            "The object being commented on.",
        ),
        ("object_history", "id", Some("CatalogItemId"), "The object."),
        (
            "object_lifetime",
            "id",
            Some("CatalogItemId"),
            "The object.",
        ),
        // Replica status/metrics
        (
            "replica_status",
            "replica_id",
            Some("ReplicaId"),
            "The replica.",
        ),
        (
            "replica_status_history",
            "replica_id",
            Some("ReplicaId"),
            "The replica.",
        ),
        (
            "replica_metrics",
            "replica_id",
            Some("ReplicaId"),
            "The replica.",
        ),
        (
            "replica_utilization",
            "replica_id",
            Some("ReplicaId"),
            "The replica.",
        ),
        // Frontiers
        (
            "frontier",
            "object_id",
            Some("GlobalId"),
            "The object. Uses GlobalId, not CatalogItemId.",
        ),
        (
            "frontier",
            "read_frontier",
            Some("MzTimestamp"),
            "The earliest timestamp at which the output is still readable.",
        ),
        (
            "frontier",
            "write_frontier",
            Some("MzTimestamp"),
            "The next timestamp at which the output may change.",
        ),
        (
            "global_frontier",
            "object_id",
            Some("GlobalId"),
            "The object. Uses GlobalId.",
        ),
        // Wallclock lag
        (
            "wallclock_lag_history",
            "object_id",
            Some("GlobalId"),
            "The object. Uses GlobalId, not CatalogItemId.",
        ),
        (
            "wallclock_lag_history",
            "replica_id",
            Some("ReplicaId"),
            "The replica.",
        ),
        (
            "wallclock_lag_history",
            "lag",
            None,
            "The amount of time the objects write frontier lags behind wallclock time.",
        ),
        (
            "wallclock_lag_history",
            "occurred_at",
            Some("WallclockTimestamp"),
            "Wall-clock timestamp of the observation.",
        ),
        (
            "wallclock_global_lag",
            "object_id",
            Some("GlobalId"),
            "The object. Uses GlobalId.",
        ),
        (
            "wallclock_global_lag",
            "lag",
            None,
            "The amount of time the objects write frontier lags behind wallclock time.",
        ),
        (
            "wallclock_global_lag_history",
            "object_id",
            Some("GlobalId"),
            "The object. Uses GlobalId.",
        ),
        (
            "wallclock_global_lag_history",
            "lag",
            None,
            "The minimum wallclock lag observed during the minute.",
        ),
        (
            "wallclock_global_lag_history",
            "occurred_at",
            Some("WallclockTimestamp"),
            "The minute-aligned timestamp of the observation.",
        ),
        // Materialization lag
        (
            "materialization_lag",
            "object_id",
            Some("CatalogItemId"),
            "The materialization. Uses CatalogItemId.",
        ),
        (
            "materialization_lag",
            "slowest_local_input_id",
            Some("CatalogItemId"),
            "Slowest direct input.",
        ),
        (
            "materialization_lag",
            "slowest_global_input_id",
            Some("CatalogItemId"),
            "Slowest root input.",
        ),
        (
            "materialization_lag",
            "local_lag",
            None,
            "The amount of time the materialization lags behind its direct inputs.",
        ),
        (
            "materialization_lag",
            "global_lag",
            None,
            "The amount of time the materialization lags behind its root inputs (sources and tables).",
        ),
        (
            "materialization_dep",
            "object_id",
            Some("CatalogItemId"),
            "The materialization.",
        ),
        (
            "materialization_dep",
            "dependency_id",
            Some("CatalogItemId"),
            "The dependency.",
        ),
        // Hydration
        (
            "hydration_status",
            "object_id",
            Some("GlobalId"),
            "The object. Uses GlobalId.",
        ),
        (
            "hydration_status",
            "replica_id",
            Some("ReplicaId"),
            "The replica.",
        ),
        (
            "hydration_status",
            "hydrated",
            None,
            "Whether the object is hydrated on the replica.",
        ),
        (
            "compute_hydration_status_view",
            "object_id",
            Some("GlobalId"),
            "The compute object.",
        ),
        (
            "compute_hydration_status_view",
            "replica_id",
            Some("ReplicaId"),
            "The replica.",
        ),
        (
            "compute_hydration_status_view",
            "hydrated",
            None,
            "Whether the compute object is hydrated.",
        ),
        (
            "compute_hydration_status_view",
            "hydration_time",
            None,
            "Time it took to hydrate.",
        ),
        // Compute dependencies
        (
            "compute_dependency",
            "object_id",
            Some("GlobalId"),
            "The compute object. Uses GlobalId.",
        ),
        (
            "compute_dependency",
            "dependency_id",
            Some("GlobalId"),
            "The dependency. Uses GlobalId.",
        ),
        // Source/sink statistics and status
        (
            "source_statistics",
            "id",
            Some("CatalogItemId"),
            "The source.",
        ),
        (
            "source_statistics",
            "replica_id",
            Some("ReplicaId"),
            "The replica running the source.",
        ),
        (
            "source_statistics",
            "messages_received",
            Some("RecordCount"),
            "Messages received from external system.",
        ),
        (
            "source_statistics",
            "bytes_received",
            Some("ByteCount"),
            "Bytes read from external system.",
        ),
        (
            "source_statistics",
            "updates_staged",
            Some("RecordCount"),
            "Updates written but not yet committed.",
        ),
        (
            "source_statistics",
            "updates_committed",
            Some("RecordCount"),
            "Updates committed to storage layer.",
        ),
        (
            "source_statistics",
            "records_indexed",
            Some("RecordCount"),
            "Records in the source envelope state index.",
        ),
        (
            "source_statistics",
            "bytes_indexed",
            Some("ByteCount"),
            "Bytes in the source internal index.",
        ),
        (
            "source_statistics",
            "rehydration_latency",
            None,
            "Time to rehydrate internal index after restart.",
        ),
        (
            "source_statistics",
            "snapshot_records_known",
            Some("RecordCount"),
            "Total records in the source snapshot.",
        ),
        (
            "source_statistics",
            "snapshot_records_staged",
            Some("RecordCount"),
            "Snapshot records read so far.",
        ),
        (
            "source_statistics",
            "snapshot_committed",
            None,
            "Whether initial snapshot is committed.",
        ),
        (
            "source_statistics",
            "offset_known",
            None,
            "Offset of most recent upstream data known.",
        ),
        (
            "source_statistics",
            "offset_committed",
            None,
            "Offset of data durably ingested.",
        ),
        ("source_status", "id", Some("CatalogItemId"), "The source."),
        ("source_status", "name", None, "The name of the source."),
        (
            "source_status",
            "type",
            Some("SourceType"),
            "The type of the source.",
        ),
        (
            "source_status",
            "last_status_change_at",
            Some("WallclockTimestamp"),
            "When the status last changed.",
        ),
        (
            "source_status",
            "status",
            None,
            "The status: created, starting, running, paused, stalled, failed, or dropped.",
        ),
        (
            "source_status",
            "error",
            None,
            "Error message if in error state.",
        ),
        (
            "source_status",
            "details",
            None,
            "Additional metadata. May contain a hint field.",
        ),
        (
            "source_status_history",
            "source_id",
            Some("CatalogItemId"),
            "The source.",
        ),
        (
            "source_status_history",
            "occurred_at",
            Some("WallclockTimestamp"),
            "When the status change occurred.",
        ),
        (
            "source_status_history",
            "status",
            None,
            "The status: created, starting, running, paused, stalled, failed, or dropped.",
        ),
        (
            "source_status_history",
            "error",
            None,
            "Error message if in error state.",
        ),
        (
            "source_status_history",
            "details",
            None,
            "Additional metadata.",
        ),
        (
            "source_status_history",
            "replica_id",
            Some("ReplicaId"),
            "The replica running this source instance.",
        ),
        ("sink_statistics", "id", Some("CatalogItemId"), "The sink."),
        (
            "sink_statistics",
            "replica_id",
            Some("ReplicaId"),
            "The replica running the sink.",
        ),
        (
            "sink_statistics",
            "messages_staged",
            Some("RecordCount"),
            "Messages staged but possibly not committed.",
        ),
        (
            "sink_statistics",
            "messages_committed",
            Some("RecordCount"),
            "Messages committed to the sink.",
        ),
        (
            "sink_statistics",
            "bytes_staged",
            Some("ByteCount"),
            "Bytes staged but possibly not committed.",
        ),
        (
            "sink_statistics",
            "bytes_committed",
            Some("ByteCount"),
            "Bytes committed to the sink.",
        ),
        ("sink_status", "id", Some("CatalogItemId"), "The sink."),
        ("sink_status", "name", None, "The name of the sink."),
        ("sink_status", "type", None, "The type of the sink."),
        (
            "sink_status",
            "last_status_change_at",
            Some("WallclockTimestamp"),
            "When the status last changed.",
        ),
        (
            "sink_status",
            "status",
            None,
            "The status: created, starting, running, stalled, failed, or dropped.",
        ),
        (
            "sink_status",
            "error",
            None,
            "Error message if in error state.",
        ),
        ("sink_status", "details", None, "Additional metadata."),
        (
            "sink_status_history",
            "sink_id",
            Some("CatalogItemId"),
            "The sink.",
        ),
        (
            "sink_status_history",
            "occurred_at",
            Some("WallclockTimestamp"),
            "When the status change occurred.",
        ),
        (
            "sink_status_history",
            "status",
            None,
            "The status: created, starting, running, stalled, failed, or dropped.",
        ),
        (
            "sink_status_history",
            "error",
            None,
            "Error message if in error state.",
        ),
        (
            "sink_status_history",
            "details",
            None,
            "Additional metadata.",
        ),
        (
            "sink_status_history",
            "replica_id",
            Some("ReplicaId"),
            "The replica running this sink instance.",
        ),
        // Storage shards
        (
            "storage_shard",
            "object_id",
            Some("GlobalId"),
            "The object. Uses GlobalId.",
        ),
        (
            "storage_shard",
            "shard_id",
            Some("ShardId"),
            "The persist shard ID.",
        ),
        ("storage_usage_by_shard", "id", None, "Row ID."),
        (
            "storage_usage_by_shard",
            "shard_id",
            Some("ShardId"),
            "The persist shard ID.",
        ),
        (
            "storage_usage_by_shard",
            "size_bytes",
            Some("ByteCount"),
            "Storage bytes used by this shard.",
        ),
        (
            "storage_usage_by_shard",
            "collection_timestamp",
            Some("WallclockTimestamp"),
            "When usage was assessed.",
        ),
        // Comments
        (
            "comment",
            "object_type",
            Some("ObjectType"),
            "The type of object the comment is on.",
        ),
        (
            "comment",
            "object_sub_id",
            None,
            "For column comments, the column number. NULL otherwise.",
        ),
        ("comment", "comment", None, "The comment text."),
        // Session / activity
        (
            "session_history",
            "session_id",
            None,
            "Globally unique session ID.",
        ),
        (
            "session_history",
            "connected_at",
            Some("WallclockTimestamp"),
            "When the session was established.",
        ),
        (
            "session_history",
            "initial_application_name",
            None,
            "The application_name at connection time.",
        ),
        (
            "session_history",
            "authenticated_user",
            None,
            "The user the session was established for.",
        ),
        (
            "activity_log",
            "execution_id",
            None,
            "Unique ID per executed statement.",
        ),
        (
            "activity_log",
            "sample_rate",
            None,
            "The rate at which the statement was sampled.",
        ),
        (
            "activity_log",
            "cluster_id",
            Some("ClusterId"),
            "The cluster the query ran on.",
        ),
        (
            "activity_log",
            "application_name",
            None,
            "The application_name at execution time.",
        ),
        (
            "activity_log",
            "cluster_name",
            None,
            "The cluster name at execution time.",
        ),
        (
            "activity_log",
            "database_name",
            None,
            "The database at execution time.",
        ),
        (
            "activity_log",
            "search_path",
            None,
            "The search_path at execution time.",
        ),
        (
            "activity_log",
            "transaction_isolation",
            None,
            "The transaction isolation level.",
        ),
        (
            "activity_log",
            "execution_timestamp",
            Some("MzTimestamp"),
            "The logical timestamp chosen for execution.",
        ),
        (
            "activity_log",
            "transient_index_id",
            Some("GlobalId"),
            "Transient index used for fast-path peeks.",
        ),
        (
            "activity_log",
            "params",
            None,
            "Bound parameter values for prepared statements.",
        ),
        (
            "activity_log",
            "mz_version",
            None,
            "The Materialize version that executed this statement.",
        ),
        (
            "activity_log",
            "began_at",
            Some("WallclockTimestamp"),
            "When the statement began executing.",
        ),
        (
            "activity_log",
            "finished_at",
            Some("WallclockTimestamp"),
            "When the statement finished executing.",
        ),
        (
            "activity_log",
            "finished_status",
            None,
            "Final status: success, canceled, error, or aborted.",
        ),
        (
            "activity_log",
            "error_message",
            None,
            "Error message if the statement failed.",
        ),
        (
            "activity_log",
            "result_size",
            Some("ByteCount"),
            "Size in bytes of the result.",
        ),
        (
            "activity_log",
            "rows_returned",
            Some("RecordCount"),
            "Number of rows returned.",
        ),
        (
            "activity_log",
            "execution_strategy",
            None,
            "For SELECTs: constant, fast-path, or standard.",
        ),
        (
            "activity_log",
            "transaction_id",
            None,
            "Transaction ID for the statement.",
        ),
        (
            "activity_log",
            "prepared_statement_id",
            None,
            "UUID of the prepared statement.",
        ),
        ("activity_log", "sql_hash", None, "Hash of the SQL text."),
        (
            "activity_log",
            "prepared_statement_name",
            None,
            "Name of the prepared statement (empty string if unnamed).",
        ),
        (
            "activity_log",
            "session_id",
            None,
            "The session ID. Corresponds to mz_sessions.id.",
        ),
        (
            "activity_log",
            "prepared_at",
            Some("WallclockTimestamp"),
            "When the statement was prepared.",
        ),
        (
            "activity_log",
            "statement_type",
            None,
            "Type of statement: select, insert, etc.",
        ),
        (
            "activity_log",
            "throttled_count",
            None,
            "Number of times this statement was throttled before being logged.",
        ),
        (
            "activity_log",
            "connected_at",
            Some("WallclockTimestamp"),
            "When the session was established.",
        ),
        (
            "activity_log",
            "initial_application_name",
            None,
            "The application_name at session connection time.",
        ),
        (
            "activity_log",
            "authenticated_user",
            None,
            "The user for the session.",
        ),
        (
            "activity_log",
            "sql",
            Some("SqlDefinition"),
            "The SQL text of the statement.",
        ),
        // Introspection: Arrangements
        (
            "arrangement_size",
            "operator_id",
            None,
            "The operator (local uint64 ID within a dataflow, not a GlobalId).",
        ),
        (
            "arrangement_size",
            "records",
            Some("RecordCount"),
            "Number of records in the arrangement.",
        ),
        (
            "arrangement_size",
            "batches",
            None,
            "Number of batches in the arrangement.",
        ),
        (
            "arrangement_size",
            "size",
            Some("ByteCount"),
            "Utilized size in bytes.",
        ),
        (
            "arrangement_size",
            "capacity",
            Some("ByteCount"),
            "Capacity in bytes (can be larger than size).",
        ),
        (
            "arrangement_size",
            "allocations",
            None,
            "Number of separate memory allocations.",
        ),
        // Scheduling
        (
            "scheduling_elapsed",
            "id",
            None,
            "The operator (local uint64 ID within a dataflow, not a GlobalId).",
        ),
        (
            "scheduling_elapsed",
            "elapsed_ns",
            None,
            "Total elapsed time in the operator in nanoseconds.",
        ),
        // Dataflows and operators
        (
            "dataflow",
            "id",
            None,
            "The dataflow ID (local uint64, not a GlobalId).",
        ),
        (
            "dataflow",
            "name",
            None,
            "The internal name of the dataflow.",
        ),
        (
            "dataflow_operator",
            "id",
            None,
            "The operator ID (local uint64, not a GlobalId).",
        ),
        (
            "dataflow_operator",
            "name",
            None,
            "The internal name of the operator.",
        ),
        // Note: mz_dataflow_operators and mz_dataflow_channels do not have a
        // direct dataflow_id column. Use mz_dataflow_operator_dataflows to map
        // operators to their parent dataflow.
        // Compute exports and frontiers
        (
            "compute_export",
            "export_id",
            Some("GlobalId"),
            "The exported collection. Uses GlobalId.",
        ),
        (
            "compute_export",
            "dataflow_id",
            None,
            "The parent dataflow (local uint64 ID, not a GlobalId). Corresponds to mz_dataflows.id.",
        ),
        (
            "compute_frontier",
            "export_id",
            Some("GlobalId"),
            "The exported collection. Uses GlobalId.",
        ),
        (
            "compute_frontier",
            "time",
            Some("MzTimestamp"),
            "The next timestamp at which the output may change.",
        ),
        (
            "compute_import_frontier",
            "export_id",
            Some("GlobalId"),
            "The exported collection. Uses GlobalId.",
        ),
        // Transitive dependencies
        (
            "transitive_dependency",
            "object_id",
            Some("CatalogItemId"),
            "The dependent object.",
        ),
        (
            "transitive_dependency",
            "referenced_object_id",
            Some("CatalogItemId"),
            "The object being depended on (directly or indirectly).",
        ),
        // Replica status detail
        (
            "replica_status",
            "process_id",
            None,
            "The ID of the process within the replica.",
        ),
        (
            "replica_status",
            "status",
            None,
            "The status of the replica: online or offline.",
        ),
        (
            "replica_status",
            "reason",
            None,
            "If offline, the reason (e.g., oom-killed).",
        ),
        (
            "replica_status",
            "updated_at",
            Some("WallclockTimestamp"),
            "When the status was last updated.",
        ),
        // Replica metrics detail
        (
            "replica_metrics",
            "process_id",
            None,
            "The ID of a process within the replica.",
        ),
        (
            "replica_metrics",
            "cpu_nano_cores",
            None,
            "Approximate CPU usage in billionths of a vCPU core.",
        ),
        (
            "replica_metrics",
            "memory_bytes",
            Some("ByteCount"),
            "Approximate RAM usage in bytes.",
        ),
        (
            "replica_metrics",
            "disk_bytes",
            Some("ByteCount"),
            "Approximate disk usage in bytes.",
        ),
        (
            "replica_metrics",
            "heap_bytes",
            Some("ByteCount"),
            "Approximate heap (RAM + swap) usage in bytes.",
        ),
        (
            "replica_metrics",
            "heap_limit",
            Some("ByteCount"),
            "Available heap (RAM + swap) space in bytes.",
        ),
        // Replica utilization detail
        (
            "replica_utilization",
            "process_id",
            None,
            "The ID of a process within the replica.",
        ),
        (
            "replica_utilization",
            "cpu_percent",
            None,
            "Approximate CPU usage as percent of allocation.",
        ),
        (
            "replica_utilization",
            "memory_percent",
            None,
            "Approximate RAM usage as percent of allocation.",
        ),
        (
            "replica_utilization",
            "disk_percent",
            None,
            "Approximate disk usage as percent of allocation.",
        ),
        (
            "replica_utilization",
            "heap_percent",
            None,
            "Approximate heap usage as percent of allocation.",
        ),
    ]
}

fn pack_properties() -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
    property_data()
        .into_iter()
        .map(|(entity_type, column_name, semantic_type, description)| {
            property_row(entity_type, column_name, semantic_type, description)
        })
        .collect()
}

fn pack_link_types() -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
    vec![
        // Hierarchy: database -> schema -> objects
        link_type_row(
            "in_database",
            "schema",
            "database",
            Some(
                r#"{"kind": "foreign_key", "source_column": "database_id", "target_column": "id", "cardinality": "many_to_one", "nullable": true}"#,
            ),
            "A schema lives in a database. Nullable because system schemas (mz_catalog, pg_catalog) have no database.",
        ),
        link_type_row(
            "in_schema",
            "table",
            "schema",
            Some(
                r#"{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A table lives in a schema.",
        ),
        link_type_row(
            "in_schema",
            "source",
            "schema",
            Some(
                r#"{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A source lives in a schema.",
        ),
        link_type_row(
            "in_schema",
            "view",
            "schema",
            Some(
                r#"{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A view lives in a schema.",
        ),
        link_type_row(
            "in_schema",
            "mv",
            "schema",
            Some(
                r#"{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A materialized view lives in a schema.",
        ),
        link_type_row(
            "in_schema",
            "connection",
            "schema",
            Some(
                r#"{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A connection lives in a schema.",
        ),
        link_type_row(
            "in_schema",
            "secret",
            "schema",
            Some(
                r#"{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A secret lives in a schema.",
        ),
        link_type_row(
            "in_schema",
            "type",
            "schema",
            Some(
                r#"{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A type lives in a schema.",
        ),
        link_type_row(
            "in_schema",
            "function",
            "schema",
            Some(
                r#"{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A function lives in a schema.",
        ),
        link_type_row(
            "in_schema",
            "sink",
            "schema",
            Some(
                r#"{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A sink lives in a schema.",
        ),
        // Ownership
        link_type_row(
            "owned_by",
            "database",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A database is owned by a role.",
        ),
        link_type_row(
            "owned_by",
            "schema",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A schema is owned by a role.",
        ),
        link_type_row(
            "owned_by",
            "cluster",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A cluster is owned by a role.",
        ),
        link_type_row(
            "owned_by",
            "replica",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A cluster replica is owned by a role.",
        ),
        link_type_row(
            "owned_by",
            "table",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A table is owned by a role.",
        ),
        link_type_row(
            "owned_by",
            "source",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A source is owned by a role.",
        ),
        link_type_row(
            "owned_by",
            "view",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A view is owned by a role.",
        ),
        link_type_row(
            "owned_by",
            "mv",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A materialized view is owned by a role.",
        ),
        link_type_row(
            "owned_by",
            "index",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "An index is owned by a role.",
        ),
        link_type_row(
            "owned_by",
            "sink",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A sink is owned by a role.",
        ),
        link_type_row(
            "owned_by",
            "connection",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A connection is owned by a role.",
        ),
        link_type_row(
            "owned_by",
            "secret",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A secret is owned by a role.",
        ),
        link_type_row(
            "owned_by",
            "type",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A type is owned by a role.",
        ),
        link_type_row(
            "owned_by",
            "function",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "owner_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A function is owned by a role.",
        ),
        // Cluster relationships
        link_type_row(
            "belongs_to_cluster",
            "replica",
            "cluster",
            Some(
                r#"{"kind": "foreign_key", "source_column": "cluster_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A replica belongs to exactly one cluster. A cluster can have multiple replicas.",
        ),
        link_type_row(
            "runs_on_cluster",
            "mv",
            "cluster",
            Some(
                r#"{"kind": "foreign_key", "source_column": "cluster_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A materialized view is maintained on a cluster.",
        ),
        link_type_row(
            "runs_on_cluster",
            "index",
            "cluster",
            Some(
                r#"{"kind": "foreign_key", "source_column": "cluster_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "An index is maintained on a cluster.",
        ),
        link_type_row(
            "runs_on_cluster",
            "source",
            "cluster",
            Some(
                r#"{"kind": "foreign_key", "source_column": "cluster_id", "target_column": "id", "cardinality": "many_to_one", "nullable": true}"#,
            ),
            "A source is ingested on a cluster. Nullable for subsources and progress sources.",
        ),
        link_type_row(
            "runs_on_cluster",
            "sink",
            "cluster",
            Some(
                r#"{"kind": "foreign_key", "source_column": "cluster_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A sink exports data from a cluster.",
        ),
        // Index relationships
        link_type_row(
            "indexes_relation",
            "index",
            "relation",
            Some(
                r#"{"kind": "foreign_key", "source_column": "on_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "An index is built on a relation (table, source, view, or materialized view).",
        ),
        link_type_row(
            "belongs_to_index",
            "index_column",
            "index",
            Some(
                r#"{"kind": "foreign_key", "source_column": "index_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "An index column belongs to an index.",
        ),
        // Column relationships
        link_type_row(
            "belongs_to_relation",
            "column",
            "object",
            Some(
                r#"{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "many_to_one", "note": "id in mz_columns is the relation ID, not a unique column ID"}"#,
            ),
            "A column belongs to a relation.",
        ),
        // Source/sink detail tables
        link_type_row(
            "details_of",
            "kafka_source",
            "source",
            Some(
                r#"{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "Kafka-specific source details. Joins to mz_sources on id.",
        ),
        link_type_row(
            "details_of",
            "kafka_sink",
            "sink",
            Some(
                r#"{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "Kafka-specific sink details. Joins to mz_sinks on id.",
        ),
        link_type_row(
            "details_of",
            "iceberg_sink",
            "sink",
            Some(
                r#"{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "Iceberg-specific sink details. Joins to mz_sinks on id.",
        ),
        link_type_row(
            "details_of",
            "kafka_connection",
            "connection",
            Some(
                r#"{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "Kafka-specific connection details.",
        ),
        link_type_row(
            "details_of",
            "ssh_tunnel",
            "connection",
            Some(
                r#"{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "SSH tunnel connection details.",
        ),
        link_type_row(
            "details_of",
            "aws_privatelink",
            "connection",
            Some(
                r#"{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "AWS PrivateLink connection details.",
        ),
        // Source -> connection
        link_type_row(
            "uses_connection",
            "source",
            "connection",
            Some(
                r#"{"kind": "foreign_key", "source_column": "connection_id", "target_column": "id", "cardinality": "many_to_one", "nullable": true}"#,
            ),
            "A source uses a connection for external access.",
        ),
        // Table -> source
        link_type_row(
            "created_by_source",
            "table",
            "source",
            Some(
                r#"{"kind": "foreign_key", "source_column": "source_id", "target_column": "id", "cardinality": "many_to_one", "nullable": true}"#,
            ),
            "A table can be created by a source (e.g., Postgres source creates tables).",
        ),
        // Type system relationships
        link_type_row(
            "is_subtype_of",
            "array_type",
            "type",
            Some(
                r#"{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "An array type is a specialization of a type.",
        ),
        link_type_row(
            "has_element_type",
            "array_type",
            "type",
            Some(
                r#"{"kind": "foreign_key", "source_column": "element_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "An array type has an element type.",
        ),
        link_type_row(
            "is_subtype_of",
            "list_type",
            "type",
            Some(
                r#"{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "A list type is a specialization of a type.",
        ),
        link_type_row(
            "has_element_type",
            "list_type",
            "type",
            Some(
                r#"{"kind": "foreign_key", "source_column": "element_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A list type has an element type.",
        ),
        link_type_row(
            "is_subtype_of",
            "map_type",
            "type",
            Some(
                r#"{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "A map type is a specialization of a type.",
        ),
        link_type_row(
            "has_key_type",
            "map_type",
            "type",
            Some(
                r#"{"kind": "foreign_key", "source_column": "key_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A map type has a key type.",
        ),
        link_type_row(
            "has_value_type",
            "map_type",
            "type",
            Some(
                r#"{"kind": "foreign_key", "source_column": "value_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A map type has a value type.",
        ),
        // Function type references
        link_type_row(
            "returns_type",
            "function",
            "type",
            Some(
                r#"{"kind": "foreign_key", "source_column": "return_type_id", "target_column": "id", "cardinality": "many_to_one", "nullable": true}"#,
            ),
            "A function returns a type.",
        ),
        // Role membership
        link_type_row(
            "member_of_role",
            "role_member",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "role_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A role membership: the member role is a member of this role.",
        ),
        link_type_row(
            "has_member",
            "role_member",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "member", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A role membership: this role is the member.",
        ),
        link_type_row(
            "granted_by",
            "role_member",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "grantor", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A role membership was granted by this role.",
        ),
        // Role parameters
        link_type_row(
            "parameter_of",
            "role_parameter",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "role_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A session parameter default belongs to a role.",
        ),
        // Default privileges
        link_type_row(
            "default_priv_for_role",
            "default_privilege",
            "role",
            Some(
                r#"{"kind": "foreign_key", "source_column": "role_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A default privilege rule applies when this role creates objects.",
        ),
        link_type_row(
            "default_priv_in_database",
            "default_privilege",
            "database",
            Some(
                r#"{"kind": "foreign_key", "source_column": "database_id", "target_column": "id", "cardinality": "many_to_one", "nullable": true}"#,
            ),
            "A default privilege rule scoped to a database.",
        ),
        link_type_row(
            "default_priv_in_schema",
            "default_privilege",
            "schema",
            Some(
                r#"{"kind": "foreign_key", "source_column": "schema_id", "target_column": "id", "cardinality": "many_to_one", "nullable": true}"#,
            ),
            "A default privilege rule scoped to a schema.",
        ),
        // Storage usage
        link_type_row(
            "storage_usage_of",
            "storage_usage",
            "object",
            Some(
                r#"{"kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "Storage usage measurement for an object over time.",
        ),
        link_type_row(
            "recent_storage_of",
            "recent_storage",
            "object",
            Some(
                r#"{"kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "Most recent storage usage snapshot for an object.",
        ),
        // Replica sizes
        link_type_row(
            "has_size",
            "replica",
            "replica_size",
            Some(
                r#"{"kind": "foreign_key", "source_column": "size", "target_column": "size", "cardinality": "many_to_one", "nullable": true}"#,
            ),
            "A replica has a size configuration.",
        ),
        link_type_row(
            "has_size",
            "cluster",
            "replica_size",
            Some(
                r#"{"kind": "foreign_key", "source_column": "size", "target_column": "size", "cardinality": "many_to_one", "nullable": true}"#,
            ),
            "A managed cluster has a default replica size.",
        ),
        // Union relationships
        link_type_row(
            "union_includes",
            "relation",
            "table",
            Some(
                r#"{"kind": "union", "discriminator_column": "type", "discriminator_value": "table"}"#,
            ),
            "mz_relations is a UNION view that includes tables.",
        ),
        link_type_row(
            "union_includes",
            "relation",
            "source",
            Some(
                r#"{"kind": "union", "discriminator_column": "type", "discriminator_value": "source"}"#,
            ),
            "mz_relations includes sources.",
        ),
        link_type_row(
            "union_includes",
            "relation",
            "view",
            Some(
                r#"{"kind": "union", "discriminator_column": "type", "discriminator_value": "view"}"#,
            ),
            "mz_relations includes views.",
        ),
        link_type_row(
            "union_includes",
            "relation",
            "mv",
            Some(
                r#"{"kind": "union", "discriminator_column": "type", "discriminator_value": "materialized-view"}"#,
            ),
            "mz_relations includes materialized views.",
        ),
        link_type_row(
            "union_includes",
            "object",
            "relation",
            Some(
                r#"{"kind": "union", "note": "mz_objects includes all relations plus indexes, connections, secrets, types, functions"}"#,
            ),
            "mz_objects includes all relations.",
        ),
        link_type_row(
            "union_includes",
            "object",
            "index",
            Some(
                r#"{"kind": "union", "discriminator_column": "type", "discriminator_value": "index"}"#,
            ),
            "mz_objects includes indexes.",
        ),
        link_type_row(
            "union_includes",
            "object",
            "connection",
            Some(
                r#"{"kind": "union", "discriminator_column": "type", "discriminator_value": "connection"}"#,
            ),
            "mz_objects includes connections.",
        ),
        link_type_row(
            "union_includes",
            "object",
            "secret",
            Some(
                r#"{"kind": "union", "discriminator_column": "type", "discriminator_value": "secret"}"#,
            ),
            "mz_objects includes secrets.",
        ),
        // Cross-layer ID mapping
        link_type_row(
            "maps_to_global_id",
            "object",
            "object",
            Some(
                r#"{"kind": "maps_to", "via": "mz_internal.mz_object_global_ids", "from_type": "CatalogItemId", "to_type": "GlobalId", "note": "A CatalogItemId (SQL layer) maps to one or more GlobalIds (runtime layer)."}"#,
            ),
            "Maps between SQL-layer CatalogItemIds and runtime GlobalIds.",
        ),
        // mz_internal link types
        link_type_row(
            "depends_on",
            "object_dependency",
            "object",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "The dependent object.",
        ),
        link_type_row(
            "depended_on_by",
            "object_dependency",
            "object",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "referenced_object_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "The object being depended on.",
        ),
        link_type_row(
            "has_global_id",
            "object_global_id",
            "object",
            Some(
                r#"{"kind": "maps_to", "source_column": "id", "target_column": "id", "from_type": "CatalogItemId", "to_type": "GlobalId"}"#,
            ),
            "Maps a CatalogItemId to its GlobalId(s).",
        ),
        link_type_row(
            "comment_on",
            "comment",
            "object",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A comment annotates an object.",
        ),
        link_type_row(
            "history_of",
            "object_history",
            "object",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "Historical events for an object.",
        ),
        link_type_row(
            "lifetime_of",
            "object_lifetime",
            "object",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "Lifetime span of an object.",
        ),
        // Replica status/metrics links
        link_type_row(
            "status_of_replica",
            "replica_status",
            "replica",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "replica_id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "Current status of a replica.",
        ),
        link_type_row(
            "status_history_of_replica",
            "replica_status_history",
            "replica",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "replica_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "Historical status events for a replica.",
        ),
        link_type_row(
            "metrics_of_replica",
            "replica_metrics",
            "replica",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "replica_id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "CPU and memory metrics for a replica.",
        ),
        link_type_row(
            "utilization_of_replica",
            "replica_utilization",
            "replica",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "replica_id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "Computed utilization metrics for a replica.",
        ),
        // Compute dependencies and hydration
        link_type_row(
            "compute_depends_on",
            "compute_dependency",
            "object",
            Some(
                r#"{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A compute object depends on another.",
        ),
        link_type_row(
            "compute_depended_by",
            "compute_dependency",
            "object",
            Some(
                r#"{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "dependency_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "The dependency target.",
        ),
        link_type_row(
            "hydration_of",
            "hydration_status",
            "object",
            Some(
                r#"{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "Hydration status of an object.",
        ),
        link_type_row(
            "hydration_on_replica",
            "hydration_status",
            "replica",
            Some(
                r#"{"source_id_type": "GlobalId", "kind": "foreign_key", "source_column": "replica_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "Which replica the hydration status is for.",
        ),
        // Frontiers
        link_type_row(
            "frontier_of",
            "frontier",
            "object",
            Some(
                r#"{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "Read/write frontier for an object.",
        ),
        link_type_row(
            "global_frontier_of",
            "global_frontier",
            "object",
            Some(
                r#"{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "Aggregated frontier across all replicas.",
        ),
        // Wallclock lag
        link_type_row(
            "measures_lag_of",
            "wallclock_lag_history",
            "object",
            Some(
                r#"{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "measures", "source_column": "object_id", "target_column": "id", "metric": "wallclock_lag"}"#,
            ),
            "Historical wallclock lag measurements for an object.",
        ),
        link_type_row(
            "measures_global_lag_of",
            "wallclock_global_lag",
            "object",
            Some(
                r#"{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "measures", "source_column": "object_id", "target_column": "id", "metric": "wallclock_lag_global"}"#,
            ),
            "Current wallclock lag aggregated across all replicas.",
        ),
        link_type_row(
            "measures_materialization_lag",
            "materialization_lag",
            "object",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "measures", "source_column": "object_id", "target_column": "id", "metric": "materialization_lag"}"#,
            ),
            "Lag between a materialization and the latest data from its inputs.",
        ),
        // Materialization dependencies
        link_type_row(
            "materialization_depends_on",
            "materialization_dep",
            "object",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "depends_on", "source_column": "object_id", "target_column": "id"}"#,
            ),
            "A materialization depends on another object for its input data.",
        ),
        // Source/sink statistics and status
        link_type_row(
            "statistics_of_source",
            "source_statistics",
            "source",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "measures", "source_column": "id", "target_column": "id", "metric": "ingestion_statistics"}"#,
            ),
            "Ingestion statistics for a source.",
        ),
        link_type_row(
            "status_of_source",
            "source_status",
            "source",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "Current status of a source.",
        ),
        link_type_row(
            "status_history_of_source",
            "source_status_history",
            "source",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "source_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "Historical status events for a source.",
        ),
        link_type_row(
            "statistics_of_sink",
            "sink_statistics",
            "sink",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "measures", "source_column": "id", "target_column": "id", "metric": "export_statistics"}"#,
            ),
            "Export statistics for a sink.",
        ),
        link_type_row(
            "status_of_sink",
            "sink_status",
            "sink",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "Current status of a sink.",
        ),
        link_type_row(
            "status_history_of_sink",
            "sink_status_history",
            "sink",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "sink_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "Historical status events for a sink.",
        ),
        // Storage shards
        link_type_row(
            "shard_of",
            "storage_shard",
            "object",
            Some(
                r#"{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A persist shard belongs to an object.",
        ),
        // Session/query -> clusters
        link_type_row(
            "session_on_cluster",
            "activity_log",
            "cluster",
            Some(
                r#"{"source_id_type": "CatalogItemId", "kind": "foreign_key", "source_column": "cluster_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A query was executed on a cluster.",
        ),
        // mz_introspection: Dataflow relationships
        link_type_row(
            "operator_in_dataflow",
            "dataflow_operator_dataflow",
            "dataflow",
            Some(
                r#"{"kind": "foreign_key", "source_column": "dataflow_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "An operator belongs to a dataflow (via mz_dataflow_operator_dataflows).",
        ),
        link_type_row(
            "channel_in_dataflow",
            "dataflow_channel",
            "dataflow",
            Some(
                r#"{"kind": "maps_to", "via": "mz_introspection.mz_dataflow_operator_dataflows", "note": "Channels do not have a direct dataflow_id. Use mz_dataflow_addresses to find the parent scope, then correlate with mz_dataflow_operator_dataflows."}"#,
            ),
            "A channel belongs to a dataflow (indirect via address scope).",
        ),
        link_type_row(
            "address_of_operator",
            "dataflow_address",
            "dataflow_operator",
            Some(
                r#"{"kind": "foreign_key", "source_column": "id", "target_column": "id", "cardinality": "one_to_one"}"#,
            ),
            "The scope address path of an operator.",
        ),
        // Compute exports/frontiers -> objects
        link_type_row(
            "export_of",
            "compute_export",
            "object",
            Some(
                r#"{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "export_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "A compute export corresponds to a maintained collection.",
        ),
        link_type_row(
            "compute_frontier_of",
            "compute_frontier",
            "object",
            Some(
                r#"{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "export_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "The compute frontier for an exported collection.",
        ),
        link_type_row(
            "compute_import_frontier_of",
            "compute_import_frontier",
            "object",
            Some(
                r#"{"source_id_type": "GlobalId", "requires_mapping": "mz_internal.mz_object_global_ids", "kind": "foreign_key", "source_column": "export_id", "target_column": "id", "cardinality": "many_to_one"}"#,
            ),
            "The import frontier of a compute dependency.",
        ),
        // Scheduling/peeks -> operators
        link_type_row(
            "elapsed_for_operator",
            "scheduling_elapsed",
            "dataflow_operator",
            Some(
                r#"{"kind": "measures", "source_column": "id", "target_column": "id", "metric": "cpu_time_ns", "note": "Both IDs are local uint64 operator IDs within a dataflow, not GlobalIds."}"#,
            ),
            "CPU time spent executing an operator.",
        ),
        link_type_row(
            "arrangement_of_operator",
            "arrangement_size",
            "dataflow_operator",
            Some(
                r#"{"kind": "measures", "source_column": "operator_id", "target_column": "id", "metric": "arrangement_size", "note": "Both IDs are local uint64 operator IDs within a dataflow, not GlobalIds."}"#,
            ),
            "Size of the arrangement maintained by an operator.",
        ),
        // Cross-schema GlobalId mapping
        link_type_row(
            "introspection_uses_global_id",
            "compute_export",
            "object_global_id",
            Some(
                r#"{"kind": "maps_to", "note": "mz_introspection tables use GlobalId. To join with mz_catalog tables (which use CatalogItemId), go through mz_internal.mz_object_global_ids."}"#,
            ),
            "Introspection tables use GlobalIds. Map them to CatalogItemIds via mz_object_global_ids.",
        ),
        // Transitive dependencies
        link_type_row(
            "transitively_depends_on",
            "transitive_dependency",
            "object",
            Some(
                r#"{"kind": "foreign_key", "source_column": "object_id", "target_column": "id", "cardinality": "many_to_one", "source_id_type": "CatalogItemId"}"#,
            ),
            "The dependent object (transitive closure).",
        ),
        link_type_row(
            "transitively_depended_on_by",
            "transitive_dependency",
            "object",
            Some(
                r#"{"kind": "foreign_key", "source_column": "referenced_object_id", "target_column": "id", "cardinality": "many_to_one", "source_id_type": "CatalogItemId"}"#,
            ),
            "The object being depended on (transitive closure).",
        ),
    ]
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use mz_catalog::builtin::{BUILTINS_STATIC, Builtin};
    use mz_repr::RelationDesc;

    use super::*;

    #[mz_ore::test]
    fn test_pack_ontology_updates_valid_json() {
        // Validates that all inline JSON strings in entity_type_row and
        // link_type_row calls are valid JSON. If any are malformed, the
        // JsonbPacker will panic here rather than at coordinator startup.
        let updates = pack_ontology_updates();
        assert!(!updates.is_empty());
    }

    /// Returns the `RelationDesc` for a builtin given its schema and name,
    /// or `None` if the builtin doesn't have a static desc (e.g., Func, Type, Index, Log).
    fn builtin_desc(schema: &str, name: &str) -> Option<&'static RelationDesc> {
        for builtin in BUILTINS_STATIC.iter() {
            if builtin.schema() == schema && builtin.name() == name {
                return match builtin {
                    Builtin::Table(t) => Some(&t.desc),
                    Builtin::View(v) => Some(&v.desc),
                    Builtin::MaterializedView(mv) => Some(&mv.desc),
                    Builtin::Source(s) => Some(&s.desc),
                    Builtin::ContinualTask(ct) => Some(&ct.desc),
                    // Log desc is computed (not &'static), skip column validation.
                    // Func, Type, Index, Connection don't have relation descs.
                    _ => None,
                };
            }
        }
        None
    }

    /// Returns true if the given schema.name exists as any builtin (including
    /// those without a RelationDesc like Func or Log).
    fn builtin_exists(schema: &str, name: &str) -> bool {
        BUILTINS_STATIC
            .iter()
            .any(|b| b.schema() == schema && b.name() == name)
    }

    #[mz_ore::test]
    fn test_entity_types_reference_valid_builtins() {
        // Verify every entity type's `relation` points to a real builtin.
        let entities = entity_type_data();
        let mut errors = Vec::new();
        for (entity_name, relation, _, _) in &entities {
            let parts: Vec<&str> = relation.split('.').collect();
            assert_eq!(
                parts.len(),
                2,
                "entity '{}' relation '{}' should be schema.name",
                entity_name,
                relation
            );
            let (schema, name) = (parts[0], parts[1]);
            if !builtin_exists(schema, name) {
                errors.push(format!(
                    "entity '{}' references relation '{}' which does not exist as a builtin",
                    entity_name, relation
                ));
            }
        }
        if !errors.is_empty() {
            panic!(
                "Ontology entity types reference non-existent builtins:\n  {}",
                errors.join("\n  ")
            );
        }
    }

    #[mz_ore::test]
    fn test_property_columns_exist_in_builtins() {
        // Build entity_name -> (schema, table_name) lookup.
        let entities = entity_type_data();
        let entity_map: BTreeMap<&str, (&str, &str)> = entities
            .iter()
            .map(|(name, relation, _, _)| {
                let parts: Vec<&str> = relation.split('.').collect();
                (*name, (parts[0], parts[1]))
            })
            .collect();

        // Build (schema, table_name) -> set of column names lookup from builtins.
        let mut builtin_columns: BTreeMap<(&str, &str), BTreeSet<String>> = BTreeMap::new();
        for (_entity_name, (schema, table_name)) in &entity_map {
            if let Some(desc) = builtin_desc(schema, table_name) {
                let cols: BTreeSet<String> =
                    desc.iter_names().map(|c| c.as_str().to_string()).collect();
                builtin_columns.insert((*schema, *table_name), cols);
            } else {
                // Already caught by test_entity_types_reference_valid_builtins
                continue;
            }
        }

        let properties = property_data();
        let mut errors = Vec::new();
        for (entity_type, column_name, _, _) in &properties {
            let Some((schema, table_name)) = entity_map.get(entity_type) else {
                errors.push(format!(
                    "property ({}, {}) references unknown entity type",
                    entity_type, column_name
                ));
                continue;
            };
            if let Some(cols) = builtin_columns.get(&(*schema, *table_name)) {
                if !cols.contains(*column_name) {
                    errors.push(format!(
                        "property ({}, {}) references column '{}' not found in {}.{} (available: {:?})",
                        entity_type, column_name, column_name, schema, table_name, cols
                    ));
                }
            }
        }
        if !errors.is_empty() {
            panic!(
                "Ontology properties reference non-existent columns:\n  {}",
                errors.join("\n  ")
            );
        }
    }

    #[mz_ore::test]
    fn test_entity_names_are_unique() {
        let entities = entity_type_data();
        let mut seen = BTreeSet::new();
        for (name, _, _, _) in &entities {
            assert!(seen.insert(*name), "duplicate entity type name: '{}'", name);
        }
    }
}
