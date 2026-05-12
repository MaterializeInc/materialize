// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Built-in catalog items for the `mz_internal` schema.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use mz_pgrepr::oid;
use mz_repr::adt::mz_acl_item::MzAclItem;
use mz_repr::namespaces::MZ_INTERNAL_SCHEMA;
use mz_repr::{RelationDesc, SemanticType, SqlScalarType};
use mz_sql::catalog::{ObjectType, SystemObjectType};
use mz_sql::rbac;
use mz_sql::session::user::{MZ_ANALYTICS_ROLE_ID, MZ_SYSTEM_ROLE_ID};
use mz_storage_client::controller::IntrospectionType;
use mz_storage_client::healthcheck::{
    MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY_DESC, MZ_PREPARED_STATEMENT_HISTORY_DESC,
    MZ_SESSION_HISTORY_DESC, MZ_SINK_STATUS_HISTORY_DESC, MZ_SOURCE_STATUS_HISTORY_DESC,
    MZ_SQL_TEXT_DESC, MZ_STATEMENT_EXECUTION_HISTORY_DESC, REPLICA_METRICS_HISTORY_DESC,
    REPLICA_STATUS_HISTORY_DESC, WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RAW_DESC,
    WALLCLOCK_LAG_HISTORY_DESC,
};
use mz_storage_client::statistics::{MZ_SINK_STATISTICS_RAW_DESC, MZ_SOURCE_STATISTICS_RAW_DESC};

use crate::memory::objects::DataSourceDesc;

use super::{
    ANALYTICS_SELECT, BuiltinConnection, BuiltinIndex, BuiltinMaterializedView, BuiltinSource,
    BuiltinTable, BuiltinView, Cardinality, LinkProperties, MONITOR_REDACTED_SELECT,
    MONITOR_SELECT, Ontology, OntologyLink, PUBLIC_SELECT, SUPPORT_SELECT,
};

pub static MZ_CATALOG_RAW: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_catalog_raw",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_CATALOG_RAW_OID,
    data_source: DataSourceDesc::Catalog,
    desc: crate::durable::persist_desc(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: false,
    // The raw catalog contains unredacted SQL statements, so we limit access to the system user.
    access: vec![],
    ontology: None,
});
pub static MZ_POSTGRES_SOURCES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_postgres_sources",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_POSTGRES_SOURCES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("replication_slot", SqlScalarType::String.nullable(false))
        .with_column("timeline_id", SqlScalarType::UInt64.nullable(true))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the source. Corresponds to `mz_catalog.mz_sources.id`.",
        ),
        (
            "replication_slot",
            "The name of the replication slot in the PostgreSQL database that Materialize will create and stream data from.",
        ),
        (
            "timeline_id",
            "The PostgreSQL timeline ID determined on source creation.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "postgres_source",
        description: "Postgres source-level details",
        links: &const {
            [OntologyLink {
                name: "details_of",
                target: "source",
                properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});
pub static MZ_POSTGRES_SOURCE_TABLES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_postgres_source_tables",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_POSTGRES_SOURCE_TABLES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("schema_name", SqlScalarType::String.nullable(false))
        .with_column("table_name", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the subsource or table. Corresponds to `mz_catalog.mz_sources.id` or `mz_catalog.mz_tables.id`.",
        ),
        (
            "schema_name",
            "The schema of the upstream table being ingested.",
        ),
        (
            "table_name",
            "The name of the upstream table being ingested.",
        ),
    ]),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "postgres_source_table",
        description: "Postgres source table-level details",
        links: &const {
            [OntologyLink {
                name: "describes_source_table",
                target: "table",
                properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});
pub static MZ_MYSQL_SOURCE_TABLES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_mysql_source_tables",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_MYSQL_SOURCE_TABLES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("schema_name", SqlScalarType::String.nullable(false))
        .with_column("table_name", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the subsource or table. Corresponds to `mz_catalog.mz_sources.id` or `mz_catalog.mz_tables.id`.",
        ),
        (
            "schema_name",
            "The schema (or, database) of the upstream table being ingested.",
        ),
        (
            "table_name",
            "The name of the upstream table being ingested.",
        ),
    ]),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "mysql_source_table",
        description: "MySQL source table-level details",
        links: &const {
            [OntologyLink {
                name: "describes_source_table",
                target: "table",
                properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});
pub static MZ_SQL_SERVER_SOURCE_TABLES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_sql_server_source_tables",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_SQL_SERVER_SOURCE_TABLES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("schema_name", SqlScalarType::String.nullable(false))
        .with_column("table_name", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the subsource or table. Corresponds to `mz_catalog.mz_sources.id` or `mz_catalog.mz_tables.id`.",
        ),
        (
            "schema_name",
            "The schema of the upstream table being ingested.",
        ),
        (
            "table_name",
            "The name of the upstream table being ingested.",
        ),
    ]),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "sql_server_source_table",
        description: "SQL Server source table-level details",
        links: &const {
            [OntologyLink {
                name: "describes_source_table",
                target: "table",
                properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});
pub static MZ_KAFKA_SOURCE_TABLES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_kafka_source_tables",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_KAFKA_SOURCE_TABLES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("topic", SqlScalarType::String.nullable(false))
        .with_column("envelope_type", SqlScalarType::String.nullable(true))
        .with_column("key_format", SqlScalarType::String.nullable(true))
        .with_column("value_format", SqlScalarType::String.nullable(true))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the table. Corresponds to `mz_catalog.mz_tables.id`.",
        ),
        ("topic", "The topic being ingested."),
        (
            "envelope_type",
            "The envelope type: `none`, `upsert`, or `debezium`. `NULL` for other source types.",
        ),
        (
            "key_format",
            "The format of the Kafka message key: `avro`, `csv`, `regex`, `bytes`, `json`, `text`, or `NULL`.",
        ),
        (
            "value_format",
            "The format of the Kafka message value: `avro`, `csv`, `regex`, `bytes`, `json`, `text`. `NULL` for other source types.",
        ),
    ]),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "kafka_source_table",
        description: "Kafka source table-level details",
        links: &const {
            [OntologyLink {
                name: "describes_source_table",
                target: "table",
                properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});
pub static MZ_OBJECT_DEPENDENCIES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_object_dependencies",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_OBJECT_DEPENDENCIES_OID,
    desc: RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column(
            "referenced_object_id",
            SqlScalarType::String.nullable(false),
        )
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "object_id",
            "The ID of the dependent object. Corresponds to `mz_objects.id`.",
        ),
        (
            "referenced_object_id",
            "The ID of the referenced object. Corresponds to `mz_objects.id`.",
        ),
    ]),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "object_dependency",
        description: "A dependency edge: one object depends on another",
        links: &const {
            [
                OntologyLink {
                    name: "depends_on",
                    target: "object",
                    properties: LinkProperties::DependsOn {
                        source_column: "object_id",
                        target_column: "id",
                        source_id_type: Some(mz_repr::SemanticType::CatalogItemId),
                        requires_mapping: None,
                    },
                },
                OntologyLink {
                    name: "dependency_is",
                    target: "object",
                    properties: LinkProperties::DependsOn {
                        source_column: "referenced_object_id",
                        target_column: "id",
                        source_id_type: Some(mz_repr::SemanticType::CatalogItemId),
                        requires_mapping: None,
                    },
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("object_id", SemanticType::CatalogItemId),
                ("referenced_object_id", SemanticType::CatalogItemId),
            ]
        },
    }),
});
pub static MZ_COMPUTE_DEPENDENCIES: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_compute_dependencies",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_COMPUTE_DEPENDENCIES_OID,
    data_source: IntrospectionType::ComputeDependencies.into(),
    desc: RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("dependency_id", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "object_id",
            "The ID of a compute object. Corresponds to `mz_catalog.mz_indexes.id`, `mz_catalog.mz_materialized_views.id`, or `mz_internal.mz_subscriptions.id`.",
        ),
        (
            "dependency_id",
            "The ID of a compute dependency. Corresponds to `mz_catalog.mz_indexes.id`, `mz_catalog.mz_materialized_views.id`, `mz_catalog.mz_sources.id`, or `mz_catalog.mz_tables.id`.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "compute_dependency",
        description: "Dependency edge from a compute object (index, materialized view, or subscription) to one of the sources of its data",
        links: &const {
            [
                OntologyLink {
                    name: "depends_on",
                    target: "object",
                    properties: LinkProperties::DependsOn {
                        source_column: "object_id",
                        target_column: "id",
                        source_id_type: Some(mz_repr::SemanticType::GlobalId),
                        requires_mapping: Some("mz_internal.mz_object_global_ids"),
                    },
                },
                OntologyLink {
                    name: "dependency_is",
                    target: "object",
                    properties: LinkProperties::DependsOn {
                        source_column: "dependency_id",
                        target_column: "id",
                        source_id_type: Some(mz_repr::SemanticType::GlobalId),
                        requires_mapping: Some("mz_internal.mz_object_global_ids"),
                    },
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("object_id", SemanticType::GlobalId),
                ("dependency_id", SemanticType::GlobalId),
            ]
        },
    }),
});

pub static MZ_MATERIALIZED_VIEW_REFRESH_STRATEGIES: LazyLock<BuiltinTable> = LazyLock::new(|| {
    BuiltinTable {
        name: "mz_materialized_view_refresh_strategies",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::TABLE_MZ_MATERIALIZED_VIEW_REFRESH_STRATEGIES_OID,
        desc: RelationDesc::builder()
            .with_column(
                "materialized_view_id",
                SqlScalarType::String.nullable(false),
            )
            .with_column("type", SqlScalarType::String.nullable(false))
            .with_column("interval", SqlScalarType::Interval.nullable(true))
            .with_column(
                "aligned_to",
                SqlScalarType::TimestampTz { precision: None }.nullable(true),
            )
            .with_column(
                "at",
                SqlScalarType::TimestampTz { precision: None }.nullable(true),
            )
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "materialized_view_id",
                "The ID of the materialized view. Corresponds to `mz_catalog.mz_materialized_views.id`",
            ),
            (
                "type",
                "`at`, `every`, or `on-commit`. Default: `on-commit`",
            ),
            (
                "interval",
                "The refresh interval of a `REFRESH EVERY` option, or `NULL` if the `type` is not `every`.",
            ),
            (
                "aligned_to",
                "The `ALIGNED TO` option of a `REFRESH EVERY` option, or `NULL` if the `type` is not `every`.",
            ),
            (
                "at",
                "The time of a `REFRESH AT`, or `NULL` if the `type` is not `at`.",
            ),
        ]),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
});

pub static MZ_NETWORK_POLICIES: LazyLock<BuiltinMaterializedView> = LazyLock::new(|| {
    BuiltinMaterializedView {
        name: "mz_network_policies",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::MV_MZ_NETWORK_POLICIES_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("owner_id", SqlScalarType::String.nullable(false))
            .with_column(
                "privileges",
                SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
            )
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_key(vec![0])
            .with_key(vec![4])
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "The ID of the network policy."),
            ("name", "The name of the network policy."),
            (
                "owner_id",
                "The role ID of the owner of the network policy. Corresponds to `mz_catalog.mz_roles.id`.",
            ),
            (
                "privileges",
                "The privileges belonging to the network policy.",
            ),
            ("oid", "A PostgreSQL-compatible OID for the network policy."),
        ]),
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id,
    ASSERT NOT NULL name,
    ASSERT NOT NULL owner_id,
    ASSERT NOT NULL privileges,
    ASSERT NOT NULL oid
) AS
SELECT
    mz_internal.parse_catalog_id(data->'key'->'id') AS id,
    data->'value'->>'name' AS name,
    mz_internal.parse_catalog_id(data->'value'->'owner_id') AS owner_id,
    mz_internal.parse_catalog_privileges(data->'value'->'privileges') AS privileges,
    (data->'value'->>'oid')::oid AS oid
FROM mz_internal.mz_catalog_raw
WHERE data->>'kind' = 'NetworkPolicy'",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "network_policy",
            description: "Network access policies",
            links: &const {
                [OntologyLink {
                    name: "owned_by",
                    target: "role",
                    properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
                }]
            },
            column_semantic_types: &const {
                [
                    ("id", SemanticType::NetworkPolicyId),
                    ("owner_id", SemanticType::RoleId),
                    ("oid", SemanticType::OID),
                ]
            },
        }),
    }
});

pub static MZ_NETWORK_POLICY_RULES: LazyLock<BuiltinMaterializedView> = LazyLock::new(|| {
    BuiltinMaterializedView {
        name: "mz_network_policy_rules",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::MV_MZ_NETWORK_POLICY_RULES_OID,
        desc: RelationDesc::builder()
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("policy_id", SqlScalarType::String.nullable(false))
            .with_column("action", SqlScalarType::String.nullable(false))
            .with_column("address", SqlScalarType::String.nullable(false))
            .with_column("direction", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "name",
                "The name of the network policy rule. Can be combined with `policy_id` to form a unique identifier.",
            ),
            (
                "policy_id",
                "The ID the network policy the rule is part of. Corresponds to `mz_internal.mz_network_policies.id`.",
            ),
            (
                "action",
                "The action of the rule. `allow` is the only supported action.",
            ),
            ("address", "The address the rule will take action on."),
            (
                "direction",
                "The direction of traffic the rule applies to. `ingress` is the only supported direction.",
            ),
        ]),
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL name,
    ASSERT NOT NULL policy_id,
    ASSERT NOT NULL action,
    ASSERT NOT NULL address,
    ASSERT NOT NULL direction
) AS
SELECT
    rule->>'name' AS name,
    mz_internal.parse_catalog_id(data->'key'->'id') AS policy_id,
    lower(rule->>'action') AS action,
    rule->>'address' AS address,
    lower(rule->>'direction') AS direction
FROM
    mz_internal.mz_catalog_raw,
    jsonb_array_elements(data->'value'->'rules') AS rule
WHERE data->>'kind' = 'NetworkPolicy'",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "network_policy_rule",
            description: "Individual rules within a network policy",
            links: &const {
                [OntologyLink {
                    name: "belongs_to_policy",
                    target: "network_policy",
                    properties: LinkProperties::fk("policy_id", "id", Cardinality::ManyToOne),
                }]
            },
            column_semantic_types: &[],
        }),
    }
});

/// PostgreSQL-specific metadata about types that doesn't make sense to expose
/// in the `mz_types` table as part of our public, stable API.
pub static MZ_TYPE_PG_METADATA: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_type_pg_metadata",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_TYPE_PG_METADATA_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("typinput", SqlScalarType::Oid.nullable(false))
        .with_column("typreceive", SqlScalarType::Oid.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});
pub static MZ_AGGREGATES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_aggregates",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_AGGREGATES_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("agg_kind", SqlScalarType::String.nullable(false))
        .with_column("agg_num_direct_args", SqlScalarType::Int16.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "aggregate",
        description: "Aggregate function metadata",
        links: &const { [] },
        column_semantic_types: &[("oid", SemanticType::OID)],
    }),
});

pub static MZ_CLUSTER_WORKLOAD_CLASSES: LazyLock<BuiltinMaterializedView> =
    LazyLock::new(|| BuiltinMaterializedView {
        name: "mz_cluster_workload_classes",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::MV_MZ_CLUSTER_WORKLOAD_CLASSES_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("workload_class", SqlScalarType::String.nullable(true))
            .with_key(vec![0])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id
) AS
SELECT
    mz_internal.parse_catalog_id(data->'key'->'id') AS id,
    CASE WHEN data->'value'->'config'->'workload_class' != 'null'
         THEN data->'value'->'config'->>'workload_class'
    END AS workload_class
FROM mz_internal.mz_catalog_raw
WHERE data->>'kind' = 'Cluster'",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub const MZ_CLUSTER_WORKLOAD_CLASSES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_cluster_workload_classes_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_CLUSTER_WORKLOAD_CLASSES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_cluster_workload_classes (id)",
    is_retained_metrics_object: false,
};

pub static MZ_CLUSTER_SCHEDULES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_cluster_schedules",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_CLUSTER_SCHEDULES_OID,
    desc: RelationDesc::builder()
        .with_column("cluster_id", SqlScalarType::String.nullable(false))
        .with_column("type", SqlScalarType::String.nullable(false))
        .with_column(
            "refresh_hydration_time_estimate",
            SqlScalarType::Interval.nullable(true),
        )
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "cluster_id",
            "The ID of the cluster. Corresponds to `mz_clusters.id`.",
        ),
        ("type", "`on-refresh`, or `manual`. Default: `manual`"),
        (
            "refresh_hydration_time_estimate",
            "The interval given in the `HYDRATION TIME ESTIMATE` option.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "cluster_schedule",
        description: "Cluster scheduling configuration",
        links: &const {
            [OntologyLink {
                name: "belongs_to_cluster",
                target: "cluster",
                properties: LinkProperties::fk("cluster_id", "id", Cardinality::ManyToOne),
            }]
        },
        column_semantic_types: &[("cluster_id", SemanticType::ClusterId)],
    }),
});

pub static MZ_INTERNAL_CLUSTER_REPLICAS: LazyLock<BuiltinMaterializedView> =
    LazyLock::new(|| BuiltinMaterializedView {
        name: "mz_internal_cluster_replicas",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::MV_MZ_INTERNAL_CLUSTER_REPLICAS_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_key(vec![0])
            .finish(),
        column_comments: BTreeMap::from_iter([(
            "id",
            "The ID of a cluster replica. Corresponds to `mz_cluster_replicas.id`.",
        )]),
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id
) AS
SELECT mz_internal.parse_catalog_id(data->'key'->'id') AS id
FROM mz_internal.mz_catalog_raw
WHERE
    data->>'kind' = 'ClusterReplica' AND
    (data->'value'->'config'->'location'->'Managed'->>'internal')::bool = true",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_PENDING_CLUSTER_REPLICAS: LazyLock<BuiltinMaterializedView> =
    LazyLock::new(|| BuiltinMaterializedView {
        name: "mz_pending_cluster_replicas",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::MV_MZ_PENDING_CLUSTER_REPLICAS_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_key(vec![0])
            .finish(),
        column_comments: BTreeMap::from_iter([(
            "id",
            "The ID of a cluster replica. Corresponds to `mz_cluster_replicas.id`.",
        )]),
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id
) AS
SELECT mz_internal.parse_catalog_id(data->'key'->'id') AS id
FROM mz_internal.mz_catalog_raw
WHERE
    data->>'kind' = 'ClusterReplica' AND
    (data->'value'->'config'->'location'->'Managed'->>'pending')::bool = true",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_CLUSTER_REPLICA_STATUS_HISTORY: LazyLock<BuiltinSource> = LazyLock::new(|| {
    BuiltinSource {
        name: "mz_cluster_replica_status_history",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_CLUSTER_REPLICA_STATUS_HISTORY_OID,
        data_source: IntrospectionType::ReplicaStatusHistory.into(),
        desc: REPLICA_STATUS_HISTORY_DESC.clone(),
        column_comments: BTreeMap::from_iter([
            ("replica_id", "The ID of a cluster replica."),
            ("process_id", "The ID of a process within the replica."),
            (
                "status",
                "The status of the cluster replica: `online` or `offline`.",
            ),
            (
                "reason",
                "If the cluster replica is in an `offline` state, the reason (if available). For example, `oom-killed`.",
            ),
            (
                "occurred_at",
                "Wall-clock timestamp at which the event occurred.",
            ),
        ]),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "replica_status_event",
            description: "Historical replica status events (ready, not-ready, etc.)",
            links: &const {
                [OntologyLink {
                    name: "status_event_of_replica",
                    target: "replica",
                    properties: LinkProperties::fk_typed(
                        "replica_id",
                        "id",
                        Cardinality::ManyToOne,
                        mz_repr::SemanticType::CatalogItemId,
                    ),
                }]
            },
            column_semantic_types: &[("replica_id", SemanticType::ReplicaId)],
        }),
    }
});

pub static MZ_CLUSTER_REPLICA_STATUSES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_cluster_replica_statuses",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_CLUSTER_REPLICA_STATUSES_OID,
    desc: RelationDesc::builder()
        .with_column("replica_id", SqlScalarType::String.nullable(false))
        .with_column("process_id", SqlScalarType::UInt64.nullable(false))
        .with_column("status", SqlScalarType::String.nullable(false))
        .with_column("reason", SqlScalarType::String.nullable(true))
        .with_column(
            "updated_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_key(vec![0, 1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "replica_id",
            "Materialize's unique ID for the cluster replica.",
        ),
        (
            "process_id",
            "The ID of the process within the cluster replica.",
        ),
        (
            "status",
            "The status of the cluster replica: `online` or `offline`.",
        ),
        (
            "reason",
            "If the cluster replica is in a `offline` state, the reason (if available). For example, `oom-killed`.",
        ),
        (
            "updated_at",
            "The time at which the status was last updated.",
        ),
    ]),
    sql: "
SELECT
    DISTINCT ON (replica_id, process_id)
    replica_id,
    process_id,
    status,
    reason,
    occurred_at as updated_at
FROM mz_internal.mz_cluster_replica_status_history
JOIN mz_cluster_replicas r ON r.id = replica_id
ORDER BY replica_id, process_id, occurred_at DESC",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "replica_status",
        description: "Current status of each replica",
        links: &const {
            [OntologyLink {
                name: "status_of_replica",
                target: "replica",
                properties: LinkProperties::fk_typed(
                    "replica_id",
                    "id",
                    Cardinality::ManyToOne,
                    mz_repr::SemanticType::ReplicaId,
                ),
            }]
        },
        column_semantic_types: &const {
            [
                ("replica_id", SemanticType::ReplicaId),
                ("updated_at", SemanticType::WallclockTimestamp),
            ]
        },
    }),
});

pub static MZ_SOURCE_STATUS_HISTORY: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_source_status_history",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_SOURCE_STATUS_HISTORY_OID,
    data_source: IntrospectionType::SourceStatusHistory.into(),
    desc: MZ_SOURCE_STATUS_HISTORY_DESC.clone(),
    column_comments: BTreeMap::from_iter([
        (
            "occurred_at",
            "Wall-clock timestamp of the source status change.",
        ),
        (
            "source_id",
            "The ID of the source. Corresponds to `mz_catalog.mz_sources.id`.",
        ),
        (
            "status",
            "The status of the source: one of `created`, `starting`, `running`, `paused`, `stalled`, `failed`, or `dropped`.",
        ),
        (
            "error",
            "If the source is in an error state, the error message.",
        ),
        (
            "details",
            "Additional metadata provided by the source. In case of error, may contain a `hint` field with helpful suggestions.",
        ),
        (
            "replica_id",
            "The ID of the replica that an instance of a source is running on.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "source_status_event",
        description: "Historical source status events",
        links: &const {
            [
                OntologyLink {
                    name: "status_event_of_source",
                    target: "source",
                    properties: LinkProperties::fk_mapped(
                        "source_id",
                        "id",
                        Cardinality::ManyToOne,
                        mz_repr::SemanticType::GlobalId,
                        "mz_internal.mz_object_global_ids",
                    ),
                },
                OntologyLink {
                    name: "on_replica",
                    target: "replica",
                    properties: LinkProperties::fk_nullable(
                        "replica_id",
                        "id",
                        Cardinality::ManyToOne,
                    ),
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("occurred_at", SemanticType::WallclockTimestamp),
                ("source_id", SemanticType::GlobalId),
                ("replica_id", SemanticType::ReplicaId),
            ]
        },
    }),
});

pub static MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY: LazyLock<BuiltinSource> = LazyLock::new(
    || BuiltinSource {
        name: "mz_aws_privatelink_connection_status_history",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY_OID,
        data_source: DataSourceDesc::Introspection(
            IntrospectionType::PrivatelinkConnectionStatusHistory,
        ),
        desc: MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY_DESC.clone(),
        column_comments: BTreeMap::from_iter([
            ("occurred_at", "Wall-clock timestamp of the status change."),
            (
                "connection_id",
                "The unique identifier of the AWS PrivateLink connection. Corresponds to `mz_catalog.mz_connections.id`.",
            ),
            (
                "status",
                "The status of the connection: one of `pending-service-discovery`, `creating-endpoint`, `recreating-endpoint`, `updating-endpoint`, `available`, `deleted`, `deleting`, `expired`, `failed`, `pending`, `pending-acceptance`, `rejected`, or `unknown`.",
            ),
        ]),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: None,
    },
);

pub static MZ_AWS_PRIVATELINK_CONNECTION_STATUSES: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "mz_aws_privatelink_connection_statuses",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_AWS_PRIVATELINK_CONNECTION_STATUSES_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column(
                "last_status_change_at",
                SqlScalarType::TimestampTz { precision: None }.nullable(true),
            )
            .with_column("status", SqlScalarType::String.nullable(true))
            .with_key(vec![0])
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "id",
                "The ID of the connection. Corresponds to `mz_catalog.mz_connections.id`.",
            ),
            ("name", "The name of the connection."),
            (
                "last_status_change_at",
                "Wall-clock timestamp of the connection status change.",
            ),
            (
                "status",
                "The status of the connection: one of `pending-service-discovery`, `creating-endpoint`, `recreating-endpoint`, `updating-endpoint`, `available`, `deleted`, `deleting`, `expired`, `failed`, `pending`, `pending-acceptance`, `rejected`, or `unknown`.",
            ),
        ]),
        sql: "
    WITH statuses_w_last_status AS (
        SELECT
            connection_id,
            occurred_at,
            status,
            lag(status) OVER (PARTITION BY connection_id ORDER BY occurred_at) AS last_status
        FROM mz_internal.mz_aws_privatelink_connection_status_history
    ),
    latest_events AS (
        -- Only take the most recent transition for each ID
        SELECT DISTINCT ON(connection_id) connection_id, occurred_at, status
        FROM statuses_w_last_status
        -- Only keep first status transitions
        WHERE status <> last_status OR last_status IS NULL
        ORDER BY connection_id, occurred_at DESC
    )
    SELECT
        conns.id,
        name,
        occurred_at as last_status_change_at,
        status
    FROM latest_events
    JOIN mz_catalog.mz_connections AS conns
    ON conns.id = latest_events.connection_id",
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "privatelink_status",
            description: "PrivateLink connection health status",
            links: &const {
                [OntologyLink {
                    name: "status_of",
                    target: "connection",
                    properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
                }]
            },
            column_semantic_types: &[("id", SemanticType::CatalogItemId)],
        }),
    }
});

pub static MZ_STATEMENT_EXECUTION_HISTORY: LazyLock<BuiltinSource> =
    LazyLock::new(|| BuiltinSource {
        name: "mz_statement_execution_history",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_STATEMENT_EXECUTION_HISTORY_OID,
        data_source: IntrospectionType::StatementExecutionHistory.into(),
        desc: MZ_STATEMENT_EXECUTION_HISTORY_DESC.clone(),
        column_comments: BTreeMap::new(),
        is_retained_metrics_object: false,
        access: vec![MONITOR_SELECT],
        ontology: None,
    });

pub static MZ_STATEMENT_EXECUTION_HISTORY_REDACTED: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
    name: "mz_statement_execution_history_redacted",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_STATEMENT_EXECUTION_HISTORY_REDACTED_OID,
    // everything but `params` and `error_message`
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::Uuid.nullable(false))
        .with_column("prepared_statement_id", SqlScalarType::Uuid.nullable(false))
        .with_column("sample_rate", SqlScalarType::Float64.nullable(false))
        .with_column("cluster_id", SqlScalarType::String.nullable(true))
        .with_column("application_name", SqlScalarType::String.nullable(false))
        .with_column("cluster_name", SqlScalarType::String.nullable(true))
        .with_column("database_name", SqlScalarType::String.nullable(false))
        .with_column("search_path", SqlScalarType::List { element_type: Box::new(SqlScalarType::String), custom_id: None }.nullable(false))
        .with_column("transaction_isolation", SqlScalarType::String.nullable(false))
        .with_column("execution_timestamp", SqlScalarType::UInt64.nullable(true))
        .with_column("transaction_id", SqlScalarType::UInt64.nullable(false))
        .with_column("transient_index_id", SqlScalarType::String.nullable(true))
        .with_column("mz_version", SqlScalarType::String.nullable(false))
        .with_column("began_at", SqlScalarType::TimestampTz { precision: None }.nullable(false))
        .with_column("finished_at", SqlScalarType::TimestampTz { precision: None }.nullable(true))
        .with_column("finished_status", SqlScalarType::String.nullable(true))
        .with_column("result_size", SqlScalarType::Int64.nullable(true))
        .with_column("rows_returned", SqlScalarType::Int64.nullable(true))
        .with_column("execution_strategy", SqlScalarType::String.nullable(true))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT id, prepared_statement_id, sample_rate, cluster_id, application_name,
cluster_name, database_name, search_path, transaction_isolation, execution_timestamp, transaction_id,
transient_index_id, mz_version, began_at, finished_at, finished_status,
result_size, rows_returned, execution_strategy
FROM mz_internal.mz_statement_execution_history",
    access: vec![SUPPORT_SELECT, ANALYTICS_SELECT, MONITOR_REDACTED_SELECT, MONITOR_SELECT],
    ontology: None,
}
});

pub static MZ_PREPARED_STATEMENT_HISTORY: LazyLock<BuiltinSource> =
    LazyLock::new(|| BuiltinSource {
        name: "mz_prepared_statement_history",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_PREPARED_STATEMENT_HISTORY_OID,
        data_source: IntrospectionType::PreparedStatementHistory.into(),
        desc: MZ_PREPARED_STATEMENT_HISTORY_DESC.clone(),
        column_comments: BTreeMap::new(),
        is_retained_metrics_object: false,
        access: vec![
            SUPPORT_SELECT,
            ANALYTICS_SELECT,
            MONITOR_REDACTED_SELECT,
            MONITOR_SELECT,
        ],
        ontology: None,
    });

pub static MZ_SQL_TEXT: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_sql_text",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_SQL_TEXT_OID,
    desc: MZ_SQL_TEXT_DESC.clone(),
    data_source: IntrospectionType::SqlText.into(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: false,
    access: vec![MONITOR_SELECT],
    ontology: Some(Ontology {
        entity_name: "sql_text",
        description: "Raw SQL text of executed statements",
        links: &const { [] },
        column_semantic_types: &[],
    }),
});

pub static MZ_SQL_TEXT_REDACTED: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_sql_text_redacted",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SQL_TEXT_REDACTED_OID,
    desc: RelationDesc::builder()
        .with_column("sql_hash", SqlScalarType::Bytes.nullable(false))
        .with_column("redacted_sql", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT sql_hash, redacted_sql FROM mz_internal.mz_sql_text",
    access: vec![
        MONITOR_SELECT,
        MONITOR_REDACTED_SELECT,
        SUPPORT_SELECT,
        ANALYTICS_SELECT,
    ],
    ontology: None,
});

pub static MZ_RECENT_SQL_TEXT: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "mz_recent_sql_text",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_RECENT_SQL_TEXT_OID,
        // This should always be 1 day more than the interval in
        // `MZ_RECENT_THINNED_ACTIVITY_LOG` , because `prepared_day`
        // is rounded down to the nearest day.  Thus something that actually happened three days ago
        // could have a `prepared day` anywhere from 3 to 4 days back.
        desc: RelationDesc::builder()
            .with_column("sql_hash", SqlScalarType::Bytes.nullable(false))
            .with_column("sql", SqlScalarType::String.nullable(false))
            .with_column("redacted_sql", SqlScalarType::String.nullable(false))
            .with_key(vec![0, 1, 2])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT DISTINCT sql_hash, sql, redacted_sql FROM mz_internal.mz_sql_text WHERE prepared_day + INTERVAL '4 days' >= mz_now()",
        access: vec![MONITOR_SELECT],
        ontology: Some(Ontology {
            entity_name: "recent_sql_text",
            description: "Recent SQL text (indexed, last ~3-4 days)",
            links: &const { [] },
            column_semantic_types: &[("sql", SemanticType::SqlDefinition)],
        }),
    }
});

pub static MZ_RECENT_SQL_TEXT_REDACTED: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_recent_sql_text_redacted",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_RECENT_SQL_TEXT_REDACTED_OID,
    desc: RelationDesc::builder()
        .with_column("sql_hash", SqlScalarType::Bytes.nullable(false))
        .with_column("redacted_sql", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT sql_hash, redacted_sql FROM mz_internal.mz_recent_sql_text",
    access: vec![
        MONITOR_SELECT,
        MONITOR_REDACTED_SELECT,
        SUPPORT_SELECT,
        ANALYTICS_SELECT,
    ],
    ontology: None,
});

pub static MZ_RECENT_SQL_TEXT_IND: LazyLock<BuiltinIndex> = LazyLock::new(|| BuiltinIndex {
    name: "mz_recent_sql_text_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_RECENT_SQL_TEXT_IND_OID,
    sql: "IN CLUSTER mz_catalog_server ON mz_internal.mz_recent_sql_text (sql_hash)",
    is_retained_metrics_object: false,
});

pub static MZ_SESSION_HISTORY: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_session_history",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_SESSION_HISTORY_OID,
    data_source: IntrospectionType::SessionHistory.into(),
    desc: MZ_SESSION_HISTORY_DESC.clone(),
    column_comments: BTreeMap::from_iter([
        (
            "session_id",
            "The globally unique ID of the session. Corresponds to `mz_sessions.id`.",
        ),
        (
            "connected_at",
            "The time at which the session was established.",
        ),
        (
            "initial_application_name",
            "The `application_name` session metadata field.",
        ),
        (
            "authenticated_user",
            "The name of the user for which the session was established.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "session",
        description: "Historical session connection events",
        links: &const {
            [OntologyLink {
                name: "active_as",
                target: "active_session",
                properties: LinkProperties::fk_nullable("session_id", "id", Cardinality::ManyToOne),
            }]
        },
        column_semantic_types: &[("connected_at", SemanticType::WallclockTimestamp)],
    }),
});

pub static MZ_ACTIVITY_LOG_THINNED: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "mz_activity_log_thinned",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_ACTIVITY_LOG_THINNED_OID,
        desc: RelationDesc::builder()
            .with_column("execution_id", SqlScalarType::Uuid.nullable(false))
            .with_column("sample_rate", SqlScalarType::Float64.nullable(false))
            .with_column("cluster_id", SqlScalarType::String.nullable(true))
            .with_column("application_name", SqlScalarType::String.nullable(false))
            .with_column("cluster_name", SqlScalarType::String.nullable(true))
            .with_column("database_name", SqlScalarType::String.nullable(false))
            .with_column("search_path", SqlScalarType::List { element_type: Box::new(SqlScalarType::String), custom_id: None }.nullable(false))
            .with_column("transaction_isolation", SqlScalarType::String.nullable(false))
            .with_column("execution_timestamp", SqlScalarType::UInt64.nullable(true))
            .with_column("transient_index_id", SqlScalarType::String.nullable(true))
            .with_column("params", SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false))
            .with_column("mz_version", SqlScalarType::String.nullable(false))
            .with_column("began_at", SqlScalarType::TimestampTz { precision: None }.nullable(false))
            .with_column("finished_at", SqlScalarType::TimestampTz { precision: None }.nullable(true))
            .with_column("finished_status", SqlScalarType::String.nullable(true))
            .with_column("error_message", SqlScalarType::String.nullable(true))
            .with_column("result_size", SqlScalarType::Int64.nullable(true))
            .with_column("rows_returned", SqlScalarType::Int64.nullable(true))
            .with_column("execution_strategy", SqlScalarType::String.nullable(true))
            .with_column("transaction_id", SqlScalarType::UInt64.nullable(false))
            .with_column("prepared_statement_id", SqlScalarType::Uuid.nullable(false))
            .with_column("sql_hash", SqlScalarType::Bytes.nullable(false))
            .with_column("prepared_statement_name", SqlScalarType::String.nullable(false))
            .with_column("session_id", SqlScalarType::Uuid.nullable(false))
            .with_column("prepared_at", SqlScalarType::TimestampTz { precision: None }.nullable(false))
            .with_column("statement_type", SqlScalarType::String.nullable(true))
            .with_column("throttled_count", SqlScalarType::UInt64.nullable(false))
            .with_column("connected_at", SqlScalarType::TimestampTz { precision: None }.nullable(false))
            .with_column("initial_application_name", SqlScalarType::String.nullable(false))
            .with_column("authenticated_user", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT mseh.id AS execution_id, sample_rate, cluster_id, application_name, cluster_name, database_name, search_path,
transaction_isolation, execution_timestamp, transient_index_id, params, mz_version, began_at, finished_at, finished_status,
error_message, result_size, rows_returned, execution_strategy, transaction_id,
mpsh.id AS prepared_statement_id, sql_hash, mpsh.name AS prepared_statement_name,
mpsh.session_id, prepared_at, statement_type, throttled_count,
connected_at, initial_application_name, authenticated_user
FROM mz_internal.mz_statement_execution_history mseh,
     mz_internal.mz_prepared_statement_history mpsh,
     mz_internal.mz_session_history msh
WHERE mseh.prepared_statement_id = mpsh.id
AND mpsh.session_id = msh.session_id",
        access: vec![MONITOR_SELECT],
        ontology: None,
    }
});

pub static MZ_RECENT_ACTIVITY_LOG_THINNED: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "mz_recent_activity_log_thinned",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_RECENT_ACTIVITY_LOG_THINNED_OID,
        desc: RelationDesc::builder()
            .with_column("execution_id", SqlScalarType::Uuid.nullable(false))
            .with_column("sample_rate", SqlScalarType::Float64.nullable(false))
            .with_column("cluster_id", SqlScalarType::String.nullable(true))
            .with_column("application_name", SqlScalarType::String.nullable(false))
            .with_column("cluster_name", SqlScalarType::String.nullable(true))
            .with_column("database_name", SqlScalarType::String.nullable(false))
            .with_column("search_path", SqlScalarType::List { element_type: Box::new(SqlScalarType::String), custom_id: None }.nullable(false))
            .with_column("transaction_isolation", SqlScalarType::String.nullable(false))
            .with_column("execution_timestamp", SqlScalarType::UInt64.nullable(true))
            .with_column("transient_index_id", SqlScalarType::String.nullable(true))
            .with_column("params", SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false))
            .with_column("mz_version", SqlScalarType::String.nullable(false))
            .with_column("began_at", SqlScalarType::TimestampTz { precision: None }.nullable(false))
            .with_column("finished_at", SqlScalarType::TimestampTz { precision: None }.nullable(true))
            .with_column("finished_status", SqlScalarType::String.nullable(true))
            .with_column("error_message", SqlScalarType::String.nullable(true))
            .with_column("result_size", SqlScalarType::Int64.nullable(true))
            .with_column("rows_returned", SqlScalarType::Int64.nullable(true))
            .with_column("execution_strategy", SqlScalarType::String.nullable(true))
            .with_column("transaction_id", SqlScalarType::UInt64.nullable(false))
            .with_column("prepared_statement_id", SqlScalarType::Uuid.nullable(false))
            .with_column("sql_hash", SqlScalarType::Bytes.nullable(false))
            .with_column("prepared_statement_name", SqlScalarType::String.nullable(false))
            .with_column("session_id", SqlScalarType::Uuid.nullable(false))
            .with_column("prepared_at", SqlScalarType::TimestampTz { precision: None }.nullable(false))
            .with_column("statement_type", SqlScalarType::String.nullable(true))
            .with_column("throttled_count", SqlScalarType::UInt64.nullable(false))
            .with_column("connected_at", SqlScalarType::TimestampTz { precision: None }.nullable(false))
            .with_column("initial_application_name", SqlScalarType::String.nullable(false))
            .with_column("authenticated_user", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        // We use a temporal window of 2 days rather than 1 day for `mz_session_history`'s `connected_at` since a statement execution at
        // the edge of the 1 day temporal window could've been executed in a session that was established an hour before the 1 day window.
        sql:
        "SELECT * FROM mz_internal.mz_activity_log_thinned WHERE prepared_at + INTERVAL '1 day' > mz_now()
AND began_at + INTERVAL '1 day' > mz_now() AND connected_at + INTERVAL '2 days' > mz_now()",
        access: vec![MONITOR_SELECT],
        ontology: None,
    }
});

pub static MZ_RECENT_ACTIVITY_LOG: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_recent_activity_log",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_RECENT_ACTIVITY_LOG_OID,
    desc: RelationDesc::builder()
        .with_column("execution_id", SqlScalarType::Uuid.nullable(false))
        .with_column("sample_rate", SqlScalarType::Float64.nullable(false))
        .with_column("cluster_id", SqlScalarType::String.nullable(true))
        .with_column("application_name", SqlScalarType::String.nullable(false))
        .with_column("cluster_name", SqlScalarType::String.nullable(true))
        .with_column("database_name", SqlScalarType::String.nullable(false))
        .with_column(
            "search_path",
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::String),
                custom_id: None,
            }
            .nullable(false),
        )
        .with_column(
            "transaction_isolation",
            SqlScalarType::String.nullable(false),
        )
        .with_column("execution_timestamp", SqlScalarType::UInt64.nullable(true))
        .with_column("transient_index_id", SqlScalarType::String.nullable(true))
        .with_column(
            "params",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false),
        )
        .with_column("mz_version", SqlScalarType::String.nullable(false))
        .with_column(
            "began_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            "finished_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(true),
        )
        .with_column("finished_status", SqlScalarType::String.nullable(true))
        .with_column("error_message", SqlScalarType::String.nullable(true))
        .with_column("result_size", SqlScalarType::Int64.nullable(true))
        .with_column("rows_returned", SqlScalarType::Int64.nullable(true))
        .with_column("execution_strategy", SqlScalarType::String.nullable(true))
        .with_column("transaction_id", SqlScalarType::UInt64.nullable(false))
        .with_column("prepared_statement_id", SqlScalarType::Uuid.nullable(false))
        .with_column("sql_hash", SqlScalarType::Bytes.nullable(false))
        .with_column(
            "prepared_statement_name",
            SqlScalarType::String.nullable(false),
        )
        .with_column("session_id", SqlScalarType::Uuid.nullable(false))
        .with_column(
            "prepared_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column("statement_type", SqlScalarType::String.nullable(true))
        .with_column("throttled_count", SqlScalarType::UInt64.nullable(false))
        .with_column(
            "connected_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            "initial_application_name",
            SqlScalarType::String.nullable(false),
        )
        .with_column("authenticated_user", SqlScalarType::String.nullable(false))
        .with_column("sql", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "execution_id",
            "An ID that is unique for each executed statement.",
        ),
        (
            "sample_rate",
            "The actual rate at which the statement was sampled.",
        ),
        (
            "cluster_id",
            "The ID of the cluster the statement execution was directed to. Corresponds to mz_clusters.id.",
        ),
        (
            "application_name",
            "The value of the `application_name` configuration parameter at execution time.",
        ),
        (
            "cluster_name",
            "The name of the cluster with ID `cluster_id` at execution time.",
        ),
        (
            "database_name",
            "The value of the `database` configuration parameter at execution time.",
        ),
        (
            "search_path",
            "The value of the `search_path` configuration parameter at execution time.",
        ),
        (
            "transaction_isolation",
            "The value of the `transaction_isolation` configuration parameter at execution time.",
        ),
        (
            "execution_timestamp",
            "The logical timestamp at which execution was scheduled.",
        ),
        (
            "transient_index_id",
            "The internal index of the compute dataflow created for the query, if any.",
        ),
        (
            "params",
            "The parameters with which the statement was executed.",
        ),
        (
            "mz_version",
            "The version of Materialize that was running when the statement was executed.",
        ),
        (
            "began_at",
            "The wall-clock time at which the statement began executing.",
        ),
        (
            "finished_at",
            "The wall-clock time at which the statement finished executing.",
        ),
        (
            "finished_status",
            "The final status of the statement (e.g., `success`, `canceled`, `error`, or `aborted`). `aborted` means that Materialize exited before the statement finished executing.",
        ),
        (
            "error_message",
            "The error message, if the statement failed.",
        ),
        (
            "result_size",
            "The size in bytes of the result, for statements that return rows.",
        ),
        (
            "rows_returned",
            "The number of rows returned, for statements that return rows.",
        ),
        (
            "execution_strategy",
            "For `SELECT` queries, the strategy for executing the query. `constant` means computed in the control plane without the involvement of a cluster, `fast-path` means read by a cluster directly from an in-memory index, and `standard` means computed by a temporary dataflow.",
        ),
        (
            "transaction_id",
            "The ID of the transaction that the statement was part of. Note that transaction IDs are only unique per session.",
        ),
        (
            "prepared_statement_id",
            "An ID that is unique for each prepared statement. For example, if a statement is prepared once and then executed multiple times, all executions will have the same value for this column (but different values for `execution_id`).",
        ),
        (
            "sql_hash",
            "An opaque value uniquely identifying the text of the query.",
        ),
        (
            "prepared_statement_name",
            "The name given by the client library to the prepared statement.",
        ),
        (
            "session_id",
            "An ID that is unique for each session. Corresponds to mz_sessions.id.",
        ),
        (
            "prepared_at",
            "The time at which the statement was prepared.",
        ),
        (
            "statement_type",
            "The _type_ of the statement, e.g. `select` for a `SELECT` query, or `NULL` if the statement was empty.",
        ),
        (
            "throttled_count",
            "The number of statement executions that were dropped due to throttling before the current one was seen. If you have a very high volume of queries and need to log them without throttling, contact our team.",
        ),
        (
            "connected_at",
            "The time at which the session was established.",
        ),
        (
            "initial_application_name",
            "The initial value of `application_name` at the beginning of the session.",
        ),
        (
            "authenticated_user",
            "The name of the user for which the session was established.",
        ),
        ("sql", "The SQL text of the statement."),
    ]),
    sql: "SELECT mralt.*, mrst.sql
FROM mz_internal.mz_recent_activity_log_thinned mralt,
     mz_internal.mz_recent_sql_text mrst
WHERE mralt.sql_hash = mrst.sql_hash",
    access: vec![MONITOR_SELECT],
    ontology: Some(Ontology {
        entity_name: "activity_log",
        description: "Recent query activity with execution stats",
        links: &const {
            [
                OntologyLink {
                    name: "in_session",
                    target: "session",
                    properties: LinkProperties::fk("session_id", "id", Cardinality::ManyToOne),
                },
                OntologyLink {
                    name: "in_active_session",
                    target: "active_session",
                    properties: LinkProperties::fk_nullable(
                        "session_id",
                        "id",
                        Cardinality::ManyToOne,
                    ),
                },
                OntologyLink {
                    name: "ran_on_cluster",
                    target: "cluster",
                    properties: LinkProperties::fk_nullable(
                        "cluster_id",
                        "id",
                        Cardinality::ManyToOne,
                    ),
                },
                OntologyLink {
                    name: "used_transient_index",
                    target: "object",
                    properties: LinkProperties::ForeignKey {
                        source_column: "transient_index_id",
                        target_column: "id",
                        cardinality: Cardinality::ManyToOne,
                        source_id_type: Some(mz_repr::SemanticType::GlobalId),
                        requires_mapping: Some("mz_internal.mz_object_global_ids"),
                        nullable: true,
                        note: None,
                        extra_key_columns: None,
                    },
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("cluster_id", SemanticType::ClusterId),
                ("execution_timestamp", SemanticType::MzTimestamp),
                ("transient_index_id", SemanticType::GlobalId),
                ("began_at", SemanticType::WallclockTimestamp),
                ("finished_at", SemanticType::WallclockTimestamp),
                ("prepared_at", SemanticType::WallclockTimestamp),
                ("connected_at", SemanticType::WallclockTimestamp),
                ("sql", SemanticType::SqlDefinition),
            ]
        },
    }),
});

pub static MZ_RECENT_ACTIVITY_LOG_REDACTED: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
    name: "mz_recent_activity_log_redacted",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_RECENT_ACTIVITY_LOG_REDACTED_OID,
    // Includes all the columns in mz_recent_activity_log_thinned except 'error_message'.
    desc: RelationDesc::builder()
        .with_column("execution_id", SqlScalarType::Uuid.nullable(false))
        .with_column("sample_rate", SqlScalarType::Float64.nullable(false))
        .with_column("cluster_id", SqlScalarType::String.nullable(true))
        .with_column("application_name", SqlScalarType::String.nullable(false))
        .with_column("cluster_name", SqlScalarType::String.nullable(true))
        .with_column("database_name", SqlScalarType::String.nullable(false))
        .with_column("search_path", SqlScalarType::List { element_type: Box::new(SqlScalarType::String), custom_id: None }.nullable(false))
        .with_column("transaction_isolation", SqlScalarType::String.nullable(false))
        .with_column("execution_timestamp", SqlScalarType::UInt64.nullable(true))
        .with_column("transient_index_id", SqlScalarType::String.nullable(true))
        .with_column("mz_version", SqlScalarType::String.nullable(false))
        .with_column("began_at", SqlScalarType::TimestampTz { precision: None }.nullable(false))
        .with_column("finished_at", SqlScalarType::TimestampTz { precision: None }.nullable(true))
        .with_column("finished_status", SqlScalarType::String.nullable(true))
        .with_column("result_size", SqlScalarType::Int64.nullable(true))
        .with_column("rows_returned", SqlScalarType::Int64.nullable(true))
        .with_column("execution_strategy", SqlScalarType::String.nullable(true))
        .with_column("transaction_id", SqlScalarType::UInt64.nullable(false))
        .with_column("prepared_statement_id", SqlScalarType::Uuid.nullable(false))
        .with_column("sql_hash", SqlScalarType::Bytes.nullable(false))
        .with_column("prepared_statement_name", SqlScalarType::String.nullable(false))
        .with_column("session_id", SqlScalarType::Uuid.nullable(false))
        .with_column("prepared_at", SqlScalarType::TimestampTz { precision: None }.nullable(false))
        .with_column("statement_type", SqlScalarType::String.nullable(true))
        .with_column("throttled_count", SqlScalarType::UInt64.nullable(false))
        .with_column("initial_application_name", SqlScalarType::String.nullable(false))
        .with_column("authenticated_user", SqlScalarType::String.nullable(false))
        .with_column("redacted_sql", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT mralt.execution_id, mralt.sample_rate, mralt.cluster_id, mralt.application_name,
    mralt.cluster_name, mralt.database_name, mralt.search_path, mralt.transaction_isolation, mralt.execution_timestamp,
    mralt.transient_index_id, mralt.mz_version, mralt.began_at, mralt.finished_at,
    mralt.finished_status, mralt.result_size, mralt.rows_returned, mralt.execution_strategy, mralt.transaction_id,
    mralt.prepared_statement_id, mralt.sql_hash, mralt.prepared_statement_name, mralt.session_id,
    mralt.prepared_at, mralt.statement_type, mralt.throttled_count,
    mralt.initial_application_name, mralt.authenticated_user,
    mrst.redacted_sql
FROM mz_internal.mz_recent_activity_log_thinned mralt,
     mz_internal.mz_recent_sql_text mrst
WHERE mralt.sql_hash = mrst.sql_hash",
    access: vec![MONITOR_SELECT, MONITOR_REDACTED_SELECT, SUPPORT_SELECT, ANALYTICS_SELECT],
    ontology: None,
}
});

pub static MZ_STATEMENT_LIFECYCLE_HISTORY: LazyLock<BuiltinSource> = LazyLock::new(|| {
    BuiltinSource {
        name: "mz_statement_lifecycle_history",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_STATEMENT_LIFECYCLE_HISTORY_OID,
        desc: RelationDesc::builder()
            .with_column("statement_id", SqlScalarType::Uuid.nullable(false))
            .with_column("event_type", SqlScalarType::String.nullable(false))
            .with_column(
                "occurred_at",
                SqlScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .finish(),
        data_source: IntrospectionType::StatementLifecycleHistory.into(),
        column_comments: BTreeMap::from_iter([
            (
                "statement_id",
                "The ID of the execution event. Corresponds to `mz_recent_activity_log.execution_id`",
            ),
            (
                "event_type",
                "The type of lifecycle event, e.g. `'execution-began'`, `'storage-dependencies-finished'`, `'compute-dependencies-finished'`, or `'execution-finished'`",
            ),
            ("occurred_at", "The time at which the event took place."),
        ]),
        is_retained_metrics_object: false,
        // TODO[btv]: Maybe this should be public instead of
        // `MONITOR_REDACTED`, but since that would be a backwards-compatible
        // change, we probably don't need to worry about it now.
        access: vec![
            SUPPORT_SELECT,
            ANALYTICS_SELECT,
            MONITOR_REDACTED_SELECT,
            MONITOR_SELECT,
        ],
        ontology: Some(Ontology {
            entity_name: "statement_lifecycle_event",
            description: "Statement lifecycle events (parse, bind, execute)",
            links: &const {
                [OntologyLink {
                    name: "for_execution",
                    target: "activity_log",
                    properties: LinkProperties::fk(
                        "statement_id",
                        "execution_id",
                        Cardinality::ManyToOne,
                    ),
                }]
            },
            column_semantic_types: &[],
        }),
    }
});

pub static MZ_SOURCE_STATUSES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_source_statuses",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SOURCE_STATUSES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("type", SqlScalarType::String.nullable(false))
        .with_column(
            "last_status_change_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(true),
        )
        .with_column("status", SqlScalarType::String.nullable(false))
        .with_column("error", SqlScalarType::String.nullable(true))
        .with_column("details", SqlScalarType::Jsonb.nullable(true))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the source. Corresponds to `mz_catalog.mz_sources.id`.",
        ),
        ("name", "The name of the source."),
        ("type", "The type of the source."),
        (
            "last_status_change_at",
            "Wall-clock timestamp of the source status change.",
        ),
        (
            "status",
            "The status of the source: one of `created`, `starting`, `running`, `paused`, `stalled`, `failed`, or `dropped`.",
        ),
        (
            "error",
            "If the source is in an error state, the error message.",
        ),
        (
            "details",
            "Additional metadata provided by the source. In case of error, may contain a `hint` field with helpful suggestions.",
        ),
    ]),
    sql: "
    WITH
    -- The status history contains per-replica events and source-global events.
    -- For the latter, replica_id is NULL. We turn these into '<source>', so that
    -- we can treat them uniformly below.
    uniform_status_history AS
    (
        SELECT
            s.source_id,
            COALESCE(s.replica_id, '<source>') as replica_id,
            s.occurred_at,
            s.status,
            s.error,
            s.details
        FROM mz_internal.mz_source_status_history s
    ),
    -- For getting the latest events, we first determine the latest per-replica
    -- events here and then apply precedence rules below.
    latest_per_replica_events AS
    (
        SELECT DISTINCT ON (source_id, replica_id)
            occurred_at, source_id, replica_id, status, error, details
        FROM uniform_status_history
        ORDER BY source_id, replica_id, occurred_at DESC
    ),
    -- We have a precedence list that determines the overall status in case
    -- there is differing per-replica (including source-global) statuses. If
    -- there is no 'dropped' status, and any replica reports 'running', the
    -- overall status is 'running' even if there might be some replica that has
    -- errors or is paused.
    latest_events AS
    (
       SELECT DISTINCT ON (source_id)
            source_id,
            occurred_at,
            status,
            error,
            details
        FROM latest_per_replica_events
        ORDER BY source_id, CASE status
                    WHEN 'dropped' THEN 1
                    WHEN 'running' THEN 2
                    WHEN 'stalled' THEN 3
                    WHEN 'starting' THEN 4
                    WHEN 'paused' THEN 5
                    WHEN 'ceased' THEN 6
                    ELSE 7  -- For any other status values
                END
    ),
    -- Determine which sources are subsources and which are parent sources
    subsources AS
    (
        SELECT subsources.id AS self, sources.id AS parent
        FROM
            mz_catalog.mz_sources AS subsources
                JOIN
                    mz_internal.mz_object_dependencies AS deps
                    ON subsources.id = deps.object_id
                JOIN mz_catalog.mz_sources AS sources ON sources.id = deps.referenced_object_id
    ),
    -- Determine which sources are source tables
    tables AS
    (
        SELECT tables.id AS self, tables.source_id AS parent, tables.name
        FROM mz_catalog.mz_tables AS tables
        WHERE tables.source_id IS NOT NULL
    ),
    -- Determine which collection's ID to use for the status
    id_of_status_to_use AS
    (
        SELECT
            self_events.source_id,
            -- If self not errored, but parent is, use parent; else self
            CASE
                WHEN
                    self_events.status <> 'ceased' AND
                    parent_events.status = 'stalled'
                THEN parent_events.source_id
                ELSE self_events.source_id
            END AS id_to_use
        FROM
            latest_events AS self_events
                LEFT JOIN subsources ON self_events.source_id = subsources.self
                LEFT JOIN tables ON self_events.source_id = tables.self
                LEFT JOIN
                    latest_events AS parent_events
                    ON parent_events.source_id = COALESCE(subsources.parent, tables.parent)
    ),
    -- Swap out events for the ID of the event we plan to use instead
    latest_events_to_use AS
    (
        SELECT occurred_at, s.source_id, status, error, details
        FROM
            id_of_status_to_use AS s
                JOIN latest_events AS e ON e.source_id = s.id_to_use
    ),
    combined AS (
        SELECT
            mz_sources.id,
            mz_sources.name,
            mz_sources.type,
            occurred_at,
            status,
            error,
            details
        FROM
            mz_catalog.mz_sources
            LEFT JOIN latest_events_to_use AS e ON mz_sources.id = e.source_id
        UNION ALL
        SELECT
            tables.self AS id,
            tables.name,
            'table' AS type,
            occurred_at,
            status,
            error,
            details
        FROM
            tables
            LEFT JOIN latest_events_to_use AS e ON tables.self = e.source_id
    )
SELECT
    id,
    name,
    type,
    occurred_at AS last_status_change_at,
    -- TODO(parkmycar): Report status of webhook source once database-issues#5986 is closed.
    CASE
        WHEN
            type = 'webhook' OR
            type = 'progress'
        THEN 'running'
        ELSE COALESCE(status, 'created')
    END AS status,
    error,
    details
FROM combined
WHERE id NOT LIKE 's%';",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "source_status",
        description: "Current source status (running, stalled, etc.)",
        links: &const {
            [OntologyLink {
                name: "status_of_source",
                target: "source",
                properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &const {
            [
                ("id", SemanticType::CatalogItemId),
                ("type", SemanticType::SourceType),
                ("last_status_change_at", SemanticType::WallclockTimestamp),
            ]
        },
    }),
});

pub static MZ_SINK_STATUS_HISTORY: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_sink_status_history",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_SINK_STATUS_HISTORY_OID,
    data_source: IntrospectionType::SinkStatusHistory.into(),
    desc: MZ_SINK_STATUS_HISTORY_DESC.clone(),
    column_comments: BTreeMap::from_iter([
        (
            "occurred_at",
            "Wall-clock timestamp of the sink status change.",
        ),
        (
            "sink_id",
            "The ID of the sink. Corresponds to `mz_catalog.mz_sinks.id`.",
        ),
        (
            "status",
            "The status of the sink: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.",
        ),
        (
            "error",
            "If the sink is in an error state, the error message.",
        ),
        (
            "details",
            "Additional metadata provided by the sink. In case of error, may contain a `hint` field with helpful suggestions.",
        ),
        (
            "replica_id",
            "The ID of the replica that an instance of a sink is running on.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "sink_status_event",
        description: "Historical sink status events",
        links: &const {
            [
                OntologyLink {
                    name: "status_event_of_sink",
                    target: "sink",
                    properties: LinkProperties::fk_mapped(
                        "sink_id",
                        "id",
                        Cardinality::ManyToOne,
                        mz_repr::SemanticType::GlobalId,
                        "mz_internal.mz_object_global_ids",
                    ),
                },
                OntologyLink {
                    name: "on_replica",
                    target: "replica",
                    properties: LinkProperties::fk_nullable(
                        "replica_id",
                        "id",
                        Cardinality::ManyToOne,
                    ),
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("occurred_at", SemanticType::WallclockTimestamp),
                ("sink_id", SemanticType::GlobalId),
                ("replica_id", SemanticType::ReplicaId),
            ]
        },
    }),
});

pub static MZ_SINK_STATUSES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_sink_statuses",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SINK_STATUSES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("type", SqlScalarType::String.nullable(false))
        .with_column(
            "last_status_change_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(true),
        )
        .with_column("status", SqlScalarType::String.nullable(false))
        .with_column("error", SqlScalarType::String.nullable(true))
        .with_column("details", SqlScalarType::Jsonb.nullable(true))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the sink. Corresponds to `mz_catalog.mz_sinks.id`.",
        ),
        ("name", "The name of the sink."),
        ("type", "The type of the sink."),
        (
            "last_status_change_at",
            "Wall-clock timestamp of the sink status change.",
        ),
        (
            "status",
            "The status of the sink: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.",
        ),
        (
            "error",
            "If the sink is in an error state, the error message.",
        ),
        (
            "details",
            "Additional metadata provided by the sink. In case of error, may contain a `hint` field with helpful suggestions.",
        ),
    ]),
    sql: "
WITH
-- The status history contains per-replica events and sink-global events.
-- For the latter, replica_id is NULL. We turn these into '<sink>', so that
-- we can treat them uniformly below.
uniform_status_history AS
(
    SELECT
        s.sink_id,
        COALESCE(s.replica_id, '<sink>') as replica_id,
        s.occurred_at,
        s.status,
        s.error,
        s.details
    FROM mz_internal.mz_sink_status_history s
),
-- For getting the latest events, we first determine the latest per-replica
-- events here and then apply precedence rules below.
latest_per_replica_events AS
(
    SELECT DISTINCT ON (sink_id, replica_id)
        occurred_at, sink_id, replica_id, status, error, details
    FROM uniform_status_history
    ORDER BY sink_id, replica_id, occurred_at DESC
),
-- We have a precedence list that determines the overall status in case
-- there is differing per-replica (including sink-global) statuses. If
-- there is no 'dropped' status, and any replica reports 'running', the
-- overall status is 'running' even if there might be some replica that has
-- errors or is paused.
latest_events AS
(
    SELECT DISTINCT ON (sink_id)
        sink_id,
        occurred_at,
        status,
        error,
        details
    FROM latest_per_replica_events
    ORDER BY sink_id, CASE status
                WHEN 'dropped' THEN 1
                WHEN 'running' THEN 2
                WHEN 'stalled' THEN 3
                WHEN 'starting' THEN 4
                WHEN 'paused' THEN 5
                WHEN 'ceased' THEN 6
                ELSE 7  -- For any other status values
            END
)
SELECT
    mz_sinks.id,
    name,
    mz_sinks.type,
    occurred_at as last_status_change_at,
    coalesce(status, 'created') as status,
    error,
    details
FROM mz_catalog.mz_sinks
LEFT JOIN latest_events ON mz_sinks.id = latest_events.sink_id
WHERE
    -- This is a convenient way to filter out system sinks, like the status_history table itself.
    mz_sinks.id NOT LIKE 's%'",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "sink_status",
        description: "Current sink status",
        links: &const {
            [OntologyLink {
                name: "status_of_sink",
                target: "sink",
                properties: LinkProperties::fk_typed(
                    "id",
                    "id",
                    Cardinality::OneToOne,
                    mz_repr::SemanticType::CatalogItemId,
                ),
            }]
        },
        column_semantic_types: &const {
            [
                ("id", SemanticType::CatalogItemId),
                ("last_status_change_at", SemanticType::WallclockTimestamp),
            ]
        },
    }),
});

pub static MZ_STORAGE_USAGE_BY_SHARD: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_storage_usage_by_shard",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_STORAGE_USAGE_BY_SHARD_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("shard_id", SqlScalarType::String.nullable(true))
        .with_column("size_bytes", SqlScalarType::UInt64.nullable(false))
        .with_column(
            "collection_timestamp",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .finish(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "storage_usage_by_shard",
        description: "Storage usage broken down by shard",
        links: &const { [] },
        column_semantic_types: &const {
            [
                ("shard_id", SemanticType::ShardId),
                ("size_bytes", SemanticType::ByteCount),
                ("collection_timestamp", SemanticType::WallclockTimestamp),
            ]
        },
    }),
});

pub static MZ_AWS_CONNECTIONS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_aws_connections",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_AWS_CONNECTIONS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("endpoint", SqlScalarType::String.nullable(true))
        .with_column("region", SqlScalarType::String.nullable(true))
        .with_column("access_key_id", SqlScalarType::String.nullable(true))
        .with_column(
            "access_key_id_secret_id",
            SqlScalarType::String.nullable(true),
        )
        .with_column(
            "secret_access_key_secret_id",
            SqlScalarType::String.nullable(true),
        )
        .with_column("session_token", SqlScalarType::String.nullable(true))
        .with_column(
            "session_token_secret_id",
            SqlScalarType::String.nullable(true),
        )
        .with_column("assume_role_arn", SqlScalarType::String.nullable(true))
        .with_column(
            "assume_role_session_name",
            SqlScalarType::String.nullable(true),
        )
        .with_column("principal", SqlScalarType::String.nullable(true))
        .with_column("external_id", SqlScalarType::String.nullable(true))
        .with_column("example_trust_policy", SqlScalarType::Jsonb.nullable(true))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the connection."),
        ("endpoint", "The value of the `ENDPOINT` option, if set."),
        ("region", "The value of the `REGION` option, if set."),
        (
            "access_key_id",
            "The value of the `ACCESS KEY ID` option, if provided in line.",
        ),
        (
            "access_key_id_secret_id",
            "The ID of the secret referenced by the `ACCESS KEY ID` option, if provided via a secret.",
        ),
        (
            "secret_access_key_secret_id",
            "The ID of the secret referenced by the `SECRET ACCESS KEY` option, if set.",
        ),
        (
            "session_token",
            "The value of the `SESSION TOKEN` option, if provided in line.",
        ),
        (
            "session_token_secret_id",
            "The ID of the secret referenced by the `SESSION TOKEN` option, if provided via a secret.",
        ),
        (
            "assume_role_arn",
            "The value of the `ASSUME ROLE ARN` option, if set.",
        ),
        (
            "assume_role_session_name",
            "The value of the `ASSUME ROLE SESSION NAME` option, if set.",
        ),
        (
            "principal",
            "The ARN of the AWS principal Materialize will use when assuming the provided role, if the connection is configured to use role assumption.",
        ),
        (
            "external_id",
            "The external ID Materialize will use when assuming the provided role, if the connection is configured to use role assumption.",
        ),
        (
            "example_trust_policy",
            "An example of an IAM role trust policy that allows this connection's principal and external ID to assume the role.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "aws_connection",
        description: "AWS connection configuration details",
        links: &const {
            [OntologyLink {
                name: "details_of",
                target: "connection",
                properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[],
    }),
});

pub static MZ_CLUSTER_REPLICA_METRICS_HISTORY: LazyLock<BuiltinSource> =
    LazyLock::new(|| BuiltinSource {
        name: "mz_cluster_replica_metrics_history",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_CLUSTER_REPLICA_METRICS_HISTORY_OID,
        data_source: IntrospectionType::ReplicaMetricsHistory.into(),
        desc: REPLICA_METRICS_HISTORY_DESC.clone(),
        column_comments: BTreeMap::from_iter([
            ("replica_id", "The ID of a cluster replica."),
            ("process_id", "The ID of a process within the replica."),
            (
                "cpu_nano_cores",
                "Approximate CPU usage, in billionths of a vCPU core.",
            ),
            ("memory_bytes", "Approximate memory usage, in bytes."),
            ("disk_bytes", "Approximate disk usage, in bytes."),
            (
                "occurred_at",
                "Wall-clock timestamp at which the event occurred.",
            ),
            (
                "heap_bytes",
                "Approximate heap (RAM + swap) usage, in bytes.",
            ),
            ("heap_limit", "Available heap (RAM + swap) space, in bytes."),
        ]),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_CLUSTER_REPLICA_METRICS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_cluster_replica_metrics",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_CLUSTER_REPLICA_METRICS_OID,
    desc: RelationDesc::builder()
        .with_column("replica_id", SqlScalarType::String.nullable(false))
        .with_column("process_id", SqlScalarType::UInt64.nullable(false))
        .with_column("cpu_nano_cores", SqlScalarType::UInt64.nullable(true))
        .with_column("memory_bytes", SqlScalarType::UInt64.nullable(true))
        .with_column("disk_bytes", SqlScalarType::UInt64.nullable(true))
        .with_column("heap_bytes", SqlScalarType::UInt64.nullable(true))
        .with_column("heap_limit", SqlScalarType::UInt64.nullable(true))
        .with_key(vec![0, 1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("replica_id", "The ID of a cluster replica."),
        ("process_id", "The ID of a process within the replica."),
        (
            "cpu_nano_cores",
            "Approximate CPU usage, in billionths of a vCPU core.",
        ),
        ("memory_bytes", "Approximate RAM usage, in bytes."),
        ("disk_bytes", "Approximate disk usage, in bytes."),
        (
            "heap_bytes",
            "Approximate heap (RAM + swap) usage, in bytes.",
        ),
        ("heap_limit", "Available heap (RAM + swap) space, in bytes."),
    ]),
    sql: "
SELECT
    DISTINCT ON (replica_id, process_id)
    replica_id,
    process_id,
    cpu_nano_cores,
    memory_bytes,
    disk_bytes,
    heap_bytes,
    heap_limit
FROM mz_internal.mz_cluster_replica_metrics_history
JOIN mz_cluster_replicas r ON r.id = replica_id
ORDER BY replica_id, process_id, occurred_at DESC",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "replica_metrics",
        description: "CPU and memory metrics per replica",
        links: &const {
            [OntologyLink {
                name: "metrics_of_replica",
                target: "replica",
                properties: LinkProperties::fk_typed(
                    "replica_id",
                    "id",
                    Cardinality::OneToOne,
                    mz_repr::SemanticType::CatalogItemId,
                ),
            }]
        },
        column_semantic_types: &const {
            [
                ("replica_id", SemanticType::ReplicaId),
                ("memory_bytes", SemanticType::ByteCount),
                ("disk_bytes", SemanticType::ByteCount),
                ("heap_bytes", SemanticType::ByteCount),
                ("heap_limit", SemanticType::ByteCount),
            ]
        },
    }),
});

pub static MZ_FRONTIERS: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_frontiers",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_FRONTIERS_OID,
    data_source: IntrospectionType::Frontiers.into(),
    desc: RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("read_frontier", SqlScalarType::MzTimestamp.nullable(true))
        .with_column("write_frontier", SqlScalarType::MzTimestamp.nullable(true))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "object_id",
            "The ID of the source, sink, table, index, materialized view, or subscription.",
        ),
        (
            "read_frontier",
            "The earliest timestamp at which the output is still readable.",
        ),
        (
            "write_frontier",
            "The next timestamp at which the output may change.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "frontier",
        description: "Current read/write frontiers for sources, sinks, tables, materialized views, indexes, and subscriptions",
        links: &const {
            [OntologyLink {
                name: "frontier_of",
                target: "object",
                properties: LinkProperties::fk_mapped(
                    "object_id",
                    "id",
                    Cardinality::ManyToOne,
                    mz_repr::SemanticType::GlobalId,
                    "mz_internal.mz_object_global_ids",
                ),
            }]
        },
        column_semantic_types: &const {
            [
                ("object_id", SemanticType::GlobalId),
                ("read_frontier", SemanticType::MzTimestamp),
                ("write_frontier", SemanticType::MzTimestamp),
            ]
        },
    }),
});

/// DEPRECATED and scheduled for removal! Use `mz_frontiers` instead.
pub static MZ_GLOBAL_FRONTIERS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_global_frontiers",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_GLOBAL_FRONTIERS_OID,
    desc: RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("time", SqlScalarType::MzTimestamp.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT object_id, write_frontier AS time
FROM mz_internal.mz_frontiers
WHERE write_frontier IS NOT NULL",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_WALLCLOCK_LAG_HISTORY: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_wallclock_lag_history",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_WALLCLOCK_LAG_HISTORY_OID,
    desc: WALLCLOCK_LAG_HISTORY_DESC.clone(),
    data_source: IntrospectionType::WallclockLagHistory.into(),
    column_comments: BTreeMap::from_iter([
        (
            "object_id",
            "The ID of the table, source, materialized view, index, or sink. Corresponds to `mz_objects.id`.",
        ),
        (
            "replica_id",
            "The ID of a replica computing the object, or `NULL` for persistent objects. Corresponds to `mz_cluster_replicas.id`.",
        ),
        (
            "lag",
            "The amount of time the object's write frontier lags behind wallclock time.",
        ),
        (
            "occurred_at",
            "Wall-clock timestamp at which the event occurred.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "wallclock_lag_event",
        description: "Historical wallclock lag per object",
        links: &const {
            [
                OntologyLink {
                    name: "measures_lag_of",
                    target: "object",
                    properties: LinkProperties::measures_mapped(
                        "object_id",
                        "id",
                        "wallclock_lag",
                        mz_repr::SemanticType::GlobalId,
                        "mz_internal.mz_object_global_ids",
                    ),
                },
                OntologyLink {
                    name: "on_replica",
                    target: "replica",
                    properties: LinkProperties::fk_nullable(
                        "replica_id",
                        "id",
                        Cardinality::ManyToOne,
                    ),
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("object_id", SemanticType::GlobalId),
                ("replica_id", SemanticType::ReplicaId),
                ("occurred_at", SemanticType::WallclockTimestamp),
            ]
        },
    }),
});

pub static MZ_WALLCLOCK_GLOBAL_LAG_HISTORY: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_wallclock_global_lag_history",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_WALLCLOCK_GLOBAL_LAG_HISTORY_OID,
    desc: RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("lag", SqlScalarType::Interval.nullable(true))
        .with_column(
            "occurred_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_key(vec![0, 2])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "object_id",
            "The ID of the table, source, materialized view, index, or sink. Corresponds to `mz_objects.id`.",
        ),
        (
            "lag",
            "The minimum wallclock lag observed for the object during the minute.",
        ),
        (
            "occurred_at",
            "The minute-aligned timestamp of the observation.",
        ),
    ]),
    sql: "
WITH times_binned AS (
    SELECT
        object_id,
        lag,
        date_trunc('minute', occurred_at) AS occurred_at
    FROM mz_internal.mz_wallclock_lag_history
)
SELECT
    object_id,
    min(lag) AS lag,
    occurred_at
FROM times_binned
GROUP BY object_id, occurred_at
OPTIONS (AGGREGATE INPUT GROUP SIZE = 1)",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "wallclock_global_lag_event",
        description: "Historical global wallclock lag",
        links: &const {
            [OntologyLink {
                name: "lag_of",
                target: "object_global_id",
                properties: LinkProperties::fk("object_id", "global_id", Cardinality::ManyToOne),
            }]
        },
        column_semantic_types: &const {
            [
                ("object_id", SemanticType::GlobalId),
                ("occurred_at", SemanticType::WallclockTimestamp),
            ]
        },
    }),
});

pub static MZ_WALLCLOCK_GLOBAL_LAG_RECENT_HISTORY: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "mz_wallclock_global_lag_recent_history",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_WALLCLOCK_GLOBAL_LAG_RECENT_HISTORY_OID,
        desc: RelationDesc::builder()
            .with_column("object_id", SqlScalarType::String.nullable(false))
            .with_column("lag", SqlScalarType::Interval.nullable(true))
            .with_column(
                "occurred_at",
                SqlScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_key(vec![0, 2])
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "object_id",
                "The ID of the table, source, materialized view, index, or sink. Corresponds to `mz_objects.id`.",
            ),
            (
                "lag",
                "The minimum wallclock lag observed for the object during the minute.",
            ),
            (
                "occurred_at",
                "The minute-aligned timestamp of the observation.",
            ),
        ]),
        sql: "
SELECT object_id, lag, occurred_at
FROM mz_internal.mz_wallclock_global_lag_history
WHERE occurred_at + '1 day' > mz_now()",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
});

pub static MZ_WALLCLOCK_GLOBAL_LAG: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_wallclock_global_lag",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_WALLCLOCK_GLOBAL_LAG_OID,
    desc: RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("lag", SqlScalarType::Interval.nullable(true))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "object_id",
            "The ID of the table, source, materialized view, index, or sink. Corresponds to `mz_objects.id`.",
        ),
        (
            "lag",
            "The amount of time the object's write frontier lags behind wallclock time.",
        ),
    ]),
    sql: "
SELECT DISTINCT ON (object_id) object_id, lag
FROM mz_internal.mz_wallclock_global_lag_recent_history
WHERE occurred_at + '5 minutes' > mz_now()
ORDER BY object_id, occurred_at DESC",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "wallclock_global_lag",
        description: "Current wallclock lag aggregated across replicas",
        links: &const {
            [OntologyLink {
                name: "measures_global_lag_of",
                target: "object",
                properties: LinkProperties::measures_mapped(
                    "object_id",
                    "id",
                    "wallclock_lag_global",
                    mz_repr::SemanticType::GlobalId,
                    "mz_internal.mz_object_global_ids",
                ),
            }]
        },
        column_semantic_types: &[("object_id", SemanticType::GlobalId)],
    }),
});

pub static MZ_WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RAW: LazyLock<BuiltinSource> =
    LazyLock::new(|| BuiltinSource {
        name: "mz_wallclock_global_lag_histogram_raw",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RAW_OID,
        desc: WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RAW_DESC.clone(),
        column_comments: BTreeMap::new(),
        data_source: IntrospectionType::WallclockLagHistogram.into(),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_WALLCLOCK_GLOBAL_LAG_HISTOGRAM: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_wallclock_global_lag_histogram",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_WALLCLOCK_GLOBAL_LAG_HISTOGRAM_OID,
        desc: RelationDesc::builder()
            .with_column(
                "period_start",
                SqlScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_column(
                "period_end",
                SqlScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_column("object_id", SqlScalarType::String.nullable(false))
            .with_column("lag_seconds", SqlScalarType::UInt64.nullable(true))
            .with_column("labels", SqlScalarType::Jsonb.nullable(false))
            .with_column("count", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1, 2, 3, 4])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT *, count(*) AS count
FROM mz_internal.mz_wallclock_global_lag_histogram_raw
GROUP BY period_start, period_end, object_id, lag_seconds, labels",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_MATERIALIZED_VIEW_REFRESHES: LazyLock<BuiltinSource> = LazyLock::new(|| {
    BuiltinSource {
        name: "mz_materialized_view_refreshes",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_MATERIALIZED_VIEW_REFRESHES_OID,
        data_source: DataSourceDesc::Introspection(
            IntrospectionType::ComputeMaterializedViewRefreshes,
        ),
        desc: RelationDesc::builder()
            .with_column(
                "materialized_view_id",
                SqlScalarType::String.nullable(false),
            )
            .with_column(
                "last_completed_refresh",
                SqlScalarType::MzTimestamp.nullable(true),
            )
            .with_column("next_refresh", SqlScalarType::MzTimestamp.nullable(true))
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "materialized_view_id",
                "The ID of the materialized view. Corresponds to `mz_catalog.mz_materialized_views.id`",
            ),
            (
                "last_completed_refresh",
                "The time of the last successfully completed refresh. `NULL` if the materialized view hasn't completed any refreshes yet.",
            ),
            (
                "next_refresh",
                "The time of the next scheduled refresh. `NULL` if the materialized view has no future scheduled refreshes.",
            ),
        ]),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
});

pub static MZ_SUBSCRIPTIONS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_subscriptions",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_SUBSCRIPTIONS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("session_id", SqlScalarType::Uuid.nullable(false))
        .with_column("cluster_id", SqlScalarType::String.nullable(false))
        .with_column(
            "created_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            "referenced_object_ids",
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::String),
                custom_id: None,
            }
            .nullable(false),
        )
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the subscription."),
        (
            "session_id",
            "The ID of the session that runs the subscription. Corresponds to `mz_sessions.id`.",
        ),
        (
            "cluster_id",
            "The ID of the cluster on which the subscription is running. Corresponds to `mz_clusters.id`.",
        ),
        (
            "created_at",
            "The time at which the subscription was created.",
        ),
        (
            "referenced_object_ids",
            "The IDs of objects referenced by the subscription. Corresponds to `mz_objects.id`",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "subscription",
        description: "Active SUBSCRIBE operations",
        links: &const {
            [
                OntologyLink {
                    name: "uses_session",
                    target: "session",
                    properties: LinkProperties::fk("session_id", "id", Cardinality::ManyToOne),
                },
                OntologyLink {
                    name: "in_active_session",
                    target: "active_session",
                    properties: LinkProperties::fk_nullable(
                        "session_id",
                        "id",
                        Cardinality::ManyToOne,
                    ),
                },
                OntologyLink {
                    name: "belongs_to_cluster",
                    target: "cluster",
                    properties: LinkProperties::fk("cluster_id", "id", Cardinality::ManyToOne),
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("id", SemanticType::CatalogItemId),
                ("cluster_id", SemanticType::ClusterId),
            ]
        },
    }),
});

pub static MZ_SESSIONS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_sessions",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_SESSIONS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::Uuid.nullable(false))
        .with_column("connection_id", SqlScalarType::UInt32.nullable(false))
        .with_column("role_id", SqlScalarType::String.nullable(false))
        .with_column("client_ip", SqlScalarType::String.nullable(true))
        .with_column(
            "connected_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The globally unique ID of the session."),
        (
            "connection_id",
            "The connection ID of the session. Unique only for active sessions and can be recycled. Corresponds to `pg_backend_pid()`.",
        ),
        (
            "role_id",
            "The role ID of the role that the session is logged in as. Corresponds to `mz_catalog.mz_roles`.",
        ),
        (
            "client_ip",
            "The IP address of the client that initiated the session.",
        ),
        (
            "connected_at",
            "The time at which the session connected to the system.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "active_session",
        description: "Currently active sessions",
        links: &const {
            [OntologyLink {
                name: "logged_in_as",
                target: "role",
                properties: LinkProperties::fk("role_id", "id", Cardinality::ManyToOne),
            }]
        },
        column_semantic_types: &[("role_id", SemanticType::RoleId)],
    }),
});

pub static MZ_COMMENTS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_comments",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_COMMENTS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("object_type", SqlScalarType::String.nullable(false))
        .with_column("object_sub_id", SqlScalarType::Int32.nullable(true))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the object. Corresponds to `mz_objects.id`.",
        ),
        (
            "object_type",
            "The type of object the comment is associated with.",
        ),
        (
            "object_sub_id",
            "For a comment on a column of a relation, the column number. `NULL` for other object types.",
        ),
        ("comment", "The comment itself."),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "comment",
        description: "A COMMENT ON annotation for a catalog object or column",
        links: &const {
            [OntologyLink {
                name: "comment_on",
                target: "object",
                properties: LinkProperties::fk_typed(
                    "id",
                    "id",
                    Cardinality::ManyToOne,
                    mz_repr::SemanticType::CatalogItemId,
                ),
            }]
        },
        column_semantic_types: &const {
            [
                ("id", SemanticType::CatalogItemId),
                ("object_type", SemanticType::ObjectType),
            ]
        },
    }),
});

pub static MZ_SOURCE_REFERENCES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_source_references",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_SOURCE_REFERENCES_OID,
    desc: RelationDesc::builder()
        .with_column("source_id", SqlScalarType::String.nullable(false))
        .with_column("namespace", SqlScalarType::String.nullable(true))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column(
            "updated_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            "columns",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(true),
        )
        .finish(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "source_reference",
        description: "External references tracked by sources",
        links: &const {
            [OntologyLink {
                name: "references_source",
                target: "source",
                properties: LinkProperties::fk("source_id", "id", Cardinality::ManyToOne),
            }]
        },
        column_semantic_types: &[("source_id", SemanticType::CatalogItemId)],
    }),
});

pub static MZ_WEBHOOKS_SOURCES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_webhook_sources",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_WEBHOOK_SOURCES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("url", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the webhook source. Corresponds to `mz_sources.id`.",
        ),
        ("name", "The name of the webhook source."),
        (
            "url",
            "The URL which can be used to send events to the source.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "webhook_source",
        description: "Webhook source configuration",
        links: &const {
            [OntologyLink {
                name: "details_of",
                target: "source",
                properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});

pub static MZ_HISTORY_RETENTION_STRATEGIES: LazyLock<BuiltinTable> = LazyLock::new(|| {
    BuiltinTable {
        name: "mz_history_retention_strategies",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::TABLE_MZ_HISTORY_RETENTION_STRATEGIES_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("strategy", SqlScalarType::String.nullable(false))
            .with_column("value", SqlScalarType::Jsonb.nullable(false))
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "The ID of the object."),
            (
                "strategy",
                "The strategy. `FOR` is the only strategy, and means the object's compaction window is the duration of the `value` field.",
            ),
            (
                "value",
                "The value of the strategy. For `FOR`, is a number of milliseconds.",
            ),
        ]),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "history_retention",
            description: "History retention strategy for an object",
            links: &const { [] },
            column_semantic_types: &[("id", SemanticType::CatalogItemId)],
        }),
    }
});

pub static MZ_LICENSE_KEYS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_license_keys",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_LICENSE_KEYS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("organization", SqlScalarType::String.nullable(false))
        .with_column("environment_id", SqlScalarType::String.nullable(false))
        .with_column(
            "expiration",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            "not_before",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The identifier of the license key."),
        (
            "organization",
            "The name of the organization that this license key was issued to.",
        ),
        (
            "environment_id",
            "The environment ID that this license key was issued for.",
        ),
        (
            "expiration",
            "The date and time when this license key expires.",
        ),
        (
            "not_before",
            "The start of the validity period for this license key.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "license_key",
        description: "License key metadata",
        links: &const { [] },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});

pub static MZ_REPLACEMENTS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_replacements",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_REPLACEMENTS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("target_id", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the replacement object. Corresponds to `mz_objects.id`.",
        ),
        (
            "target_id",
            "The ID of the replacement target. Corresponds to `mz_objects.id`.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "replacement",
        description: "A record of an object replacement (ALTER ... SWAP)",
        links: &const {
            [
                OntologyLink {
                    name: "replacement_object",
                    target: "object",
                    properties: LinkProperties::fk("id", "id", Cardinality::ManyToOne),
                },
                OntologyLink {
                    name: "replacement_target",
                    target: "object",
                    properties: LinkProperties::fk("target_id", "id", Cardinality::ManyToOne),
                },
            ]
        },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});

// These will be replaced with per-replica tables once source/sink multiplexing on
// a single cluster is supported.
pub static MZ_SOURCE_STATISTICS_RAW: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_source_statistics_raw",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_SOURCE_STATISTICS_RAW_OID,
    data_source: IntrospectionType::StorageSourceStatistics.into(),
    desc: MZ_SOURCE_STATISTICS_RAW_DESC.clone(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});
pub static MZ_SINK_STATISTICS_RAW: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_sink_statistics_raw",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_SINK_STATISTICS_RAW_OID,
    data_source: IntrospectionType::StorageSinkStatistics.into(),
    desc: MZ_SINK_STATISTICS_RAW_DESC.clone(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_STORAGE_SHARDS: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_storage_shards",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_STORAGE_SHARDS_OID,
    data_source: IntrospectionType::ShardMapping.into(),
    desc: RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("shard_id", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "storage_shard",
        description: "Persist shards used by storage objects",
        links: &const {
            [OntologyLink {
                name: "shard_of",
                target: "object",
                properties: LinkProperties::fk_mapped(
                    "object_id",
                    "id",
                    Cardinality::ManyToOne,
                    mz_repr::SemanticType::GlobalId,
                    "mz_internal.mz_object_global_ids",
                ),
            }]
        },
        column_semantic_types: &const {
            [
                ("object_id", SemanticType::GlobalId),
                ("shard_id", SemanticType::ShardId),
            ]
        },
    }),
});

pub static MZ_OBJECTS_ID_NAMESPACE_TYPES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_objects_id_namespace_types",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_OBJECTS_ID_NAMESPACE_TYPES_OID,
    desc: RelationDesc::builder()
        .with_column("object_type", SqlScalarType::String.nullable(false))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: r#"SELECT *
    FROM (
        VALUES
            ('table'),
            ('view'),
            ('materialized-view'),
            ('source'),
            ('sink'),
            ('index'),
            ('connection'),
            ('type'),
            ('function'),
            ('secret')
    )
    AS _ (object_type)"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_OBJECT_OID_ALIAS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_object_oid_alias",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_OBJECT_OID_ALIAS_OID,
    desc: RelationDesc::builder()
        .with_column("object_type", SqlScalarType::String.nullable(false))
        .with_column("oid_alias", SqlScalarType::String.nullable(false))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT object_type, oid_alias
    FROM (
        VALUES
            (
                'table'::pg_catalog.text,
                'regclass'::pg_catalog.text
            ),
            ('source', 'regclass'),
            ('view', 'regclass'),
            ('materialized-view', 'regclass'),
            ('index', 'regclass'),
            ('type', 'regtype'),
            ('function', 'regproc')
    )
    AS _ (object_type, oid_alias);",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_OBJECT_FULLY_QUALIFIED_NAMES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_object_fully_qualified_names",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_OBJECT_FULLY_QUALIFIED_NAMES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("object_type", SqlScalarType::String.nullable(false))
        .with_column("schema_id", SqlScalarType::String.nullable(false))
        .with_column("schema_name", SqlScalarType::String.nullable(false))
        .with_column("database_id", SqlScalarType::String.nullable(true))
        .with_column("database_name", SqlScalarType::String.nullable(true))
        .with_column("cluster_id", SqlScalarType::String.nullable(true))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "Materialize's unique ID for the object."),
        ("name", "The name of the object."),
        (
            "object_type",
            "The type of the object: one of `table`, `source`, `view`, `materialized-view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`.",
        ),
        (
            "schema_id",
            "The ID of the schema to which the object belongs. Corresponds to `mz_schemas.id`.",
        ),
        (
            "schema_name",
            "The name of the schema to which the object belongs. Corresponds to `mz_schemas.name`.",
        ),
        (
            "database_id",
            "The ID of the database to which the object belongs. Corresponds to `mz_databases.id`.",
        ),
        (
            "database_name",
            "The name of the database to which the object belongs. Corresponds to `mz_databases.name`.",
        ),
        (
            "cluster_id",
            "The ID of the cluster maintaining the source, materialized view, index, or sink. Corresponds to `mz_clusters.id`. `NULL` for other object types.",
        ),
    ]),
    sql: "
    SELECT o.id,
        o.name,
        o.type as object_type,
        sc.id as schema_id,
        sc.name as schema_name,
        db.id as database_id,
        db.name as database_name,
        o.cluster_id
    FROM mz_catalog.mz_objects o
    INNER JOIN mz_catalog.mz_schemas sc ON sc.id = o.schema_id
    -- LEFT JOIN accounts for objects in the ambient database.
    LEFT JOIN mz_catalog.mz_databases db ON db.id = sc.database_id",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "object_fqn",
        description: "Fully qualified name (database.schema.name) for objects",
        links: &const {
            [
                OntologyLink {
                    name: "details_of",
                    target: "object",
                    properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
                },
                OntologyLink {
                    name: "in_schema",
                    target: "schema",
                    properties: LinkProperties::fk("schema_id", "id", Cardinality::ManyToOne),
                },
                OntologyLink {
                    name: "in_database",
                    target: "database",
                    properties: LinkProperties::fk("database_id", "id", Cardinality::ManyToOne),
                },
                OntologyLink {
                    name: "belongs_to_cluster",
                    target: "cluster",
                    properties: LinkProperties::fk("cluster_id", "id", Cardinality::ManyToOne),
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("id", SemanticType::CatalogItemId),
                ("object_type", SemanticType::ObjectType),
                ("schema_id", SemanticType::SchemaId),
                ("database_id", SemanticType::DatabaseId),
                ("cluster_id", SemanticType::ClusterId),
            ]
        },
    }),
});

pub static MZ_OBJECT_GLOBAL_IDS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_object_global_ids",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_OBJECT_GLOBAL_IDS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("global_id", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the object. Corresponds to `mz_objects.id`.",
        ),
        ("global_id", "The global ID of the object."),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "object_global_id",
        description: "Mapping between CatalogItemId (SQL layer) and GlobalId (runtime layer)",
        links: &const {
            [OntologyLink {
                name: "id_references",
                target: "object",
                properties: LinkProperties::fk("id", "id", Cardinality::ManyToOne),
            }]
        },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});

// TODO (SangJunBak): Remove once mz_object_history is released and used in the Console https://github.com/MaterializeInc/console/issues/3342
pub static MZ_OBJECT_LIFETIMES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_object_lifetimes",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_OBJECT_LIFETIMES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(true))
        .with_column("previous_id", SqlScalarType::String.nullable(true))
        .with_column("object_type", SqlScalarType::String.nullable(false))
        .with_column("event_type", SqlScalarType::String.nullable(false))
        .with_column(
            "occurred_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "Materialize's unique ID for the object."),
        ("previous_id", "The object's previous ID, if one exists."),
        (
            "object_type",
            "The type of the object: one of `table`, `source`, `view`, `materialized-view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`.",
        ),
        (
            "event_type",
            "The lifetime event, either `create` or `drop`.",
        ),
        (
            "occurred_at",
            "Wall-clock timestamp of when the event occurred.",
        ),
    ]),
    sql: "
    SELECT
        CASE
            WHEN a.object_type = 'cluster-replica' THEN a.details ->> 'replica_id'
            ELSE a.details ->> 'id'
        END id,
        a.details ->> 'previous_id' as previous_id,
        a.object_type,
        a.event_type,
        a.occurred_at
    FROM mz_catalog.mz_audit_events a
    WHERE a.event_type = 'create' OR a.event_type = 'drop'",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "object_lifetime_event",
        description: "Create or drop lifecycle event for a catalog object",
        links: &const {
            [OntologyLink {
                name: "lifetime_event_of",
                target: "object",
                properties: LinkProperties::fk_typed(
                    "id",
                    "id",
                    Cardinality::ManyToOne,
                    mz_repr::SemanticType::CatalogItemId,
                ),
            }]
        },
        column_semantic_types: &const {
            [
                ("id", SemanticType::CatalogItemId),
                ("object_type", SemanticType::ObjectType),
            ]
        },
    }),
});

pub static MZ_OBJECT_HISTORY: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_object_history",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_OBJECT_HISTORY_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(true))
        .with_column("cluster_id", SqlScalarType::String.nullable(true))
        .with_column("object_type", SqlScalarType::String.nullable(false))
        .with_column(
            "created_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(true),
        )
        .with_column(
            "dropped_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(true),
        )
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "Materialize's unique ID for the object."),
        (
            "cluster_id",
            "The object's cluster ID. `NULL` if the object has no associated cluster.",
        ),
        (
            "object_type",
            "The type of the object: one of `table`, `source`, `view`, `materialized-view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`.",
        ),
        (
            "created_at",
            "Wall-clock timestamp of when the object was created. `NULL` for built in system objects.",
        ),
        (
            "dropped_at",
            "Wall-clock timestamp of when the object was dropped. `NULL` for built in system objects or if the object hasn't been dropped.",
        ),
    ]),
    sql: r#"
    WITH
        creates AS
        (
            SELECT
                details ->> 'id' AS id,
                -- We need to backfill cluster_id since older object create events don't include the cluster ID in the audit log
                COALESCE(details ->> 'cluster_id', objects.cluster_id) AS cluster_id,
                object_type,
                occurred_at
            FROM
                mz_catalog.mz_audit_events AS events
                    LEFT JOIN mz_catalog.mz_objects AS objects ON details ->> 'id' = objects.id
            WHERE event_type = 'create' AND object_type IN ( SELECT object_type FROM mz_internal.mz_objects_id_namespace_types )
        ),
        drops AS
        (
            SELECT details ->> 'id' AS id, occurred_at
            FROM mz_catalog.mz_audit_events
            WHERE event_type = 'drop' AND object_type IN ( SELECT object_type FROM mz_internal.mz_objects_id_namespace_types )
        ),
        user_object_history AS
        (
            SELECT
                creates.id,
                creates.cluster_id,
                creates.object_type,
                creates.occurred_at AS created_at,
                drops.occurred_at AS dropped_at
            FROM creates LEFT JOIN drops ON creates.id = drops.id
            WHERE creates.id LIKE 'u%'
        ),
        -- We need to union built in objects since they aren't in the audit log
        built_in_objects AS
        (
            -- Functions that accept different arguments have different oids but the same id. We deduplicate in this case.
            SELECT DISTINCT ON (objects.id)
                objects.id,
                objects.cluster_id,
                objects.type AS object_type,
                NULL::timestamptz AS created_at,
                NULL::timestamptz AS dropped_at
            FROM mz_catalog.mz_objects AS objects
            WHERE objects.id LIKE 's%'
        )
    SELECT * FROM user_object_history UNION ALL (SELECT * FROM built_in_objects)"#,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "object_history",
        description: "Historical record of object creation and drops",
        links: &const {
            [OntologyLink {
                name: "history_of",
                target: "object",
                properties: LinkProperties::fk("id", "id", Cardinality::ManyToOne),
            }]
        },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});

pub static MZ_OBJECT_TRANSITIVE_DEPENDENCIES: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "mz_object_transitive_dependencies",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_OBJECT_TRANSITIVE_DEPENDENCIES_OID,
        desc: RelationDesc::builder()
            .with_column("object_id", SqlScalarType::String.nullable(false))
            .with_column(
                "referenced_object_id",
                SqlScalarType::String.nullable(false),
            )
            .with_key(vec![0, 1])
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "object_id",
                "The ID of the dependent object. Corresponds to `mz_objects.id`.",
            ),
            (
                "referenced_object_id",
                "The ID of the (possibly transitively) referenced object. Corresponds to `mz_objects.id`.",
            ),
        ]),
        sql: "
WITH MUTUALLY RECURSIVE
  reach(object_id text, referenced_object_id text) AS (
    SELECT object_id, referenced_object_id FROM mz_internal.mz_object_dependencies
    UNION
    SELECT x, z FROM reach r1(x, y) JOIN reach r2(y, z) USING(y)
  )
SELECT object_id, referenced_object_id FROM reach;",
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "transitive_dependency",
            description: "Transitive closure of object dependencies — all direct and indirect dependencies",
            links: &const {
                [
                    OntologyLink {
                        name: "depends_on",
                        target: "object",
                        properties: LinkProperties::DependsOn {
                            source_column: "object_id",
                            target_column: "id",
                            source_id_type: Some(mz_repr::SemanticType::CatalogItemId),
                            requires_mapping: None,
                        },
                    },
                    OntologyLink {
                        name: "dependency_is",
                        target: "object",
                        properties: LinkProperties::DependsOn {
                            source_column: "referenced_object_id",
                            target_column: "id",
                            source_id_type: Some(mz_repr::SemanticType::CatalogItemId),
                            requires_mapping: None,
                        },
                    },
                ]
            },
            column_semantic_types: &const {
                [
                    ("object_id", SemanticType::CatalogItemId),
                    ("referenced_object_id", SemanticType::CatalogItemId),
                ]
            },
        }),
    }
});

/// Peeled version of `PG_NAMESPACE`:
/// - This doesn't check `mz_schemas.database_id IS NULL OR d.name = pg_catalog.current_database()`,
///   in order to make this view indexable.
/// - This has the database name as an extra column, so that downstream views can check it against
///  `current_database()`.
pub static PG_NAMESPACE_ALL_DATABASES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_namespace_all_databases",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_PG_NAMESPACE_ALL_DATABASES_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("nspname", SqlScalarType::String.nullable(false))
        .with_column("nspowner", SqlScalarType::Oid.nullable(false))
        .with_column(
            "nspacl",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(true),
        )
        .with_column("database_name", SqlScalarType::String.nullable(true))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT
    s.oid AS oid,
    s.name AS nspname,
    role_owner.oid AS nspowner,
    NULL::pg_catalog.text[] AS nspacl,
    d.name as database_name
FROM mz_catalog.mz_schemas s
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
JOIN mz_catalog.mz_roles role_owner ON role_owner.id = s.owner_id",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub const PG_NAMESPACE_ALL_DATABASES_IND: BuiltinIndex = BuiltinIndex {
    name: "pg_namespace_all_databases_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_PG_NAMESPACE_ALL_DATABASES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.pg_namespace_all_databases (nspname)",
    is_retained_metrics_object: false,
};

/// Peeled version of `PG_CLASS`:
/// - This doesn't check `mz_schemas.database_id IS NULL OR d.name = pg_catalog.current_database()`,
///   in order to make this view indexable.
/// - This has the database name as an extra column, so that downstream views can check it against
///  `current_database()`.
pub static PG_CLASS_ALL_DATABASES: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "pg_class_all_databases",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_PG_CLASS_ALL_DATABASES_OID,
        desc: RelationDesc::builder()
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("relname", SqlScalarType::String.nullable(false))
            .with_column("relnamespace", SqlScalarType::Oid.nullable(false))
            .with_column("reloftype", SqlScalarType::Oid.nullable(false))
            .with_column("relowner", SqlScalarType::Oid.nullable(false))
            .with_column("relam", SqlScalarType::Oid.nullable(false))
            .with_column("reltablespace", SqlScalarType::Oid.nullable(false))
            .with_column("reltuples", SqlScalarType::Float32.nullable(false))
            .with_column("reltoastrelid", SqlScalarType::Oid.nullable(false))
            .with_column("relhasindex", SqlScalarType::Bool.nullable(false))
            .with_column("relpersistence", SqlScalarType::PgLegacyChar.nullable(false))
            .with_column("relkind", SqlScalarType::String.nullable(true))
            .with_column("relnatts", SqlScalarType::Int16.nullable(false))
            .with_column("relchecks", SqlScalarType::Int16.nullable(false))
            .with_column("relhasrules", SqlScalarType::Bool.nullable(false))
            .with_column("relhastriggers", SqlScalarType::Bool.nullable(false))
            .with_column("relhassubclass", SqlScalarType::Bool.nullable(false))
            .with_column("relrowsecurity", SqlScalarType::Bool.nullable(false))
            .with_column("relforcerowsecurity", SqlScalarType::Bool.nullable(false))
            .with_column("relreplident", SqlScalarType::PgLegacyChar.nullable(false))
            .with_column("relispartition", SqlScalarType::Bool.nullable(false))
            .with_column("relhasoids", SqlScalarType::Bool.nullable(false))
            .with_column("reloptions", SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(true))
            .with_column("database_name", SqlScalarType::String.nullable(true))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    class_objects.oid,
    class_objects.name AS relname,
    mz_schemas.oid AS relnamespace,
    -- MZ doesn't support typed tables so reloftype is filled with 0
    0::pg_catalog.oid AS reloftype,
    role_owner.oid AS relowner,
    0::pg_catalog.oid AS relam,
    -- MZ doesn't have tablespaces so reltablespace is filled in with 0 implying the default tablespace
    0::pg_catalog.oid AS reltablespace,
    -- MZ doesn't support (estimated) row counts currently.
    -- Postgres defines a value of -1 as unknown.
    -1::float4 as reltuples,
    -- MZ doesn't use TOAST tables so reltoastrelid is filled with 0
    0::pg_catalog.oid AS reltoastrelid,
    EXISTS (SELECT id, oid, name, on_id, cluster_id FROM mz_catalog.mz_indexes where mz_indexes.on_id = class_objects.id) AS relhasindex,
    -- MZ doesn't have unlogged tables and because of (https://github.com/MaterializeInc/database-issues/issues/2689)
    -- temporary objects don't show up here, so relpersistence is filled with 'p' for permanent.
    -- TODO(jkosh44): update this column when issue is resolved.
    'p'::pg_catalog.\"char\" AS relpersistence,
    CASE
        WHEN class_objects.type = 'table' THEN 'r'
        WHEN class_objects.type = 'source' THEN 'r'
        WHEN class_objects.type = 'index' THEN 'i'
        WHEN class_objects.type = 'view' THEN 'v'
        WHEN class_objects.type = 'materialized-view' THEN 'm'
    END relkind,
    CASE
        WHEN class_objects.type = 'index' THEN COALESCE(
            (
                SELECT count(*)::pg_catalog.int2
                FROM mz_catalog.mz_index_columns
                WHERE mz_index_columns.index_id = class_objects.id
            ),
            0::pg_catalog.int2
        )
        ELSE COALESCE(
            (
                SELECT count(*)::pg_catalog.int2
                FROM mz_catalog.mz_columns
                WHERE mz_columns.id = class_objects.id
            ),
            0::pg_catalog.int2
        )
    END AS relnatts,
    -- MZ doesn't support CHECK constraints so relchecks is filled with 0
    0::pg_catalog.int2 AS relchecks,
    -- MZ doesn't support creating rules so relhasrules is filled with false
    false AS relhasrules,
    -- MZ doesn't support creating triggers so relhastriggers is filled with false
    false AS relhastriggers,
    -- MZ doesn't support table inheritance or partitions so relhassubclass is filled with false
    false AS relhassubclass,
    -- MZ doesn't have row level security so relrowsecurity and relforcerowsecurity is filled with false
    false AS relrowsecurity,
    false AS relforcerowsecurity,
    -- MZ doesn't support replication so relreplident is filled with 'd' for default
    'd'::pg_catalog.\"char\" AS relreplident,
    -- MZ doesn't support table partitioning so relispartition is filled with false
    false AS relispartition,
    -- PG removed relhasoids in v12 so it's filled with false
    false AS relhasoids,
    -- MZ doesn't support options for relations
    NULL::pg_catalog.text[] as reloptions,
    d.name as database_name
FROM (
    -- pg_class catalogs relations and indexes
    SELECT id, oid, schema_id, name, type, owner_id FROM mz_catalog.mz_relations
    UNION ALL
        SELECT mz_indexes.id, mz_indexes.oid, mz_relations.schema_id, mz_indexes.name, 'index' AS type, mz_indexes.owner_id
        FROM mz_catalog.mz_indexes
        JOIN mz_catalog.mz_relations ON mz_indexes.on_id = mz_relations.id
) AS class_objects
JOIN mz_catalog.mz_schemas ON mz_schemas.id = class_objects.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = mz_schemas.database_id
JOIN mz_catalog.mz_roles role_owner ON role_owner.id = class_objects.owner_id",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
});

pub const PG_CLASS_ALL_DATABASES_IND: BuiltinIndex = BuiltinIndex {
    name: "pg_class_all_databases_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_PG_CLASS_ALL_DATABASES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.pg_class_all_databases (relname)",
    is_retained_metrics_object: false,
};

/// Peeled version of `PG_DESCRIPTION`:
/// - This doesn't check `mz_schemas.database_id IS NULL OR d.name = pg_catalog.current_database()`,
///   in order to make this view indexable.
/// - This has 2 extra columns for the database names, so that downstream views can check them
///   against `current_database()`.
pub static PG_DESCRIPTION_ALL_DATABASES: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "pg_description_all_databases",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_PG_DESCRIPTION_ALL_DATABASES_OID,
        desc: RelationDesc::builder()
            .with_column("objoid", SqlScalarType::Oid.nullable(false))
            .with_column("classoid", SqlScalarType::Oid.nullable(true))
            .with_column("objsubid", SqlScalarType::Int32.nullable(false))
            .with_column("description", SqlScalarType::String.nullable(false))
            .with_column("oid_database_name", SqlScalarType::String.nullable(true))
            .with_column("class_database_name", SqlScalarType::String.nullable(true))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
(
    -- Gather all of the class oid's for objects that can have comments.
    WITH pg_classoids AS (
        SELECT oid, database_name as oid_database_name,
          (SELECT oid FROM mz_internal.pg_class_all_databases WHERE relname = 'pg_class') AS classoid,
          (SELECT database_name FROM mz_internal.pg_class_all_databases WHERE relname = 'pg_class') AS class_database_name
        FROM mz_internal.pg_class_all_databases
        UNION ALL
        SELECT oid, database_name as oid_database_name,
          (SELECT oid FROM mz_internal.pg_class_all_databases WHERE relname = 'pg_type') AS classoid,
          (SELECT database_name FROM mz_internal.pg_class_all_databases WHERE relname = 'pg_type') AS class_database_name
        FROM mz_internal.pg_type_all_databases
        UNION ALL
        SELECT oid, database_name as oid_database_name,
          (SELECT oid FROM mz_internal.pg_class_all_databases WHERE relname = 'pg_namespace') AS classoid,
          (SELECT database_name FROM mz_internal.pg_class_all_databases WHERE relname = 'pg_namespace') AS class_database_name
        FROM mz_internal.pg_namespace_all_databases
    ),

    -- Gather all of the MZ ids for objects that can have comments.
    mz_objects AS (
        SELECT id, oid, type FROM mz_catalog.mz_objects
        UNION ALL
        SELECT id, oid, 'schema' AS type FROM mz_catalog.mz_schemas
    )
    SELECT
        pg_classoids.oid AS objoid,
        pg_classoids.classoid as classoid,
        COALESCE(cmt.object_sub_id, 0) AS objsubid,
        cmt.comment AS description,
        -- Columns added because of the peeling. (Note that there are 2 of these here.)
        oid_database_name,
        class_database_name
    FROM
        pg_classoids
    JOIN
        mz_objects ON pg_classoids.oid = mz_objects.oid
    JOIN
        mz_internal.mz_comments AS cmt ON mz_objects.id = cmt.id AND lower(mz_objects.type) = lower(cmt.object_type)
)",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
});

pub const PG_DESCRIPTION_ALL_DATABASES_IND: BuiltinIndex = BuiltinIndex {
    name: "pg_description_all_databases_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_PG_DESCRIPTION_ALL_DATABASES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.pg_description_all_databases (objoid, classoid, objsubid, description, oid_database_name, class_database_name)",
    is_retained_metrics_object: false,
};

/// Peeled version of `PG_TYPE`:
/// - This doesn't check `mz_schemas.database_id IS NULL OR d.name = pg_catalog.current_database()`,
///   in order to make this view indexable.
/// - This has the database name as an extra column, so that downstream views can check it against
///  `current_database()`.
pub static PG_TYPE_ALL_DATABASES: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "pg_type_all_databases",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_PG_TYPE_ALL_DATABASES_OID,
        desc: RelationDesc::builder()
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("typname", SqlScalarType::String.nullable(false))
            .with_column("typnamespace", SqlScalarType::Oid.nullable(false))
            .with_column("typowner", SqlScalarType::Oid.nullable(false))
            .with_column("typlen", SqlScalarType::Int16.nullable(true))
            .with_column("typtype", SqlScalarType::PgLegacyChar.nullable(false))
            .with_column("typcategory", SqlScalarType::PgLegacyChar.nullable(true))
            .with_column("typdelim", SqlScalarType::PgLegacyChar.nullable(false))
            .with_column("typrelid", SqlScalarType::Oid.nullable(false))
            .with_column("typelem", SqlScalarType::Oid.nullable(false))
            .with_column("typarray", SqlScalarType::Oid.nullable(false))
            .with_column("typinput", SqlScalarType::RegProc.nullable(true))
            .with_column("typreceive", SqlScalarType::Oid.nullable(false))
            .with_column("typnotnull", SqlScalarType::Bool.nullable(false))
            .with_column("typbasetype", SqlScalarType::Oid.nullable(false))
            .with_column("typtypmod", SqlScalarType::Int32.nullable(false))
            .with_column("typcollation", SqlScalarType::Oid.nullable(false))
            .with_column("typdefault", SqlScalarType::String.nullable(true))
            .with_column("database_name", SqlScalarType::String.nullable(true))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    mz_types.oid,
    mz_types.name AS typname,
    mz_schemas.oid AS typnamespace,
    role_owner.oid AS typowner,
    NULL::pg_catalog.int2 AS typlen,
    -- 'a' is used internally to denote an array type, but in postgres they show up
    -- as 'b'.
    (CASE mztype WHEN 'a' THEN 'b' ELSE mztype END)::pg_catalog.char AS typtype,
    (CASE category
        WHEN 'array' THEN 'A'
        WHEN 'bit-string' THEN 'V'
        WHEN 'boolean' THEN 'B'
        WHEN 'composite' THEN 'C'
        WHEN 'date-time' THEN 'D'
        WHEN 'enum' THEN 'E'
        WHEN 'geometric' THEN 'G'
        WHEN 'list' THEN 'U' -- List types are user-defined from PostgreSQL's perspective.
        WHEN 'network-address' THEN 'I'
        WHEN 'numeric' THEN 'N'
        WHEN 'pseudo' THEN 'P'
        WHEN 'string' THEN 'S'
        WHEN 'timespan' THEN 'T'
        WHEN 'user-defined' THEN 'U'
        WHEN 'unknown' THEN 'X'
    END)::pg_catalog.char AS typcategory,
    -- In pg only the 'box' type is not ','.
    ','::pg_catalog.char AS typdelim,
    0::pg_catalog.oid AS typrelid,
    coalesce(
        (
            SELECT t.oid
            FROM mz_catalog.mz_array_types a
            JOIN mz_catalog.mz_types t ON a.element_id = t.id
            WHERE a.id = mz_types.id
        ),
        (
            SELECT t.oid
            FROM mz_catalog.mz_list_types l
            JOIN mz_catalog.mz_types t ON l.element_id = t.id
            WHERE l.id = mz_types.id
        ),
        0
    ) AS typelem,
    coalesce(
        (
            SELECT
                t.oid
            FROM
                mz_catalog.mz_array_types AS a
                JOIN mz_catalog.mz_types AS t ON a.id = t.id
            WHERE
                a.element_id = mz_types.id
        ),
        0
    )
        AS typarray,
    mz_internal.mz_type_pg_metadata.typinput::pg_catalog.regproc AS typinput,
    COALESCE(mz_internal.mz_type_pg_metadata.typreceive, 0) AS typreceive,
    false::pg_catalog.bool AS typnotnull,
    0::pg_catalog.oid AS typbasetype,
    -1::pg_catalog.int4 AS typtypmod,
    -- MZ doesn't support COLLATE so typcollation is filled with 0
    0::pg_catalog.oid AS typcollation,
    NULL::pg_catalog.text AS typdefault,
    d.name as database_name
FROM
    mz_catalog.mz_types
    LEFT JOIN mz_internal.mz_type_pg_metadata ON mz_catalog.mz_types.id = mz_internal.mz_type_pg_metadata.id
    JOIN mz_catalog.mz_schemas ON mz_schemas.id = mz_types.schema_id
    JOIN (
            -- 'a' is not a supported typtype, but we use it to denote an array. It is
            -- converted to the correct value above.
            SELECT id, 'a' AS mztype FROM mz_catalog.mz_array_types
            UNION ALL SELECT id, 'b' FROM mz_catalog.mz_base_types
            UNION ALL SELECT id, 'l' FROM mz_catalog.mz_list_types
            UNION ALL SELECT id, 'm' FROM mz_catalog.mz_map_types
            UNION ALL SELECT id, 'p' FROM mz_catalog.mz_pseudo_types
        )
            AS t ON mz_types.id = t.id
    LEFT JOIN mz_catalog.mz_databases d ON d.id = mz_schemas.database_id
    JOIN mz_catalog.mz_roles role_owner ON role_owner.id = mz_types.owner_id",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
});

pub const PG_TYPE_ALL_DATABASES_IND: BuiltinIndex = BuiltinIndex {
    name: "pg_type_all_databases_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_PG_TYPE_ALL_DATABASES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.pg_type_all_databases (oid)",
    is_retained_metrics_object: false,
};

/// Peeled version of `PG_ATTRIBUTE`:
/// - This doesn't check `mz_schemas.database_id IS NULL OR d.name = pg_catalog.current_database()`,
///   in order to make this view indexable.
/// - This has 2 extra columns for the database names, so that downstream views can check them
///   against `current_database()`.
pub static PG_ATTRIBUTE_ALL_DATABASES: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "pg_attribute_all_databases",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_PG_ATTRIBUTE_ALL_DATABASES_OID,
        desc: RelationDesc::builder()
            .with_column("attrelid", SqlScalarType::Oid.nullable(false))
            .with_column("attname", SqlScalarType::String.nullable(false))
            .with_column("atttypid", SqlScalarType::Oid.nullable(false))
            .with_column("attlen", SqlScalarType::Int16.nullable(true))
            .with_column("attnum", SqlScalarType::Int16.nullable(false))
            .with_column("atttypmod", SqlScalarType::Int32.nullable(false))
            .with_column("attndims", SqlScalarType::Int16.nullable(false))
            .with_column("attnotnull", SqlScalarType::Bool.nullable(false))
            .with_column("atthasdef", SqlScalarType::Bool.nullable(false))
            .with_column("attidentity", SqlScalarType::PgLegacyChar.nullable(false))
            .with_column("attgenerated", SqlScalarType::PgLegacyChar.nullable(false))
            .with_column("attisdropped", SqlScalarType::Bool.nullable(false))
            .with_column("attcollation", SqlScalarType::Oid.nullable(false))
            .with_column("database_name", SqlScalarType::String.nullable(true))
            .with_column("pg_type_database_name", SqlScalarType::String.nullable(true))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    class_objects.oid as attrelid,
    mz_columns.name as attname,
    mz_columns.type_oid AS atttypid,
    pg_type_all_databases.typlen AS attlen,
    position::int8::int2 as attnum,
    mz_columns.type_mod as atttypmod,
    -- dummy value, just to make go-jet's workaround work for now. Discussion:
    -- https://github.com/MaterializeInc/materialize/pull/34649#issuecomment-3714291409
    0::int2 as attndims,
    NOT nullable as attnotnull,
    mz_columns.default IS NOT NULL as atthasdef,
    ''::pg_catalog.\"char\" as attidentity,
    -- MZ doesn't support generated columns so attgenerated is filled with ''
    ''::pg_catalog.\"char\" as attgenerated,
    FALSE as attisdropped,
    -- MZ doesn't support COLLATE so attcollation is filled with 0
    0::pg_catalog.oid as attcollation,
    -- Columns added because of the peeling. (Note that there are 2 of these here.)
    d.name as database_name,
    pg_type_all_databases.database_name as pg_type_database_name
FROM (
    -- pg_attribute catalogs columns on relations and indexes
    SELECT id, oid, schema_id, name, type FROM mz_catalog.mz_relations
    UNION ALL
        SELECT mz_indexes.id, mz_indexes.oid, mz_relations.schema_id, mz_indexes.name, 'index' AS type
        FROM mz_catalog.mz_indexes
        JOIN mz_catalog.mz_relations ON mz_indexes.on_id = mz_relations.id
) AS class_objects
JOIN mz_catalog.mz_columns ON class_objects.id = mz_columns.id
JOIN mz_internal.pg_type_all_databases ON pg_type_all_databases.oid = mz_columns.type_oid
JOIN mz_catalog.mz_schemas ON mz_schemas.id = class_objects.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = mz_schemas.database_id",
        // Since this depends on pg_type, its id must be higher due to initialization
        // ordering.
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
});

pub const PG_ATTRIBUTE_ALL_DATABASES_IND: BuiltinIndex = BuiltinIndex {
    name: "pg_attribute_all_databases_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_PG_ATTRIBUTE_ALL_DATABASES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.pg_attribute_all_databases (
    attrelid, attname, atttypid, attlen, attnum, atttypmod, attnotnull, atthasdef, attidentity,
    attgenerated, attisdropped, attcollation, database_name, pg_type_database_name
)",
    is_retained_metrics_object: false,
};

/// Peeled version of `PG_ATTRDEF`:
/// - This doesn't check `mz_schemas.database_id IS NULL OR d.name = pg_catalog.current_database()`,
///   in order to make this view indexable.
pub static PG_ATTRDEF_ALL_DATABASES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_attrdef_all_databases",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_PG_ATTRDEF_ALL_DATABASES_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(true))
        .with_column("adrelid", SqlScalarType::Oid.nullable(false))
        .with_column("adnum", SqlScalarType::Int64.nullable(false))
        .with_column("adbin", SqlScalarType::String.nullable(false))
        .with_column("adsrc", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT
    NULL::pg_catalog.oid AS oid,
    mz_objects.oid AS adrelid,
    mz_columns.position::int8 AS adnum,
    mz_columns.default AS adbin,
    mz_columns.default AS adsrc
FROM mz_catalog.mz_columns
    JOIN mz_catalog.mz_objects ON mz_columns.id = mz_objects.id
WHERE default IS NOT NULL",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub const PG_ATTRDEF_ALL_DATABASES_IND: BuiltinIndex = BuiltinIndex {
    name: "pg_attrdef_all_databases_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_PG_ATTRDEF_ALL_DATABASES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.pg_attrdef_all_databases (oid, adrelid, adnum, adbin, adsrc)",
    is_retained_metrics_object: false,
};

pub static MZ_COMPUTE_ERROR_COUNTS_RAW_UNIFIED: LazyLock<BuiltinSource> =
    LazyLock::new(|| BuiltinSource {
        // TODO(database-issues#8173): Rename this source to `mz_compute_error_counts_raw`. Currently this causes a
        // naming conflict because the resolver stumbles over the source with the same name in
        // `mz_introspection` due to the automatic schema translation.
        name: "mz_compute_error_counts_raw_unified",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_COMPUTE_ERROR_COUNTS_RAW_UNIFIED_OID,
        desc: RelationDesc::builder()
            .with_column("replica_id", SqlScalarType::String.nullable(false))
            .with_column("object_id", SqlScalarType::String.nullable(false))
            .with_column(
                "count",
                SqlScalarType::Numeric { max_scale: None }.nullable(false),
            )
            .finish(),
        data_source: IntrospectionType::ComputeErrorCounts.into(),
        column_comments: BTreeMap::new(),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_COMPUTE_HYDRATION_TIMES: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_compute_hydration_times",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_COMPUTE_HYDRATION_TIMES_OID,
    desc: RelationDesc::builder()
        .with_column("replica_id", SqlScalarType::String.nullable(false))
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("time_ns", SqlScalarType::UInt64.nullable(true))
        .finish(),
    data_source: IntrospectionType::ComputeHydrationTimes.into(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "compute_hydration_time",
        description: "Time to hydrate compute objects",
        links: &const { [] },
        column_semantic_types: &const {
            [
                ("replica_id", SemanticType::ReplicaId),
                ("object_id", SemanticType::CatalogItemId),
            ]
        },
    }),
});

pub static MZ_COMPUTE_HYDRATION_TIMES_IND: LazyLock<BuiltinIndex> =
    LazyLock::new(|| BuiltinIndex {
        name: "mz_compute_hydration_times_ind",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::INDEX_MZ_COMPUTE_HYDRATION_TIMES_IND_OID,
        sql: "IN CLUSTER mz_catalog_server
    ON mz_internal.mz_compute_hydration_times (replica_id)",
        is_retained_metrics_object: true,
    });

pub static MZ_OBJECT_ARRANGEMENT_SIZES_UNIFIED: LazyLock<BuiltinSource> = LazyLock::new(|| {
    BuiltinSource {
        name: "mz_object_arrangement_sizes",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_OBJECT_ARRANGEMENT_SIZES_OID,
        desc: RelationDesc::builder()
            .with_column("replica_id", SqlScalarType::String.nullable(false))
            .with_column("object_id", SqlScalarType::String.nullable(false))
            .with_column("size", SqlScalarType::Int64.nullable(true))
            .finish(),
        data_source: IntrospectionType::ComputeObjectArrangementSizes.into(),
        column_comments: BTreeMap::from_iter([
            (
                "replica_id",
                "The ID of the cluster replica. Corresponds to `mz_cluster_replicas.id`.",
            ),
            (
                "object_id",
                "The ID of the compute object (index or materialized view). Corresponds to `mz_objects.id`.",
            ),
            (
                "size",
                "The total arrangement heap and batcher size in bytes for this object on this replica. \
                 Objects smaller than 10 MiB are reported at their exact size; objects 10 MiB or larger \
                 are rounded to the nearest 10 MiB boundary to reduce per-byte churn in the differential \
                 collection.",
            ),
        ]),
        is_retained_metrics_object: true,
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
});

pub static MZ_OBJECT_ARRANGEMENT_SIZES_IND: LazyLock<BuiltinIndex> =
    LazyLock::new(|| BuiltinIndex {
        name: "mz_object_arrangement_sizes_ind",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::INDEX_MZ_OBJECT_ARRANGEMENT_SIZES_IND_OID,
        sql: "IN CLUSTER mz_catalog_server
    ON mz_internal.mz_object_arrangement_sizes (replica_id)",
        is_retained_metrics_object: true,
    });

pub static MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY: LazyLock<BuiltinTable> = LazyLock::new(|| {
    BuiltinTable {
        name: "mz_object_arrangement_size_history",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::TABLE_MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY_OID,
        desc: RelationDesc::builder()
            .with_column("replica_id", SqlScalarType::String.nullable(false))
            .with_column("object_id", SqlScalarType::String.nullable(false))
            .with_column("size", SqlScalarType::Int64.nullable(false))
            .with_column(
                "collection_timestamp",
                SqlScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_column("hydration_complete", SqlScalarType::Bool.nullable(false))
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "replica_id",
                "The ID of the cluster replica. Corresponds to `mz_cluster_replicas.id`.",
            ),
            (
                "object_id",
                "The ID of the compute object (index or materialized view). Corresponds to `mz_objects.id`.",
            ),
            (
                "size",
                "The total arrangement heap and batcher size in bytes for this object on this replica \
                 at `collection_timestamp`. Objects below 10 MiB are dropped from the snapshot; \
                 objects at or above the floor are rounded to the nearest 10 MiB to reduce \
                 per-byte churn in the underlying differential collection. May reflect a mid-build \
                 size if `hydration_complete` is `false`.",
            ),
            (
                "collection_timestamp",
                "The timestamp when this snapshot was collected.",
            ),
            (
                "hydration_complete",
                "Whether the arrangement had finished its initial hydration on this replica when \
                 the snapshot was collected. Filter for `true` to consider only stable, post-build \
                 sizes.",
            ),
        ]),
        is_retained_metrics_object: true,
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
});

pub static MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY_OBJECT_IND: LazyLock<BuiltinIndex> =
    LazyLock::new(|| BuiltinIndex {
        name: "mz_object_arrangement_size_history_object_ind",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::INDEX_MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY_OBJECT_IND_OID,
        sql: "IN CLUSTER mz_catalog_server
    ON mz_internal.mz_object_arrangement_size_history (object_id)",
        is_retained_metrics_object: true,
    });

pub static MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY_TS_IND: LazyLock<BuiltinIndex> =
    LazyLock::new(|| BuiltinIndex {
        name: "mz_object_arrangement_size_history_ts_ind",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::INDEX_MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY_TS_IND_OID,
        sql: "IN CLUSTER mz_catalog_server
    ON mz_internal.mz_object_arrangement_size_history (collection_timestamp)",
        is_retained_metrics_object: true,
    });

pub static MZ_COMPUTE_HYDRATION_STATUSES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_compute_hydration_statuses",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_COMPUTE_HYDRATION_STATUSES_OID,
    desc: RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("replica_id", SqlScalarType::String.nullable(false))
        .with_column("hydrated", SqlScalarType::Bool.nullable(false))
        .with_column("hydration_time", SqlScalarType::Interval.nullable(true))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "object_id",
            "The ID of a compute object. Corresponds to `mz_catalog.mz_indexes.id` or `mz_catalog.mz_materialized_views.id`",
        ),
        ("replica_id", "The ID of a cluster replica."),
        (
            "hydrated",
            "Whether the compute object is hydrated on the replica.",
        ),
        (
            "hydration_time",
            "The amount of time it took for the replica to hydrate the compute object.",
        ),
    ]),
    sql: "
WITH
    dataflows AS (
        SELECT
            object_id,
            replica_id,
            time_ns IS NOT NULL AS hydrated,
            ((time_ns / 1000) || 'microseconds')::interval AS hydration_time
        FROM mz_internal.mz_compute_hydration_times
    ),
    -- MVs that have advanced to the empty frontier don't have a dataflow installed anymore and
    -- therefore don't show up in `mz_compute_hydration_times`. We still want to show them here to
    -- avoid surprises for people joining `mz_materialized_views` against this relation (like the
    -- blue-green readiness query does), so we include them as 'hydrated'.
    complete_mvs AS (
        SELECT
            mv.id,
            f.replica_id,
            true AS hydrated,
            NULL::interval AS hydration_time
        FROM mz_materialized_views mv
        JOIN mz_catalog.mz_cluster_replica_frontiers f ON f.object_id = mv.id
        WHERE f.write_frontier IS NULL
    )
SELECT * FROM dataflows
UNION ALL
SELECT * FROM complete_mvs",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "compute_hydration_status_view",
        description: "Computed hydration status per compute object",
        links: &const { [] },
        column_semantic_types: &const {
            [
                ("object_id", SemanticType::GlobalId),
                ("replica_id", SemanticType::ReplicaId),
            ]
        },
    }),
});

pub static MZ_COMPUTE_OPERATOR_HYDRATION_STATUSES: LazyLock<BuiltinSource> = LazyLock::new(|| {
    BuiltinSource {
        name: "mz_compute_operator_hydration_statuses",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_COMPUTE_OPERATOR_HYDRATION_STATUSES_OID,
        desc: RelationDesc::builder()
            .with_column("replica_id", SqlScalarType::String.nullable(false))
            .with_column("object_id", SqlScalarType::String.nullable(false))
            .with_column(
                "physical_plan_node_id",
                SqlScalarType::UInt64.nullable(false),
            )
            .with_column("hydrated", SqlScalarType::Bool.nullable(false))
            .with_key(vec![0, 1, 2])
            .finish(),
        data_source: IntrospectionType::ComputeOperatorHydrationStatus.into(),
        column_comments: BTreeMap::from_iter([
            ("replica_id", "The ID of a cluster replica."),
            (
                "object_id",
                "The ID of a compute object. Corresponds to `mz_catalog.mz_indexes.id` or `mz_catalog.mz_materialized_views.id`.",
            ),
            (
                "physical_plan_node_id",
                "The ID of a node in the physical plan of the compute object. Corresponds to a `node_id` displayed in the output of `EXPLAIN PHYSICAL PLAN WITH (node identifiers)`.",
            ),
            ("hydrated", "Whether the node is hydrated on the replica."),
        ]),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "compute_hydration_status",
            description: "Hydration status per compute operator",
            links: &const { [] },
            column_semantic_types: &const {
                [
                    ("replica_id", SemanticType::ReplicaId),
                    ("object_id", SemanticType::CatalogItemId),
                ]
            },
        }),
    }
});

pub static MZ_CLUSTER_REPLICA_UTILIZATION: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_cluster_replica_utilization",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_CLUSTER_REPLICA_UTILIZATION_OID,
    desc: RelationDesc::builder()
        .with_column("replica_id", SqlScalarType::String.nullable(false))
        .with_column("process_id", SqlScalarType::UInt64.nullable(false))
        .with_column("cpu_percent", SqlScalarType::Float64.nullable(true))
        .with_column("memory_percent", SqlScalarType::Float64.nullable(true))
        .with_column("disk_percent", SqlScalarType::Float64.nullable(true))
        .with_column("heap_percent", SqlScalarType::Float64.nullable(true))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("replica_id", "The ID of a cluster replica."),
        ("process_id", "The ID of a process within the replica."),
        (
            "cpu_percent",
            "Approximate CPU usage, in percent of the total allocation.",
        ),
        (
            "memory_percent",
            "Approximate RAM usage, in percent of the total allocation.",
        ),
        (
            "disk_percent",
            "Approximate disk usage, in percent of the total allocation.",
        ),
        (
            "heap_percent",
            "Approximate heap (RAM + swap) usage, in percent of the total allocation.",
        ),
    ]),
    sql: "
SELECT
    r.id AS replica_id,
    m.process_id,
    m.cpu_nano_cores::float8 / NULLIF(s.cpu_nano_cores, 0) * 100 AS cpu_percent,
    m.memory_bytes::float8 / NULLIF(s.memory_bytes, 0) * 100 AS memory_percent,
    m.disk_bytes::float8 / NULLIF(s.disk_bytes, 0) * 100 AS disk_percent,
    m.heap_bytes::float8 / NULLIF(m.heap_limit, 0) * 100 AS heap_percent
FROM
    mz_catalog.mz_cluster_replicas AS r
        JOIN mz_catalog.mz_cluster_replica_sizes AS s ON r.size = s.size
        JOIN mz_internal.mz_cluster_replica_metrics AS m ON m.replica_id = r.id",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "replica_utilization",
        description: "Computed utilization metrics per replica",
        links: &const {
            [OntologyLink {
                name: "utilization_of_replica",
                target: "replica",
                properties: LinkProperties::fk_typed(
                    "replica_id",
                    "id",
                    Cardinality::OneToOne,
                    mz_repr::SemanticType::CatalogItemId,
                ),
            }]
        },
        column_semantic_types: &[("replica_id", SemanticType::ReplicaId)],
    }),
});

pub static MZ_CLUSTER_REPLICA_UTILIZATION_HISTORY: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_cluster_replica_utilization_history",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_CLUSTER_REPLICA_UTILIZATION_HISTORY_OID,
        desc: RelationDesc::builder()
            .with_column("replica_id", SqlScalarType::String.nullable(false))
            .with_column("process_id", SqlScalarType::UInt64.nullable(false))
            .with_column("cpu_percent", SqlScalarType::Float64.nullable(true))
            .with_column("memory_percent", SqlScalarType::Float64.nullable(true))
            .with_column("disk_percent", SqlScalarType::Float64.nullable(true))
            .with_column("heap_percent", SqlScalarType::Float64.nullable(true))
            .with_column(
                "occurred_at",
                SqlScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("replica_id", "The ID of a cluster replica."),
            ("process_id", "The ID of a process within the replica."),
            (
                "cpu_percent",
                "Approximate CPU usage, in percent of the total allocation.",
            ),
            (
                "memory_percent",
                "Approximate RAM usage, in percent of the total allocation.",
            ),
            (
                "disk_percent",
                "Approximate disk usage, in percent of the total allocation.",
            ),
            (
                "heap_percent",
                "Approximate heap (RAM + swap) usage, in percent of the total allocation.",
            ),
            (
                "occurred_at",
                "Wall-clock timestamp at which the event occurred.",
            ),
        ]),
        sql: "
SELECT
    r.id AS replica_id,
    m.process_id,
    m.cpu_nano_cores::float8 / NULLIF(s.cpu_nano_cores, 0) * 100 AS cpu_percent,
    m.memory_bytes::float8 / NULLIF(s.memory_bytes, 0) * 100 AS memory_percent,
    m.disk_bytes::float8 / NULLIF(s.disk_bytes, 0) * 100 AS disk_percent,
    m.heap_bytes::float8 / NULLIF(m.heap_limit, 0) * 100 AS heap_percent,
    m.occurred_at
FROM
    mz_catalog.mz_cluster_replicas AS r
        JOIN mz_catalog.mz_cluster_replica_sizes AS s ON r.size = s.size
        JOIN mz_internal.mz_cluster_replica_metrics_history AS m ON m.replica_id = r.id",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_INDEX_ADVICE: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "mz_index_advice",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_INDEX_ADVICE_OID,
        desc: RelationDesc::builder()
            .with_column("object_id", SqlScalarType::String.nullable(true))
            .with_column("hint", SqlScalarType::String.nullable(false))
            .with_column("details", SqlScalarType::String.nullable(false))
            .with_column("referenced_object_ids", SqlScalarType::List { element_type: Box::new(SqlScalarType::String), custom_id: None }.nullable(true))
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("object_id", "The ID of the object. Corresponds to mz_objects.id."),
            ("hint", "A suggestion to either change the object (e.g. create an index, turn a materialized view into an indexed view) or keep the object unchanged."),
            ("details", "Additional details on why the `hint` was proposed based on the dependencies of the object."),
            ("referenced_object_ids", "The IDs of objects referenced by `details`. Corresponds to mz_objects.id."),
        ]),
        sql: "
-- To avoid confusion with sources and sinks in the materialize sense,
-- the following uses the terms leafs (instead of sinks) and roots (instead of sources)
-- when referring to the object dependency graph.
--
-- The basic idea is to walk up the dependency graph to propagate the transitive dependencies
-- of maintained objected upwards. The leaves of the dependency graph are maintained objects
-- that are not depended on by other maintained objects and have a justification why they must
-- be maintained (e.g. a materialized view that is depended on by a sink).
-- Starting from these leaves, the dependencies are propagated upwards towards the roots according
-- to the object dependencies. Whenever there is a node that is being depended on by multiple
-- downstream objects, that node is marked to be converted into a maintained object and this
-- node is then propagated further up. Once completed, the list of objects that are marked as
-- maintained is checked against all objects to generate appropriate recommendations.
--
-- Note that the recommendations only incorporate dependencies between objects.
-- This can lead to bad recommendations, e.g. filters can no longer be pushed into (or close to)
-- a sink if an index is added in between the sink and the filter. For very selective filters,
-- this can lead to redundant work: the index is computing stuff only to discarded by the selective
-- filter later on. But these kind of aspects cannot be understood by merely looking at the
-- dependencies.
WITH MUTUALLY RECURSIVE
    -- for all objects, understand if they have an index on them and on which cluster they are running
    -- this avoids having different cases for views with an index and materialized views later on
    objects(id text, type text, cluster_id text, indexes text list) AS (
        -- views and materialized views without an index
        SELECT
            o.id,
            o.type,
            o.cluster_id,
            '{}'::text list AS indexes
        FROM mz_catalog.mz_objects o
        WHERE o.id LIKE 'u%' AND o.type IN ('materialized-view', 'view') AND NOT EXISTS (
            SELECT FROM mz_internal.mz_object_dependencies d
            JOIN mz_catalog.mz_objects AS i
                ON (i.id = d.object_id AND i.type = 'index')
            WHERE (o.id = d.referenced_object_id)
        )

        UNION ALL

        -- views and materialized views with an index
        SELECT
            o.id,
            o.type,
            -- o.cluster_id is always NULL for views, so use the cluster of the index instead
            COALESCE(o.cluster_id, i.cluster_id) AS cluster_id,
            list_agg(i.id) AS indexes
        FROM mz_catalog.mz_objects o
        JOIN mz_internal.mz_object_dependencies AS d
            ON (o.id = d.referenced_object_id)
        JOIN mz_catalog.mz_objects AS i
            ON (i.id = d.object_id AND i.type = 'index')
        WHERE o.id LIKE 'u%' AND o.type IN ('materialized-view', 'view', 'source')
        GROUP BY o.id, o.type, o.cluster_id, i.cluster_id
    ),

    -- maintained objects that are at the leafs of the dependency graph with respect to a specific cluster
    maintained_leafs(id text, justification text) AS (
        -- materialized views that are connected to a sink
        SELECT
            m.id,
            s.id AS justification
        FROM objects AS m
        JOIN mz_internal.mz_object_dependencies AS d
            ON (m.id = d.referenced_object_id)
        JOIN mz_catalog.mz_objects AS s
            ON (s.id = d.object_id AND s.type = 'sink')
        WHERE m.type = 'materialized-view'

        UNION ALL

        -- (materialized) views with an index that are not transitively depend on by maintained objects on the same cluster
        SELECT
            v.id,
            unnest(v.indexes) AS justification
        FROM objects AS v
        WHERE v.type IN ('view', 'materialized-view', 'source') AND NOT EXISTS (
            SELECT FROM mz_internal.mz_object_transitive_dependencies AS d
            INNER JOIN mz_catalog.mz_objects AS child
                ON (d.object_id = child.id)
            WHERE d.referenced_object_id = v.id AND child.type IN ('materialized-view', 'index') AND v.cluster_id = child.cluster_id AND NOT v.indexes @> LIST[child.id]
        )
    ),

    -- this is just a helper cte to union multiple lists as part of an aggregation, which is not directly possible in SQL
    agg_maintained_children(id text, maintained_children text list) AS (
        SELECT
            parent_id AS id,
            list_agg(maintained_child) AS maintained_leafs
        FROM (
            SELECT DISTINCT
                d.referenced_object_id AS parent_id,
                -- it's not possible to union lists in an aggregation, so we have to unnest the list first
                unnest(child.maintained_children) AS maintained_child
            FROM propagate_dependencies AS child
            INNER JOIN mz_internal.mz_object_dependencies AS d
                ON (child.id = d.object_id)
        )
        GROUP BY parent_id
    ),

    -- propagate dependencies of maintained objects from the leafs to the roots of the dependency graph and
    -- record a justification when an object should be maintained, e.g. when it is depended on by more than one maintained object
    -- when an object should be maintained, maintained_children will just contain that object so that further upstream objects refer to it in their maintained_children
    propagate_dependencies(id text, maintained_children text list, justification text list) AS (
        -- base case: start with the leafs
        SELECT DISTINCT
            id,
            LIST[id] AS maintained_children,
            list_agg(justification) AS justification
        FROM maintained_leafs
        GROUP BY id

        UNION

        -- recursive case: if there is a child with the same dependencies as the parent,
        -- the parent is only reused by a single child
        SELECT
            parent.id,
            child.maintained_children,
            NULL::text list AS justification
        FROM agg_maintained_children AS parent
        INNER JOIN mz_internal.mz_object_dependencies AS d
            ON (parent.id = d.referenced_object_id)
        INNER JOIN propagate_dependencies AS child
            ON (d.object_id = child.id)
        WHERE parent.maintained_children = child.maintained_children

        UNION

        -- recursive case: if there is NO child with the same dependencies as the parent,
        -- different children are reusing the parent so maintaining the object is justified by itself
        SELECT DISTINCT
            parent.id,
            LIST[parent.id] AS maintained_children,
            parent.maintained_children AS justification
        FROM agg_maintained_children AS parent
        WHERE NOT EXISTS (
            SELECT FROM mz_internal.mz_object_dependencies AS d
            INNER JOIN propagate_dependencies AS child
                ON (d.object_id = child.id AND d.referenced_object_id = parent.id)
            WHERE parent.maintained_children = child.maintained_children
        )
    ),

    objects_with_justification(id text, type text, cluster_id text, maintained_children text list, justification text list, indexes text list) AS (
        SELECT
            p.id,
            o.type,
            o.cluster_id,
            p.maintained_children,
            p.justification,
            o.indexes
        FROM propagate_dependencies p
        JOIN objects AS o
            ON (p.id = o.id)
    ),

    hints(id text, hint text, details text, justification text list) AS (
        -- materialized views that are not required
        SELECT
            id,
            'convert to a view' AS hint,
            'no dependencies from sinks nor from objects on different clusters' AS details,
            justification
        FROM objects_with_justification
        WHERE type = 'materialized-view' AND justification IS NULL

        UNION ALL

        -- materialized views that are required because a sink or a maintained object from a different cluster depends on them
        SELECT
            id,
            'keep' AS hint,
            'dependencies from sinks or objects on different clusters: ' AS details,
            justification
        FROM objects_with_justification AS m
        WHERE type = 'materialized-view' AND justification IS NOT NULL AND EXISTS (
            SELECT FROM unnest(justification) AS dependency
            JOIN mz_catalog.mz_objects s ON (s.type = 'sink' AND s.id = dependency)

            UNION ALL

            SELECT FROM unnest(justification) AS dependency
            JOIN mz_catalog.mz_objects AS d ON (d.id = dependency)
            WHERE d.cluster_id != m.cluster_id
        )

        UNION ALL

        -- materialized views that can be converted to a view with or without an index because NO sink or a maintained object from a different cluster depends on them
        SELECT
            id,
            'convert to a view with an index' AS hint,
            'no dependencies from sinks nor from objects on different clusters, but maintained dependencies on the same cluster: ' AS details,
            justification
        FROM objects_with_justification AS m
        WHERE type = 'materialized-view' AND justification IS NOT NULL AND NOT EXISTS (
            SELECT FROM unnest(justification) AS dependency
            JOIN mz_catalog.mz_objects s ON (s.type = 'sink' AND s.id = dependency)

            UNION ALL

            SELECT FROM unnest(justification) AS dependency
            JOIN mz_catalog.mz_objects AS d ON (d.id = dependency)
            WHERE d.cluster_id != m.cluster_id
        )

        UNION ALL

        -- views that have indexes on different clusters should be a materialized view
        SELECT
            o.id,
            'convert to materialized view' AS hint,
            'dependencies on multiple clusters: ' AS details,
            o.justification
        FROM objects_with_justification o,
            LATERAL unnest(o.justification) j
        LEFT JOIN mz_catalog.mz_objects AS m
            ON (m.id = j AND m.type IN ('index', 'materialized-view'))
        WHERE o.type = 'view' AND o.justification IS NOT NULL
        GROUP BY o.id, o.justification
        HAVING count(DISTINCT m.cluster_id) >= 2

        UNION ALL

        -- views without an index that should be maintained
        SELECT
            id,
            'add index' AS hint,
            'multiple downstream dependencies: ' AS details,
            justification
        FROM objects_with_justification
        WHERE type = 'view' AND justification IS NOT NULL AND indexes = '{}'::text list

        UNION ALL

        -- index inside the dependency graph (not a leaf)
        SELECT
            unnest(indexes) AS id,
            'drop unless queried directly' AS hint,
            'fewer than two downstream dependencies: ' AS details,
            maintained_children AS justification
        FROM objects_with_justification
        WHERE type = 'view' AND NOT indexes = '{}'::text list AND justification IS NULL

        UNION ALL

        -- index on a leaf of the dependency graph
        SELECT
            unnest(indexes) AS id,
            'drop unless queried directly' AS hint,
            'associated object does not have any dependencies (maintained or not maintained)' AS details,
            NULL::text list AS justification
        FROM objects_with_justification
        -- indexes can only be part of justification for leaf nodes
        WHERE type IN ('view', 'materialized-view') AND NOT indexes = '{}'::text list AND justification @> indexes

        UNION ALL

        -- index on a source
        SELECT
            unnest(indexes) AS id,
            'drop unless queried directly' AS hint,
            'sources do not transform data and can expose data directly' AS details,
            NULL::text list AS justification
        FROM objects_with_justification
        -- indexes can only be part of justification for leaf nodes
        WHERE type = 'source' AND NOT indexes = '{}'::text list

        UNION ALL

        -- indexes on views inside the dependency graph
        SELECT
            unnest(indexes) AS id,
            'keep' AS hint,
            'multiple downstream dependencies: ' AS details,
            justification
        FROM objects_with_justification
        -- indexes can only be part of justification for leaf nodes
        WHERE type = 'view' AND justification IS NOT NULL AND NOT indexes = '{}'::text list AND NOT justification @> indexes
    ),

    hints_resolved_ids(id text, hint text, details text, justification text list) AS (
        SELECT
            h.id,
            h.hint,
            h.details || list_agg(o.name)::text AS details,
            h.justification
        FROM hints AS h,
            LATERAL unnest(h.justification) j
        JOIN mz_catalog.mz_objects AS o
            ON (o.id = j)
        GROUP BY h.id, h.hint, h.details, h.justification

        UNION ALL

        SELECT
            id,
            hint,
            details,
            justification
        FROM hints
        WHERE justification IS NULL
    )

SELECT
    h.id AS object_id,
    h.hint AS hint,
    h.details,
    h.justification AS referenced_object_ids
FROM hints_resolved_ids AS h",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
});

/// Peeled version of `PG_AUTHID`: Excludes the columns rolcreaterole and rolcreatedb, to make this
/// view indexable.
pub static PG_AUTHID_CORE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_authid_core",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_PG_AUTHID_CORE_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("rolname", SqlScalarType::String.nullable(false))
        .with_column("rolsuper", SqlScalarType::Bool.nullable(true))
        .with_column("rolinherit", SqlScalarType::Bool.nullable(false))
        .with_column("rolcanlogin", SqlScalarType::Bool.nullable(false))
        .with_column("rolreplication", SqlScalarType::Bool.nullable(false))
        .with_column("rolbypassrls", SqlScalarType::Bool.nullable(false))
        .with_column("rolconnlimit", SqlScalarType::Int32.nullable(false))
        .with_column("rolpassword", SqlScalarType::String.nullable(true))
        .with_column(
            "rolvaliduntil",
            SqlScalarType::TimestampTz { precision: None }.nullable(true),
        )
        .finish(),
    column_comments: BTreeMap::new(),
    sql: r#"
SELECT
    r.oid AS oid,
    r.name AS rolname,
    rolsuper,
    inherit AS rolinherit,
    COALESCE(r.rolcanlogin, false) AS rolcanlogin,
    -- MZ doesn't support replication in the same way Postgres does
    false AS rolreplication,
    -- MZ doesn't how row level security
    false AS rolbypassrls,
    -- MZ doesn't have a connection limit
    -1 AS rolconnlimit,
    a.password_hash AS rolpassword,
    NULL::pg_catalog.timestamptz AS rolvaliduntil
FROM mz_catalog.mz_roles r
LEFT JOIN mz_catalog.mz_role_auth a ON r.oid = a.role_oid"#,
    access: vec![rbac::owner_privilege(ObjectType::Table, MZ_SYSTEM_ROLE_ID)],
    ontology: None,
});

pub const PG_AUTHID_CORE_IND: BuiltinIndex = BuiltinIndex {
    name: "pg_authid_core_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_PG_AUTHID_CORE_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.pg_authid_core (rolname)",
    is_retained_metrics_object: false,
};

pub static MZ_SHOW_ALL_OBJECTS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_all_objects",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_ALL_OBJECTS_OID,
    desc: RelationDesc::builder()
        .with_column("schema_id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("type", SqlScalarType::String.nullable(false))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "WITH comments AS (
        SELECT id, object_type, comment
        FROM mz_internal.mz_comments
        WHERE object_sub_id IS NULL
    )
    SELECT schema_id, name, type, COALESCE(comment, '') AS comment
    FROM mz_catalog.mz_objects AS objs
    LEFT JOIN comments ON objs.id = comments.id
    WHERE (comments.object_type = objs.type OR comments.object_type IS NULL)",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_CLUSTERS: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
    name: "mz_show_clusters",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_CLUSTERS_OID,
    desc: RelationDesc::builder()
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("replicas", SqlScalarType::String.nullable(true))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
    WITH clusters AS (
        SELECT
            mc.id,
            mc.name,
            pg_catalog.string_agg(mcr.name || ' (' || mcr.size || ')', ', ' ORDER BY mcr.name) AS replicas
        FROM mz_catalog.mz_clusters mc
        LEFT JOIN mz_catalog.mz_cluster_replicas mcr
        ON mc.id = mcr.cluster_id
        GROUP BY mc.id, mc.name
    ),
    comments AS (
        SELECT id, comment
        FROM mz_internal.mz_comments
        WHERE object_type = 'cluster' AND object_sub_id IS NULL
    )
    SELECT name, replicas, COALESCE(comment, '') as comment
    FROM clusters
    LEFT JOIN comments ON clusters.id = comments.id",
    access: vec![PUBLIC_SELECT],
    ontology: None,
}
});

pub static MZ_SHOW_SECRETS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_secrets",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_SECRETS_OID,
    desc: RelationDesc::builder()
        .with_column("schema_id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "WITH comments AS (
        SELECT id, comment
        FROM mz_internal.mz_comments
        WHERE object_type = 'secret' AND object_sub_id IS NULL
    )
    SELECT schema_id, name, COALESCE(comment, '') as comment
    FROM mz_catalog.mz_secrets secrets
    LEFT JOIN comments ON secrets.id = comments.id",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_COLUMNS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_columns",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_COLUMNS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("nullable", SqlScalarType::Bool.nullable(false))
        .with_column("type", SqlScalarType::String.nullable(false))
        .with_column("position", SqlScalarType::UInt64.nullable(false))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
    SELECT columns.id, name, nullable, type, position, COALESCE(comment, '') as comment
    FROM mz_catalog.mz_columns columns
    LEFT JOIN mz_internal.mz_comments comments
    ON columns.id = comments.id AND columns.position = comments.object_sub_id",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_DATABASES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_databases",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_DATABASES_OID,
    desc: RelationDesc::builder()
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "WITH comments AS (
        SELECT id, comment
        FROM mz_internal.mz_comments
        WHERE object_type = 'database' AND object_sub_id IS NULL
    )
    SELECT name, COALESCE(comment, '') as comment
    FROM mz_catalog.mz_databases databases
    LEFT JOIN comments ON databases.id = comments.id",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_SCHEMAS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_schemas",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_SCHEMAS_OID,
    desc: RelationDesc::builder()
        .with_column("database_id", SqlScalarType::String.nullable(true))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "WITH comments AS (
        SELECT id, comment
        FROM mz_internal.mz_comments
        WHERE object_type = 'schema' AND object_sub_id IS NULL
    )
    SELECT database_id, name, COALESCE(comment, '') as comment
    FROM mz_catalog.mz_schemas schemas
    LEFT JOIN comments ON schemas.id = comments.id",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_ROLES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_roles",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_ROLES_OID,
    desc: RelationDesc::builder()
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "WITH comments AS (
        SELECT id, comment
        FROM mz_internal.mz_comments
        WHERE object_type = 'role' AND object_sub_id IS NULL
    )
    SELECT name, COALESCE(comment, '') as comment
    FROM mz_catalog.mz_roles roles
    LEFT JOIN comments ON roles.id = comments.id
    WHERE roles.id NOT LIKE 's%'
      AND roles.id NOT LIKE 'g%'",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_TABLES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_tables",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_TABLES_OID,
    desc: RelationDesc::builder()
        .with_column("schema_id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .with_column("source_id", SqlScalarType::String.nullable(true))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "WITH comments AS (
        SELECT id, comment
        FROM mz_internal.mz_comments
        WHERE object_type = 'table' AND object_sub_id IS NULL
    )
    SELECT schema_id, name, COALESCE(comment, '') as comment, source_id
    FROM mz_catalog.mz_tables tables
    LEFT JOIN comments ON tables.id = comments.id",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_VIEWS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_views",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_VIEWS_OID,
    desc: RelationDesc::builder()
        .with_column("schema_id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "WITH comments AS (
        SELECT id, comment
        FROM mz_internal.mz_comments
        WHERE object_type = 'view' AND object_sub_id IS NULL
    )
    SELECT schema_id, name, COALESCE(comment, '') as comment
    FROM mz_catalog.mz_views views
    LEFT JOIN comments ON views.id = comments.id",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_TYPES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_types",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_TYPES_OID,
    desc: RelationDesc::builder()
        .with_column("schema_id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "WITH comments AS (
        SELECT id, comment
        FROM mz_internal.mz_comments
        WHERE object_type = 'type' AND object_sub_id IS NULL
    )
    SELECT schema_id, name, COALESCE(comment, '') as comment
    FROM mz_catalog.mz_types types
    LEFT JOIN comments ON types.id = comments.id",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_CONNECTIONS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_connections",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_CONNECTIONS_OID,
    desc: RelationDesc::builder()
        .with_column("schema_id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("type", SqlScalarType::String.nullable(false))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "WITH comments AS (
        SELECT id, comment
        FROM mz_internal.mz_comments
        WHERE object_type = 'connection' AND object_sub_id IS NULL
    )
    SELECT schema_id, name, type, COALESCE(comment, '') as comment
    FROM mz_catalog.mz_connections connections
    LEFT JOIN comments ON connections.id = comments.id",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_SOURCES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_sources",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_SOURCES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("type", SqlScalarType::String.nullable(false))
        .with_column("cluster", SqlScalarType::String.nullable(true))
        .with_column("schema_id", SqlScalarType::String.nullable(false))
        .with_column("cluster_id", SqlScalarType::String.nullable(true))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
WITH comments AS (
    SELECT id, comment
    FROM mz_internal.mz_comments
    WHERE object_type = 'source' AND object_sub_id IS NULL
)
SELECT
    sources.id,
    sources.name,
    sources.type,
    clusters.name AS cluster,
    schema_id,
    cluster_id,
    COALESCE(comments.comment, '') as comment
FROM
    mz_catalog.mz_sources AS sources
        LEFT JOIN
            mz_catalog.mz_clusters AS clusters
            ON clusters.id = sources.cluster_id
        LEFT JOIN comments ON sources.id = comments.id",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_SINKS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_sinks",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_SINKS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("type", SqlScalarType::String.nullable(false))
        .with_column("cluster", SqlScalarType::String.nullable(false))
        .with_column("schema_id", SqlScalarType::String.nullable(false))
        .with_column("cluster_id", SqlScalarType::String.nullable(false))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
WITH comments AS (
    SELECT id, comment
    FROM mz_internal.mz_comments
    WHERE object_type = 'sink' AND object_sub_id IS NULL
)
SELECT
    sinks.id,
    sinks.name,
    sinks.type,
    clusters.name AS cluster,
    schema_id,
    cluster_id,
    COALESCE(comments.comment, '') as comment
FROM
    mz_catalog.mz_sinks AS sinks
    JOIN
        mz_catalog.mz_clusters AS clusters
        ON clusters.id = sinks.cluster_id
    LEFT JOIN comments ON sinks.id = comments.id",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_MATERIALIZED_VIEWS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_materialized_views",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_MATERIALIZED_VIEWS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("cluster", SqlScalarType::String.nullable(false))
        .with_column("schema_id", SqlScalarType::String.nullable(false))
        .with_column("cluster_id", SqlScalarType::String.nullable(false))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
WITH
    comments AS (
        SELECT id, comment
        FROM mz_internal.mz_comments
        WHERE object_type = 'materialized-view' AND object_sub_id IS NULL
    )
SELECT
    mviews.id as id,
    mviews.name,
    clusters.name AS cluster,
    schema_id,
    cluster_id,
    COALESCE(comments.comment, '') as comment
FROM
    mz_catalog.mz_materialized_views AS mviews
    JOIN mz_catalog.mz_clusters AS clusters ON clusters.id = mviews.cluster_id
    LEFT JOIN comments ON mviews.id = comments.id",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_INDEXES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_indexes",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_INDEXES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("on", SqlScalarType::String.nullable(false))
        .with_column("cluster", SqlScalarType::String.nullable(false))
        .with_column(
            "key",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false),
        )
        .with_column("on_id", SqlScalarType::String.nullable(false))
        .with_column("schema_id", SqlScalarType::String.nullable(false))
        .with_column("cluster_id", SqlScalarType::String.nullable(false))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
WITH comments AS (
    SELECT id, comment
    FROM mz_internal.mz_comments
    WHERE object_type = 'index' AND object_sub_id IS NULL
)
SELECT
    idxs.id AS id,
    idxs.name AS name,
    objs.name AS on,
    clusters.name AS cluster,
    COALESCE(keys.key, '{}'::_text) AS key,
    idxs.on_id AS on_id,
    objs.schema_id AS schema_id,
    clusters.id AS cluster_id,
    COALESCE(comments.comment, '') as comment
FROM
    mz_catalog.mz_indexes AS idxs
    JOIN mz_catalog.mz_objects AS objs ON idxs.on_id = objs.id
    JOIN mz_catalog.mz_clusters AS clusters ON clusters.id = idxs.cluster_id
    LEFT JOIN
        (SELECT
            idxs.id,
            ARRAY_AGG(
                CASE
                    WHEN idx_cols.on_expression IS NULL THEN obj_cols.name
                    ELSE idx_cols.on_expression
                END
                ORDER BY idx_cols.index_position ASC
            ) AS key
        FROM
            mz_catalog.mz_indexes AS idxs
            JOIN mz_catalog.mz_index_columns idx_cols ON idxs.id = idx_cols.index_id
            LEFT JOIN mz_catalog.mz_columns obj_cols ON
                idxs.on_id = obj_cols.id AND idx_cols.on_position = obj_cols.position
        GROUP BY idxs.id) AS keys
    ON idxs.id = keys.id
    LEFT JOIN comments ON idxs.id = comments.id",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_CLUSTER_REPLICAS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_cluster_replicas",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_CLUSTER_REPLICAS_OID,
    desc: RelationDesc::builder()
        .with_column("cluster", SqlScalarType::String.nullable(false))
        .with_column("replica", SqlScalarType::String.nullable(false))
        .with_column("replica_id", SqlScalarType::String.nullable(false))
        .with_column("size", SqlScalarType::String.nullable(true))
        .with_column("ready", SqlScalarType::Bool.nullable(false))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: r#"SELECT
    mz_catalog.mz_clusters.name AS cluster,
    mz_catalog.mz_cluster_replicas.name AS replica,
    mz_catalog.mz_cluster_replicas.id as replica_id,
    mz_catalog.mz_cluster_replicas.size AS size,
    coalesce(statuses.ready, FALSE) AS ready,
    coalesce(comments.comment, '') as comment
FROM
    mz_catalog.mz_cluster_replicas
        JOIN mz_catalog.mz_clusters
            ON mz_catalog.mz_cluster_replicas.cluster_id = mz_catalog.mz_clusters.id
        LEFT JOIN
            (
                SELECT
                    replica_id,
                    bool_and(hydrated) AS ready
                FROM mz_internal.mz_hydration_statuses
                WHERE replica_id is not null
                GROUP BY replica_id
            ) AS statuses
            ON mz_catalog.mz_cluster_replicas.id = statuses.replica_id
        LEFT JOIN mz_internal.mz_comments comments
            ON mz_catalog.mz_cluster_replicas.id = comments.id
WHERE (comments.object_type = 'cluster-replica' OR comments.object_type IS NULL)
ORDER BY 1, 2"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

/// Lightweight data product discovery for MCP (Model Context Protocol).
///
/// Lists materialized views and indexed views that the current user has
/// SELECT privileges on. Non-indexed regular views are excluded because
/// querying them would trigger a full recompute. Comments are optional
/// enrichment.
/// Used by the `get_data_products` and `read_data_product` MCP tools.
/// Does not include schema details: use `mz_mcp_data_product_details` for that.
pub static MZ_MCP_DATA_PRODUCTS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_mcp_data_products",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_MCP_DATA_PRODUCTS_OID,
    desc: RelationDesc::builder()
        .with_column("object_name", SqlScalarType::String.nullable(false))
        .with_column("cluster", SqlScalarType::String.nullable(true))
        .with_column("description", SqlScalarType::String.nullable(true))
        .with_key(vec![0, 1, 2])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "object_name",
            "Fully qualified object name (database.schema.name).",
        ),
        (
            "cluster",
            "Cluster where the object computes or its index is hosted. The object can be read from any cluster.",
        ),
        (
            "description",
            "Index comment if available, otherwise object comment. Used as data product description.",
        ),
    ]),
    sql: r#"
SELECT DISTINCT
    '"' || op.database || '"."' || op.schema || '"."' || op.name || '"' AS object_name,
    COALESCE(c_idx.name, c_obj.name) AS cluster,
    COALESCE(cts_idx.comment, cts_obj.comment) AS description
FROM mz_internal.mz_show_my_object_privileges op
JOIN mz_objects o ON op.name = o.name AND op.object_type = o.type
JOIN mz_schemas s ON s.name = op.schema AND s.id = o.schema_id
JOIN mz_databases d ON d.name = op.database AND d.id = s.database_id
LEFT JOIN mz_indexes i ON i.on_id = o.id
LEFT JOIN mz_clusters c_idx ON c_idx.id = i.cluster_id
LEFT JOIN mz_clusters c_obj ON c_obj.id = o.cluster_id
LEFT JOIN mz_internal.mz_comments cts_idx ON cts_idx.id = i.id AND cts_idx.object_sub_id IS NULL
LEFT JOIN mz_internal.mz_comments cts_obj ON cts_obj.id = o.id AND cts_obj.object_sub_id IS NULL
WHERE op.privilege_type = 'SELECT'
  AND (o.type = 'materialized-view' OR (o.type = 'view' AND i.id IS NOT NULL))
  AND s.name NOT IN ('mz_catalog', 'mz_internal', 'pg_catalog', 'information_schema', 'mz_introspection')
"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

/// Full data product details with JSON Schema for MCP agents.
///
/// Extends `mz_mcp_data_products` with column types, index keys (when
/// available), and column comments, formatted as a JSON Schema object.
/// Used by the `get_data_product_details` MCP tool. Lists materialized
/// views and indexed views; non-indexed regular views are excluded to
/// avoid triggering full recompute on query. Comments are optional
/// enrichment.
pub static MZ_MCP_DATA_PRODUCT_DETAILS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_mcp_data_product_details",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_MCP_DATA_PRODUCT_DETAILS_OID,
    desc: RelationDesc::builder()
        .with_column("object_name", SqlScalarType::String.nullable(false))
        .with_column("cluster", SqlScalarType::String.nullable(true))
        .with_column("description", SqlScalarType::String.nullable(true))
        .with_column("schema", SqlScalarType::Jsonb.nullable(false))
        .with_key(vec![0, 1, 2])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "object_name",
            "Fully qualified object name (database.schema.name).",
        ),
        (
            "cluster",
            "Cluster where the object computes or its index is hosted. The object can be read from any cluster.",
        ),
        (
            "description",
            "Index comment if available, otherwise object comment. Used as data product description.",
        ),
        (
            "schema",
            "JSON Schema describing the object's columns and types.",
        ),
    ]),
    sql: r#"
SELECT * FROM (
    SELECT
        '"' || op.database || '"."' || op.schema || '"."' || op.name || '"' AS object_name,
        COALESCE(c_idx.name, c_obj.name) AS cluster,
        COALESCE(cts_idx.comment, cts_obj.comment) AS description,
        COALESCE(jsonb_build_object(
        'type', 'object',
        'required', jsonb_agg(distinct ccol.name) FILTER (WHERE ccol.position = ic.on_position),
        'properties', jsonb_strip_nulls(jsonb_object_agg(
            ccol.name,
            CASE
                WHEN ccol.type IN (
                    'uint2', 'uint4','uint8', 'int', 'integer', 'smallint',
                    'double', 'double precision', 'bigint', 'float',
                    'numeric', 'real'
                ) THEN jsonb_build_object(
                    'type', 'number',
                    'description', cts_col.comment
                )
                WHEN ccol.type = 'boolean' THEN jsonb_build_object(
                    'type', 'boolean',
                    'description', cts_col.comment
                )
                WHEN ccol.type = 'bytea' THEN jsonb_build_object(
                    'type', 'string',
                    'description', cts_col.comment,
                    'contentEncoding', 'base64',
                    'contentMediaType', 'application/octet-stream'
                )
                WHEN ccol.type = 'date' THEN jsonb_build_object(
                    'type', 'string',
                    'format', 'date',
                    'description', cts_col.comment
                )
                WHEN ccol.type = 'time' THEN jsonb_build_object(
                    'type', 'string',
                    'format', 'time',
                    'description', cts_col.comment
                )
                WHEN ccol.type ilike 'timestamp%%' THEN jsonb_build_object(
                    'type', 'string',
                    'format', 'date-time',
                    'description', cts_col.comment
                )
                WHEN ccol.type = 'jsonb' THEN jsonb_build_object(
                    'type', 'object',
                    'description', cts_col.comment
                )
                WHEN ccol.type = 'uuid' THEN jsonb_build_object(
                    'type', 'string',
                    'format', 'uuid',
                    'description', cts_col.comment
                )
                ELSE jsonb_build_object(
                    'type', 'string',
                    'description', cts_col.comment
                )
            END
        ))
    ), '{"type": "object", "properties": {}}'::jsonb) AS schema
FROM mz_internal.mz_show_my_object_privileges op
JOIN mz_objects o ON op.name = o.name AND op.object_type = o.type
JOIN mz_schemas s ON s.name = op.schema AND s.id = o.schema_id
JOIN mz_databases d ON d.name = op.database AND d.id = s.database_id
JOIN mz_columns ccol ON ccol.id = o.id
LEFT JOIN mz_indexes i ON i.on_id = o.id
LEFT JOIN mz_index_columns ic ON i.id = ic.index_id
LEFT JOIN mz_clusters c_idx ON c_idx.id = i.cluster_id
LEFT JOIN mz_clusters c_obj ON c_obj.id = o.cluster_id
LEFT JOIN mz_internal.mz_comments cts_idx ON cts_idx.id = i.id AND cts_idx.object_sub_id IS NULL
LEFT JOIN mz_internal.mz_comments cts_obj ON cts_obj.id = o.id AND cts_obj.object_sub_id IS NULL
LEFT JOIN mz_internal.mz_comments cts_col ON cts_col.id = o.id AND cts_col.object_sub_id = ccol.position
WHERE op.privilege_type = 'SELECT'
  AND (o.type = 'materialized-view' OR (o.type = 'view' AND i.id IS NOT NULL))
  AND s.name NOT IN ('mz_catalog', 'mz_internal', 'pg_catalog', 'information_schema', 'mz_introspection')
GROUP BY 1, 2, 3
)
"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_ROLE_MEMBERS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_role_members",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_ROLE_MEMBERS_OID,
    desc: RelationDesc::builder()
        .with_column("role", SqlScalarType::String.nullable(false))
        .with_column("member", SqlScalarType::String.nullable(false))
        .with_column("grantor", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("role", "The role that `member` is a member of."),
        ("member", "The role that is a member of `role`."),
        (
            "grantor",
            "The role that granted membership of `member` to `role`.",
        ),
    ]),
    sql: r#"SELECT
    r1.name AS role,
    r2.name AS member,
    r3.name AS grantor
FROM mz_catalog.mz_role_members rm
JOIN mz_catalog.mz_roles r1 ON r1.id = rm.role_id
JOIN mz_catalog.mz_roles r2 ON r2.id = rm.member
JOIN mz_catalog.mz_roles r3 ON r3.id = rm.grantor
ORDER BY role"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_MY_ROLE_MEMBERS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_my_role_members",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_MY_ROLE_MEMBERS_OID,
    desc: RelationDesc::builder()
        .with_column("role", SqlScalarType::String.nullable(false))
        .with_column("member", SqlScalarType::String.nullable(false))
        .with_column("grantor", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("role", "The role that `member` is a member of."),
        ("member", "The role that is a member of `role`."),
        (
            "grantor",
            "The role that granted membership of `member` to `role`.",
        ),
    ]),
    sql: r#"SELECT role, member, grantor
FROM mz_internal.mz_show_role_members
WHERE pg_has_role(member, 'USAGE')"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_SYSTEM_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_system_privileges",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_SYSTEM_PRIVILEGES_OID,
    desc: RelationDesc::builder()
        .with_column("grantor", SqlScalarType::String.nullable(true))
        .with_column("grantee", SqlScalarType::String.nullable(true))
        .with_column("privilege_type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("grantor", "The role that granted the privilege."),
        ("grantee", "The role that the privilege was granted to."),
        ("privilege_type", "They type of privilege granted."),
    ]),
    sql: r#"SELECT
    grantor.name AS grantor,
    CASE privileges.grantee
        WHEN 'p' THEN 'PUBLIC'
        ELSE grantee.name
    END AS grantee,
    privileges.privilege_type AS privilege_type
FROM
    (SELECT mz_internal.mz_aclexplode(ARRAY[privileges]).*
    FROM mz_catalog.mz_system_privileges) AS privileges
LEFT JOIN mz_catalog.mz_roles grantor ON privileges.grantor = grantor.id
LEFT JOIN mz_catalog.mz_roles grantee ON privileges.grantee = grantee.id
WHERE privileges.grantee NOT LIKE 's%'"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_MY_SYSTEM_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_my_system_privileges",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_MY_SYSTEM_PRIVILEGES_OID,
    desc: RelationDesc::builder()
        .with_column("grantor", SqlScalarType::String.nullable(true))
        .with_column("grantee", SqlScalarType::String.nullable(true))
        .with_column("privilege_type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("grantor", "The role that granted the privilege."),
        ("grantee", "The role that the privilege was granted to."),
        ("privilege_type", "They type of privilege granted."),
    ]),
    sql: r#"SELECT grantor, grantee, privilege_type
FROM mz_internal.mz_show_system_privileges
WHERE
    CASE
        WHEN grantee = 'PUBLIC' THEN true
        ELSE pg_has_role(grantee, 'USAGE')
    END"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_CLUSTER_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_cluster_privileges",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_CLUSTER_PRIVILEGES_OID,
    desc: RelationDesc::builder()
        .with_column("grantor", SqlScalarType::String.nullable(true))
        .with_column("grantee", SqlScalarType::String.nullable(true))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("privilege_type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("grantor", "The role that granted the privilege."),
        ("grantee", "The role that the privilege was granted to."),
        ("name", "The name of the cluster."),
        ("privilege_type", "They type of privilege granted."),
    ]),
    sql: r#"SELECT
    grantor.name AS grantor,
    CASE privileges.grantee
        WHEN 'p' THEN 'PUBLIC'
        ELSE grantee.name
    END AS grantee,
    privileges.name AS name,
    privileges.privilege_type AS privilege_type
FROM
    (SELECT mz_internal.mz_aclexplode(privileges).*, name
    FROM mz_catalog.mz_clusters
    WHERE id NOT LIKE 's%') AS privileges
LEFT JOIN mz_catalog.mz_roles grantor ON privileges.grantor = grantor.id
LEFT JOIN mz_catalog.mz_roles grantee ON privileges.grantee = grantee.id
WHERE privileges.grantee NOT LIKE 's%'"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_MY_CLUSTER_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_my_cluster_privileges",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_MY_CLUSTER_PRIVILEGES_OID,
    desc: RelationDesc::builder()
        .with_column("grantor", SqlScalarType::String.nullable(true))
        .with_column("grantee", SqlScalarType::String.nullable(true))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("privilege_type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("grantor", "The role that granted the privilege."),
        ("grantee", "The role that the privilege was granted to."),
        ("name", "The name of the cluster."),
        ("privilege_type", "They type of privilege granted."),
    ]),
    sql: r#"SELECT grantor, grantee, name, privilege_type
FROM mz_internal.mz_show_cluster_privileges
WHERE
    CASE
        WHEN grantee = 'PUBLIC' THEN true
        ELSE pg_has_role(grantee, 'USAGE')
    END"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_DATABASE_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_database_privileges",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_DATABASE_PRIVILEGES_OID,
    desc: RelationDesc::builder()
        .with_column("grantor", SqlScalarType::String.nullable(true))
        .with_column("grantee", SqlScalarType::String.nullable(true))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("privilege_type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("grantor", "The role that granted the privilege."),
        ("grantee", "The role that the privilege was granted to."),
        ("name", "The name of the database."),
        ("privilege_type", "They type of privilege granted."),
    ]),
    sql: r#"SELECT
    grantor.name AS grantor,
    CASE privileges.grantee
        WHEN 'p' THEN 'PUBLIC'
        ELSE grantee.name
    END AS grantee,
    privileges.name AS name,
    privileges.privilege_type AS privilege_type
FROM
    (SELECT mz_internal.mz_aclexplode(privileges).*, name
    FROM mz_catalog.mz_databases
    WHERE id NOT LIKE 's%') AS privileges
LEFT JOIN mz_catalog.mz_roles grantor ON privileges.grantor = grantor.id
LEFT JOIN mz_catalog.mz_roles grantee ON privileges.grantee = grantee.id
WHERE privileges.grantee NOT LIKE 's%'"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_MY_DATABASE_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_my_database_privileges",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_MY_DATABASE_PRIVILEGES_OID,
    desc: RelationDesc::builder()
        .with_column("grantor", SqlScalarType::String.nullable(true))
        .with_column("grantee", SqlScalarType::String.nullable(true))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("privilege_type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("grantor", "The role that granted the privilege."),
        ("grantee", "The role that the privilege was granted to."),
        ("name", "The name of the cluster."),
        ("privilege_type", "They type of privilege granted."),
    ]),
    sql: r#"SELECT grantor, grantee, name, privilege_type
FROM mz_internal.mz_show_database_privileges
WHERE
    CASE
        WHEN grantee = 'PUBLIC' THEN true
        ELSE pg_has_role(grantee, 'USAGE')
    END"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_SCHEMA_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_schema_privileges",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_SCHEMA_PRIVILEGES_OID,
    desc: RelationDesc::builder()
        .with_column("grantor", SqlScalarType::String.nullable(true))
        .with_column("grantee", SqlScalarType::String.nullable(true))
        .with_column("database", SqlScalarType::String.nullable(true))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("privilege_type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("grantor", "The role that granted the privilege."),
        ("grantee", "The role that the privilege was granted to."),
        (
            "database",
            "The name of the database containing the schema.",
        ),
        ("name", "The name of the schema."),
        ("privilege_type", "They type of privilege granted."),
    ]),
    sql: r#"SELECT
    grantor.name AS grantor,
    CASE privileges.grantee
        WHEN 'p' THEN 'PUBLIC'
        ELSE grantee.name
    END AS grantee,
    databases.name AS database,
    privileges.name AS name,
    privileges.privilege_type AS privilege_type
FROM
    (SELECT mz_internal.mz_aclexplode(privileges).*, database_id, name
    FROM mz_catalog.mz_schemas
    WHERE id NOT LIKE 's%') AS privileges
LEFT JOIN mz_catalog.mz_roles grantor ON privileges.grantor = grantor.id
LEFT JOIN mz_catalog.mz_roles grantee ON privileges.grantee = grantee.id
LEFT JOIN mz_catalog.mz_databases databases ON privileges.database_id = databases.id
WHERE privileges.grantee NOT LIKE 's%'"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_MY_SCHEMA_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_my_schema_privileges",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_MY_SCHEMA_PRIVILEGES_OID,
    desc: RelationDesc::builder()
        .with_column("grantor", SqlScalarType::String.nullable(true))
        .with_column("grantee", SqlScalarType::String.nullable(true))
        .with_column("database", SqlScalarType::String.nullable(true))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("privilege_type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("grantor", "The role that granted the privilege."),
        ("grantee", "The role that the privilege was granted to."),
        (
            "database",
            "The name of the database containing the schema.",
        ),
        ("name", "The name of the schema."),
        ("privilege_type", "They type of privilege granted."),
    ]),
    sql: r#"SELECT grantor, grantee, database, name, privilege_type
FROM mz_internal.mz_show_schema_privileges
WHERE
    CASE
        WHEN grantee = 'PUBLIC' THEN true
        ELSE pg_has_role(grantee, 'USAGE')
    END"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_OBJECT_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_object_privileges",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_OBJECT_PRIVILEGES_OID,
    desc: RelationDesc::builder()
        .with_column("grantor", SqlScalarType::String.nullable(true))
        .with_column("grantee", SqlScalarType::String.nullable(true))
        .with_column("database", SqlScalarType::String.nullable(true))
        .with_column("schema", SqlScalarType::String.nullable(true))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("object_type", SqlScalarType::String.nullable(false))
        .with_column("privilege_type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("grantor", "The role that granted the privilege."),
        ("grantee", "The role that the privilege was granted to."),
        (
            "database",
            "The name of the database containing the object.",
        ),
        ("schema", "The name of the schema containing the object."),
        ("name", "The name of the object."),
        (
            "object_type",
            "The type of object the privilege is granted on.",
        ),
        ("privilege_type", "They type of privilege granted."),
    ]),
    sql: r#"SELECT
    grantor.name AS grantor,
    CASE privileges.grantee
            WHEN 'p' THEN 'PUBLIC'
            ELSE grantee.name
        END AS grantee,
    databases.name AS database,
    schemas.name AS schema,
    privileges.name AS name,
    privileges.type AS object_type,
    privileges.privilege_type AS privilege_type
FROM
    (SELECT mz_internal.mz_aclexplode(privileges).*, schema_id, name, type
    FROM mz_catalog.mz_objects
    WHERE id NOT LIKE 's%') AS privileges
LEFT JOIN mz_catalog.mz_roles grantor ON privileges.grantor = grantor.id
LEFT JOIN mz_catalog.mz_roles grantee ON privileges.grantee = grantee.id
LEFT JOIN mz_catalog.mz_schemas schemas ON privileges.schema_id = schemas.id
LEFT JOIN mz_catalog.mz_databases databases ON schemas.database_id = databases.id
WHERE privileges.grantee NOT LIKE 's%'"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_MY_OBJECT_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_my_object_privileges",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_MY_OBJECT_PRIVILEGES_OID,
    desc: RelationDesc::builder()
        .with_column("grantor", SqlScalarType::String.nullable(true))
        .with_column("grantee", SqlScalarType::String.nullable(true))
        .with_column("database", SqlScalarType::String.nullable(true))
        .with_column("schema", SqlScalarType::String.nullable(true))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("object_type", SqlScalarType::String.nullable(false))
        .with_column("privilege_type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("grantor", "The role that granted the privilege."),
        ("grantee", "The role that the privilege was granted to."),
        (
            "database",
            "The name of the database containing the object.",
        ),
        ("schema", "The name of the schema containing the object."),
        ("name", "The name of the object."),
        (
            "object_type",
            "The type of object the privilege is granted on.",
        ),
        ("privilege_type", "They type of privilege granted."),
    ]),
    sql: r#"SELECT grantor, grantee, database, schema, name, object_type, privilege_type
FROM mz_internal.mz_show_object_privileges
WHERE
    CASE
        WHEN grantee = 'PUBLIC' THEN true
        -- Semantically equivalent to pg_has_role(grantee, 'USAGE'), which checks
        -- whether the current user holds role `grantee`. For a nonexistent grantee
        -- name, both return false. We use mz_session_role_memberships() instead
        -- because pg_has_role internally calls mz_role_oid_memberships(), which
        -- loads the full system role graph and is blocked in restricted sessions.
        ELSE grantee = ANY(mz_internal.mz_session_role_memberships())
    END"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_ALL_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_all_privileges",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_ALL_PRIVILEGES_OID,
    desc: RelationDesc::builder()
        .with_column("grantor", SqlScalarType::String.nullable(true))
        .with_column("grantee", SqlScalarType::String.nullable(true))
        .with_column("database", SqlScalarType::String.nullable(true))
        .with_column("schema", SqlScalarType::String.nullable(true))
        .with_column("name", SqlScalarType::String.nullable(true))
        .with_column("object_type", SqlScalarType::String.nullable(false))
        .with_column("privilege_type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("grantor", "The role that granted the privilege."),
        ("grantee", "The role that the privilege was granted to."),
        (
            "database",
            "The name of the database containing the object.",
        ),
        ("schema", "The name of the schema containing the object."),
        ("name", "The name of the privilege target."),
        (
            "object_type",
            "The type of object the privilege is granted on.",
        ),
        ("privilege_type", "They type of privilege granted."),
    ]),
    sql: r#"SELECT grantor, grantee, NULL AS database, NULL AS schema, NULL AS name, 'system' AS object_type, privilege_type
FROM mz_internal.mz_show_system_privileges
UNION ALL
SELECT grantor, grantee, NULL AS database, NULL AS schema, name, 'cluster' AS object_type, privilege_type
FROM mz_internal.mz_show_cluster_privileges
UNION ALL
SELECT grantor, grantee, NULL AS database, NULL AS schema, name, 'database' AS object_type, privilege_type
FROM mz_internal.mz_show_database_privileges
UNION ALL
SELECT grantor, grantee, database, NULL AS schema, name, 'schema' AS object_type, privilege_type
FROM mz_internal.mz_show_schema_privileges
UNION ALL
SELECT grantor, grantee, database, schema, name, object_type, privilege_type
FROM mz_internal.mz_show_object_privileges"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_ALL_MY_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_all_my_privileges",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_ALL_MY_PRIVILEGES_OID,
    desc: RelationDesc::builder()
        .with_column("grantor", SqlScalarType::String.nullable(true))
        .with_column("grantee", SqlScalarType::String.nullable(true))
        .with_column("database", SqlScalarType::String.nullable(true))
        .with_column("schema", SqlScalarType::String.nullable(true))
        .with_column("name", SqlScalarType::String.nullable(true))
        .with_column("object_type", SqlScalarType::String.nullable(false))
        .with_column("privilege_type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("grantor", "The role that granted the privilege."),
        ("grantee", "The role that the privilege was granted to."),
        (
            "database",
            "The name of the database containing the object.",
        ),
        ("schema", "The name of the schema containing the object."),
        ("name", "The name of the privilege target."),
        (
            "object_type",
            "The type of object the privilege is granted on.",
        ),
        ("privilege_type", "They type of privilege granted."),
    ]),
    sql: r#"SELECT grantor, grantee, database, schema, name, object_type, privilege_type
FROM mz_internal.mz_show_all_privileges
WHERE
    CASE
        WHEN grantee = 'PUBLIC' THEN true
        ELSE pg_has_role(grantee, 'USAGE')
    END"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_DEFAULT_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_default_privileges",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_DEFAULT_PRIVILEGES_OID,
    desc: RelationDesc::builder()
        .with_column("object_owner", SqlScalarType::String.nullable(true))
        .with_column("database", SqlScalarType::String.nullable(true))
        .with_column("schema", SqlScalarType::String.nullable(true))
        .with_column("object_type", SqlScalarType::String.nullable(false))
        .with_column("grantee", SqlScalarType::String.nullable(true))
        .with_column("privilege_type", SqlScalarType::String.nullable(true))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "object_owner",
            "Privileges described in this row will be granted on objects created by `object_owner`.",
        ),
        (
            "database",
            "Privileges described in this row will be granted only on objects created in `database` if non-null.",
        ),
        (
            "schema",
            "Privileges described in this row will be granted only on objects created in `schema` if non-null.",
        ),
        (
            "object_type",
            "Privileges described in this row will be granted only on objects of type `object_type`.",
        ),
        (
            "grantee",
            "Privileges described in this row will be granted to `grantee`.",
        ),
        ("privilege_type", "They type of privilege to be granted."),
    ]),
    sql: r#"SELECT
    CASE defaults.role_id
        WHEN 'p' THEN 'PUBLIC'
        ELSE object_owner.name
    END AS object_owner,
    databases.name AS database,
    schemas.name AS schema,
    object_type,
    CASE defaults.grantee
        WHEN 'p' THEN 'PUBLIC'
        ELSE grantee.name
    END AS grantee,
    unnest(mz_internal.mz_format_privileges(defaults.privileges)) AS privilege_type
FROM mz_catalog.mz_default_privileges defaults
LEFT JOIN mz_catalog.mz_roles AS object_owner ON defaults.role_id = object_owner.id
LEFT JOIN mz_catalog.mz_roles AS grantee ON defaults.grantee = grantee.id
LEFT JOIN mz_catalog.mz_databases AS databases ON defaults.database_id = databases.id
LEFT JOIN mz_catalog.mz_schemas AS schemas ON defaults.schema_id = schemas.id
WHERE defaults.grantee NOT LIKE 's%'
    AND defaults.database_id IS NULL OR defaults.database_id NOT LIKE 's%'
    AND defaults.schema_id IS NULL OR defaults.schema_id NOT LIKE 's%'"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_MY_DEFAULT_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_my_default_privileges",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_MY_DEFAULT_PRIVILEGES_OID,
    desc: RelationDesc::builder()
        .with_column("object_owner", SqlScalarType::String.nullable(true))
        .with_column("database", SqlScalarType::String.nullable(true))
        .with_column("schema", SqlScalarType::String.nullable(true))
        .with_column("object_type", SqlScalarType::String.nullable(false))
        .with_column("grantee", SqlScalarType::String.nullable(true))
        .with_column("privilege_type", SqlScalarType::String.nullable(true))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "object_owner",
            "Privileges described in this row will be granted on objects created by `object_owner`.",
        ),
        (
            "database",
            "Privileges described in this row will be granted only on objects created in `database` if non-null.",
        ),
        (
            "schema",
            "Privileges described in this row will be granted only on objects created in `schema` if non-null.",
        ),
        (
            "object_type",
            "Privileges described in this row will be granted only on objects of type `object_type`.",
        ),
        (
            "grantee",
            "Privileges described in this row will be granted to `grantee`.",
        ),
        ("privilege_type", "They type of privilege to be granted."),
    ]),
    sql: r#"SELECT object_owner, database, schema, object_type, grantee, privilege_type
FROM mz_internal.mz_show_default_privileges
WHERE
    CASE
        WHEN grantee = 'PUBLIC' THEN true
        ELSE pg_has_role(grantee, 'USAGE')
    END"#,
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_SHOW_NETWORK_POLICIES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_network_policies",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_NETWORK_POLICIES_OID,
    desc: RelationDesc::builder()
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("rules", SqlScalarType::String.nullable(true))
        .with_column("comment", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
WITH comments AS (
    SELECT id, comment
    FROM mz_internal.mz_comments
    WHERE object_type = 'network-policy' AND object_sub_id IS NULL
)
SELECT
    policy.name,
    pg_catalog.string_agg(rule.name,',' ORDER BY rule.name) as rules,
    COALESCE(comment, '') as comment
FROM
    mz_internal.mz_network_policies as policy
LEFT JOIN
    mz_internal.mz_network_policy_rules as rule ON policy.id = rule.policy_id
LEFT JOIN
    comments ON policy.id = comments.id
WHERE
    policy.id NOT LIKE 's%'
AND
    policy.id NOT LIKE 'g%'
GROUP BY policy.name, comments.comment;",
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_CLUSTER_REPLICA_HISTORY: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_cluster_replica_history",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_CLUSTER_REPLICA_HISTORY_OID,
    desc: RelationDesc::builder()
        .with_column("replica_id", SqlScalarType::String.nullable(true))
        .with_column("size", SqlScalarType::String.nullable(true))
        .with_column("cluster_id", SqlScalarType::String.nullable(true))
        .with_column("cluster_name", SqlScalarType::String.nullable(true))
        .with_column("replica_name", SqlScalarType::String.nullable(true))
        .with_column(
            "created_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_column(
            "dropped_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(true),
        )
        .with_column(
            "credits_per_hour",
            SqlScalarType::Numeric { max_scale: None }.nullable(true),
        )
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("replica_id", "The ID of a cluster replica."),
        (
            "size",
            "The size of the cluster replica. Corresponds to `mz_cluster_replica_sizes.size`.",
        ),
        (
            "cluster_id",
            "The ID of the cluster associated with the replica.",
        ),
        (
            "cluster_name",
            "The name of the cluster associated with the replica.",
        ),
        ("replica_name", "The name of the replica."),
        ("created_at", "The time at which the replica was created."),
        (
            "dropped_at",
            "The time at which the replica was dropped, or `NULL` if it still exists.",
        ),
        (
            "credits_per_hour",
            "The number of compute credits consumed per hour. Corresponds to `mz_cluster_replica_sizes.credits_per_hour`.",
        ),
    ]),
    sql: r#"
        WITH
            creates AS
            (
                SELECT
                    details ->> 'logical_size' AS size,
                    details ->> 'replica_id' AS replica_id,
                    details ->> 'replica_name' AS replica_name,
                    details ->> 'cluster_name' AS cluster_name,
                    details ->> 'cluster_id' AS cluster_id,
                    occurred_at
                FROM mz_catalog.mz_audit_events
                WHERE
                    object_type = 'cluster-replica' AND event_type = 'create'
                        AND
                    details ->> 'replica_id' IS NOT NULL
                        AND
                    details ->> 'cluster_id' !~~ 's%'
            ),
            drops AS
            (
                SELECT details ->> 'replica_id' AS replica_id, occurred_at
                FROM mz_catalog.mz_audit_events
                WHERE object_type = 'cluster-replica' AND event_type = 'drop'
            )
        SELECT
            creates.replica_id,
            creates.size,
            creates.cluster_id,
            creates.cluster_name,
            creates.replica_name,
            creates.occurred_at AS created_at,
            drops.occurred_at AS dropped_at,
            mz_cluster_replica_sizes.credits_per_hour as credits_per_hour
        FROM
            creates
                LEFT JOIN drops ON creates.replica_id = drops.replica_id
                LEFT JOIN
                    mz_catalog.mz_cluster_replica_sizes
                    ON mz_cluster_replica_sizes.size = creates.size"#,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "replica_history",
        description: "Historical record of replica creation/drops",
        links: &const { [] },
        column_semantic_types: &[],
    }),
});

pub static MZ_CLUSTER_REPLICA_NAME_HISTORY: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_cluster_replica_name_history",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_CLUSTER_REPLICA_NAME_HISTORY_OID,
    desc: RelationDesc::builder()
        .with_column(
            "occurred_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(true),
        )
        .with_column("id", SqlScalarType::String.nullable(true))
        .with_column("previous_name", SqlScalarType::String.nullable(true))
        .with_column("new_name", SqlScalarType::String.nullable(true))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "occurred_at",
            "The time at which the cluster replica was created or renamed. `NULL` if it's a built in system cluster replica.",
        ),
        ("id", "The ID of the cluster replica."),
        (
            "previous_name",
            "The previous name of the cluster replica. `NULL` if there was no previous name.",
        ),
        ("new_name", "The new name of the cluster replica."),
    ]),
    sql: r#"WITH user_replica_alter_history AS (
  SELECT occurred_at,
    audit_events.details->>'replica_id' AS id,
    audit_events.details->>'old_name' AS previous_name,
    audit_events.details->>'new_name' AS new_name
  FROM mz_catalog.mz_audit_events AS audit_events
  WHERE object_type = 'cluster-replica'
    AND audit_events.event_type = 'alter'
    AND audit_events.details->>'replica_id' like 'u%'
),
user_replica_create_history AS (
  SELECT occurred_at,
    audit_events.details->>'replica_id' AS id,
    NULL AS previous_name,
    audit_events.details->>'replica_name' AS new_name
  FROM mz_catalog.mz_audit_events AS audit_events
  WHERE object_type = 'cluster-replica'
    AND audit_events.event_type = 'create'
    AND audit_events.details->>'replica_id' like 'u%'
),
-- Because built in system cluster replicas don't have audit events, we need to manually add them
system_replicas AS (
  -- We assume that the system cluster replicas were created at the beginning of time
  SELECT NULL::timestamptz AS occurred_at,
    id,
    NULL AS previous_name,
    name AS new_name
  FROM mz_catalog.mz_cluster_replicas
  WHERE id LIKE 's%'
)
SELECT *
FROM user_replica_alter_history
UNION ALL
SELECT *
FROM user_replica_create_history
UNION ALL
SELECT *
FROM system_replicas"#,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "replica_name_history",
        description: "Historical replica names",
        links: &const { [] },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});

pub static MZ_HYDRATION_STATUSES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_hydration_statuses",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_HYDRATION_STATUSES_OID,
    desc: RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("replica_id", SqlScalarType::String.nullable(true))
        .with_column("hydrated", SqlScalarType::Bool.nullable(true))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "object_id",
            "The ID of a dataflow-powered object. Corresponds to `mz_catalog.mz_indexes.id`, `mz_catalog.mz_materialized_views.id`, `mz_internal.mz_subscriptions`, `mz_catalog.mz_sources.id`, or `mz_catalog.mz_sinks.id`.",
        ),
        ("replica_id", "The ID of a cluster replica."),
        ("hydrated", "Whether the object is hydrated on the replica."),
    ]),
    sql: r#"WITH
-- Joining against the linearizable catalog tables ensures that this view
-- always contains the set of installed objects, even when it depends
-- on introspection relations that may received delayed updates.
--
-- Note that this view only includes objects that are maintained by dataflows.
-- In particular, some source types (webhook, introspection, ...) are not and
-- are therefore omitted.
indexes AS (
    SELECT
        i.id AS object_id,
        h.replica_id,
        COALESCE(h.hydrated, false) AS hydrated
    FROM mz_catalog.mz_indexes i
    LEFT JOIN mz_internal.mz_compute_hydration_statuses h
        ON (h.object_id = i.id)
),
materialized_views AS (
    SELECT
        i.id AS object_id,
        h.replica_id,
        COALESCE(h.hydrated, false) AS hydrated
    FROM mz_catalog.mz_materialized_views i
    LEFT JOIN mz_internal.mz_compute_hydration_statuses h
        ON (h.object_id = i.id)
),
-- Hydration is a dataflow concept and not all sources are maintained by
-- dataflows, so we need to find the ones that are. Generally, sources that
-- have a cluster ID are maintained by a dataflow running on that cluster.
-- Webhook sources are an exception to this rule.
sources_with_clusters AS (
    SELECT id, cluster_id
    FROM mz_catalog.mz_sources
    WHERE cluster_id IS NOT NULL AND type != 'webhook'
),
sources AS (
    SELECT
        s.id AS object_id,
        ss.replica_id AS replica_id,
        ss.rehydration_latency IS NOT NULL AS hydrated
    FROM sources_with_clusters s
    LEFT JOIN mz_internal.mz_source_statistics ss USING (id)
),
-- We don't yet report sink hydration status (database-issues#8331), so we do a best effort attempt here and
-- define a sink as hydrated when it's both "running" and has a frontier greater than the minimum.
-- There is likely still a possibility of FPs.
sinks AS (
    SELECT
        s.id AS object_id,
        r.id AS replica_id,
        ss.status = 'running' AND COALESCE(f.write_frontier, 0) > 0 AS hydrated
    FROM mz_catalog.mz_sinks s
    LEFT JOIN mz_internal.mz_sink_statuses ss USING (id)
    JOIN mz_catalog.mz_cluster_replicas r
        ON (r.cluster_id = s.cluster_id)
    LEFT JOIN mz_catalog.mz_cluster_replica_frontiers f
        ON (f.object_id = s.id AND f.replica_id = r.id)
)
SELECT * FROM indexes
UNION ALL
SELECT * FROM materialized_views
UNION ALL
SELECT * FROM sources
UNION ALL
SELECT * FROM sinks"#,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "hydration_status",
        description: "Overall hydration status per object",
        links: &const {
            [
                OntologyLink {
                    name: "hydration_of",
                    target: "object",
                    properties: LinkProperties::fk_typed(
                        "object_id",
                        "id",
                        Cardinality::OneToOne,
                        mz_repr::SemanticType::CatalogItemId,
                    ),
                },
                OntologyLink {
                    name: "hydration_on_replica",
                    target: "replica",
                    properties: LinkProperties::fk("replica_id", "id", Cardinality::ManyToOne),
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("object_id", SemanticType::CatalogItemId),
                ("replica_id", SemanticType::ReplicaId),
            ]
        },
    }),
});

pub const MZ_HYDRATION_STATUSES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_hydration_statuses_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_HYDRATION_STATUSES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_hydration_statuses (object_id, replica_id)",
    is_retained_metrics_object: false,
};

pub static MZ_MATERIALIZATION_DEPENDENCIES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_materialization_dependencies",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_MATERIALIZATION_DEPENDENCIES_OID,
    desc: RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("dependency_id", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "object_id",
            "The ID of a materialization. Corresponds to `mz_catalog.mz_indexes.id`, `mz_catalog.mz_materialized_views.id`, or `mz_catalog.mz_sinks.id`.",
        ),
        (
            "dependency_id",
            "The ID of a dataflow dependency. Corresponds to `mz_catalog.mz_indexes.id`, `mz_catalog.mz_materialized_views.id`, `mz_catalog.mz_sources.id`, or `mz_catalog.mz_tables.id`.",
        ),
    ]),
    sql: "
SELECT object_id, dependency_id
FROM mz_internal.mz_compute_dependencies
UNION ALL
SELECT s.id, d.referenced_object_id AS dependency_id
FROM mz_internal.mz_object_dependencies d
JOIN mz_catalog.mz_sinks s ON (s.id = d.object_id)
JOIN mz_catalog.mz_relations r ON (r.id = d.referenced_object_id)",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "materialization_dep",
        description: "Dependencies between materializations",
        links: &const {
            [
                OntologyLink {
                    name: "depends_on",
                    target: "object",
                    properties: LinkProperties::DependsOn {
                        source_column: "object_id",
                        target_column: "id",
                        source_id_type: Some(mz_repr::SemanticType::CatalogItemId),
                        requires_mapping: None,
                    },
                },
                OntologyLink {
                    name: "dependency_is",
                    target: "object",
                    properties: LinkProperties::fk("dependency_id", "id", Cardinality::ManyToOne),
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("object_id", SemanticType::CatalogItemId),
                ("dependency_id", SemanticType::CatalogItemId),
            ]
        },
    }),
});

pub static MZ_MATERIALIZATION_LAG: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_materialization_lag",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_MATERIALIZATION_LAG_OID,
    desc: RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("local_lag", SqlScalarType::Interval.nullable(true))
        .with_column("global_lag", SqlScalarType::Interval.nullable(true))
        .with_column(
            "slowest_local_input_id",
            SqlScalarType::String.nullable(false),
        )
        .with_column(
            "slowest_global_input_id",
            SqlScalarType::String.nullable(false),
        )
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "object_id",
            "The ID of the materialized view, index, or sink.",
        ),
        (
            "local_lag",
            "The amount of time the materialization lags behind its direct inputs.",
        ),
        (
            "global_lag",
            "The amount of time the materialization lags behind its root inputs (sources and tables).",
        ),
        (
            "slowest_local_input_id",
            "The ID of the slowest direct input.",
        ),
        (
            "slowest_global_input_id",
            "The ID of the slowest root input.",
        ),
    ]),
    sql: "
WITH MUTUALLY RECURSIVE
    -- IDs of objects for which we want to know the lag.
    materializations (id text) AS (
        SELECT id FROM mz_catalog.mz_indexes
        UNION ALL
        SELECT id FROM mz_catalog.mz_materialized_views
        UNION ALL
        SELECT id FROM mz_catalog.mz_sinks
    ),
    -- Direct dependencies of materializations.
    direct_dependencies (id text, dep_id text) AS (
        SELECT m.id, d.dependency_id
        FROM materializations m
        JOIN mz_internal.mz_materialization_dependencies d ON (m.id = d.object_id)
    ),
    -- All transitive dependencies of materializations.
    transitive_dependencies (id text, dep_id text) AS (
        SELECT id, dep_id FROM direct_dependencies
        UNION
        SELECT td.id, dd.dep_id
        FROM transitive_dependencies td
        JOIN direct_dependencies dd ON (dd.id = td.dep_id)
    ),
    -- Root dependencies of materializations (sources and tables).
    root_dependencies (id text, dep_id text) AS (
        SELECT *
        FROM transitive_dependencies td
        WHERE NOT EXISTS (
            SELECT 1
            FROM direct_dependencies dd
            WHERE dd.id = td.dep_id
        )
    ),
    -- Write progress times of materializations.
    materialization_times (id text, time timestamptz) AS (
        SELECT m.id, to_timestamp(f.write_frontier::text::double / 1000)
        FROM materializations m
        JOIN mz_internal.mz_frontiers f ON (m.id = f.object_id)
    ),
    -- Write progress times of direct dependencies of materializations.
    input_times (id text, slowest_dep text, time timestamptz) AS (
        SELECT DISTINCT ON (d.id)
            d.id,
            d.dep_id,
            to_timestamp(f.write_frontier::text::double / 1000)
        FROM direct_dependencies d
        JOIN mz_internal.mz_frontiers f ON (d.dep_id = f.object_id)
        ORDER BY d.id, f.write_frontier ASC
    ),
    -- Write progress times of root dependencies of materializations.
    root_times (id text, slowest_dep text, time timestamptz) AS (
        SELECT DISTINCT ON (d.id)
            d.id,
            d.dep_id,
            to_timestamp(f.write_frontier::text::double / 1000)
        FROM root_dependencies d
        JOIN mz_internal.mz_frontiers f ON (d.dep_id = f.object_id)
        ORDER BY d.id, f.write_frontier ASC
    )
SELECT
    id AS object_id,
    -- Ensure that lag values are always NULL for materializations that have reached the empty
    -- frontier, as those have processed all their input data.
    -- Also make sure that lag values are never negative, even when input frontiers are before
    -- output frontiers (as can happen during hydration).
    CASE
        WHEN m.time IS NULL THEN INTERVAL '0'
        WHEN i.time IS NULL THEN NULL
        ELSE greatest(i.time - m.time, INTERVAL '0')
    END AS local_lag,
    CASE
        WHEN m.time IS NULL THEN INTERVAL '0'
        WHEN r.time IS NULL THEN NULL
        ELSE greatest(r.time - m.time, INTERVAL '0')
    END AS global_lag,
    i.slowest_dep AS slowest_local_input_id,
    r.slowest_dep AS slowest_global_input_id
FROM materialization_times m
JOIN input_times i USING (id)
JOIN root_times r USING (id)",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "materialization_lag",
        description: "Lag between a materialization and its inputs",
        links: &const {
            [
                OntologyLink {
                    name: "measures_materialization_lag",
                    target: "object",
                    properties: LinkProperties::measures("object_id", "id", "materialization_lag"),
                },
                OntologyLink {
                    name: "slowest_local_input",
                    target: "object",
                    properties: LinkProperties::fk(
                        "slowest_local_input_id",
                        "id",
                        Cardinality::ManyToOne,
                    ),
                },
                OntologyLink {
                    name: "slowest_global_input",
                    target: "object",
                    properties: LinkProperties::fk(
                        "slowest_global_input_id",
                        "id",
                        Cardinality::ManyToOne,
                    ),
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("object_id", SemanticType::CatalogItemId),
                ("slowest_local_input_id", SemanticType::CatalogItemId),
                ("slowest_global_input_id", SemanticType::CatalogItemId),
            ]
        },
    }),
});
/**
 * This view is used to display the cluster utilization over 14 days bucketed by 8 hours.
 * It's specifically for the Console's environment overview page to speed up load times.
 * This query should be kept in sync with MaterializeInc/console/src/api/materialize/cluster/replicaUtilizationHistory.ts
 */
pub static MZ_CONSOLE_CLUSTER_UTILIZATION_OVERVIEW: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "mz_console_cluster_utilization_overview",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_CONSOLE_CLUSTER_UTILIZATION_OVERVIEW_OID,
        desc: RelationDesc::builder()
            .with_column(
                "bucket_start",
                SqlScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_column("replica_id", SqlScalarType::String.nullable(false))
            .with_column("memory_percent", SqlScalarType::Float64.nullable(true))
            .with_column(
                "max_memory_at",
                SqlScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_column("disk_percent", SqlScalarType::Float64.nullable(true))
            .with_column(
                "max_disk_at",
                SqlScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_column(
                "memory_and_disk_percent",
                SqlScalarType::Float64.nullable(true),
            )
            .with_column(
                "max_memory_and_disk_memory_percent",
                SqlScalarType::Float64.nullable(true),
            )
            .with_column(
                "max_memory_and_disk_disk_percent",
                SqlScalarType::Float64.nullable(true),
            )
            .with_column(
                "max_memory_and_disk_at",
                SqlScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_column("heap_percent", SqlScalarType::Float64.nullable(true))
            .with_column(
                "max_heap_at",
                SqlScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_column("max_cpu_percent", SqlScalarType::Float64.nullable(true))
            .with_column(
                "max_cpu_at",
                SqlScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_column("offline_events", SqlScalarType::Jsonb.nullable(true))
            .with_column(
                "bucket_end",
                SqlScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_column("name", SqlScalarType::String.nullable(true))
            .with_column("cluster_id", SqlScalarType::String.nullable(true))
            .with_column("size", SqlScalarType::String.nullable(true))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: r#"WITH replica_history AS (
  SELECT replica_id,
    size,
    cluster_id
  FROM mz_internal.mz_cluster_replica_history
  UNION
  -- We need to union the current set of cluster replicas since mz_cluster_replica_history doesn't include system clusters
  SELECT id AS replica_id,
    size,
    cluster_id
  FROM mz_catalog.mz_cluster_replicas
),
replica_metrics_history AS (
  SELECT
    m.occurred_at,
    m.replica_id,
    r.size,
    (SUM(m.cpu_nano_cores::float8) / NULLIF(s.cpu_nano_cores, 0)) / NULLIF(s.processes, 0) AS cpu_percent,
    (SUM(m.memory_bytes::float8) / NULLIF(s.memory_bytes, 0)) / NULLIF(s.processes, 0) AS memory_percent,
    (SUM(m.disk_bytes::float8) / NULLIF(s.disk_bytes, 0)) / NULLIF(s.processes, 0) AS disk_percent,
    (SUM(m.heap_bytes::float8) / NULLIF(m.heap_limit, 0)) / NULLIF(s.processes, 0) AS heap_percent,
    SUM(m.disk_bytes::float8) AS disk_bytes,
    SUM(m.memory_bytes::float8) AS memory_bytes,
    s.disk_bytes::numeric * s.processes AS total_disk_bytes,
    s.memory_bytes::numeric * s.processes AS total_memory_bytes
  FROM
    replica_history AS r
    INNER JOIN mz_catalog.mz_cluster_replica_sizes AS s ON r.size = s.size
    INNER JOIN mz_internal.mz_cluster_replica_metrics_history AS m ON m.replica_id = r.replica_id
  GROUP BY
    m.occurred_at,
    m.replica_id,
    r.size,
    s.cpu_nano_cores,
    s.memory_bytes,
    s.disk_bytes,
    m.heap_limit,
    s.processes
),
replica_utilization_history_binned AS (
  SELECT m.occurred_at,
    m.replica_id,
    m.cpu_percent,
    m.memory_percent,
    m.memory_bytes,
    m.disk_percent,
    m.disk_bytes,
    m.heap_percent,
    m.total_disk_bytes,
    m.total_memory_bytes,
    m.size,
    date_bin(
      '8 HOURS',
      occurred_at,
      '1970-01-01'::timestamp
    ) AS bucket_start
  FROM replica_history AS r
    JOIN replica_metrics_history AS m ON m.replica_id = r.replica_id
  WHERE mz_now() <= date_bin(
      '8 HOURS',
      occurred_at,
      '1970-01-01'::timestamp
    ) + INTERVAL '14 DAYS'
),
-- For each (replica, bucket), take the (replica, bucket) with the highest memory
max_memory AS (
  SELECT DISTINCT ON (bucket_start, replica_id) bucket_start,
    replica_id,
    memory_percent,
    occurred_at
  FROM replica_utilization_history_binned
  OPTIONS (DISTINCT ON INPUT GROUP SIZE = 480)
  ORDER BY bucket_start,
    replica_id,
    COALESCE(memory_bytes, 0) DESC
),
max_disk AS (
  SELECT DISTINCT ON (bucket_start, replica_id) bucket_start,
    replica_id,
    disk_percent,
    occurred_at
  FROM replica_utilization_history_binned
  OPTIONS (DISTINCT ON INPUT GROUP SIZE = 480)
  ORDER BY bucket_start,
    replica_id,
    COALESCE(disk_bytes, 0) DESC
),
max_cpu AS (
  SELECT DISTINCT ON (bucket_start, replica_id) bucket_start,
    replica_id,
    cpu_percent,
    occurred_at
  FROM replica_utilization_history_binned
  OPTIONS (DISTINCT ON INPUT GROUP SIZE = 480)
  ORDER BY bucket_start,
    replica_id,
    COALESCE(cpu_percent, 0) DESC
),
/*
 This is different
 from adding max_memory
 and max_disk per bucket because both
 values may not occur at the same time if the bucket interval is large.
 */
max_memory_and_disk AS (
  SELECT DISTINCT ON (bucket_start, replica_id) bucket_start,
    replica_id,
    memory_percent,
    disk_percent,
    memory_and_disk_percent,
    occurred_at
  FROM (
      SELECT *,
        CASE
          WHEN disk_bytes IS NULL
          AND memory_bytes IS NULL THEN NULL
          ELSE (COALESCE(disk_bytes, 0) + COALESCE(memory_bytes, 0))
               / (total_disk_bytes::numeric + total_memory_bytes::numeric)
        END AS memory_and_disk_percent
      FROM replica_utilization_history_binned
    ) AS max_memory_and_disk_inner
  OPTIONS (DISTINCT ON INPUT GROUP SIZE = 480)
  ORDER BY bucket_start,
    replica_id,
    COALESCE(memory_and_disk_percent, 0) DESC
),
max_heap AS (
  SELECT DISTINCT ON (bucket_start, replica_id)
    bucket_start,
    replica_id,
    heap_percent,
    occurred_at
  FROM replica_utilization_history_binned
  OPTIONS (DISTINCT ON INPUT GROUP SIZE = 480)
  ORDER BY bucket_start, replica_id, COALESCE(heap_percent, 0) DESC
),
-- For each (replica, bucket), get its offline events at that time
replica_offline_event_history AS (
  SELECT date_bin(
      '8 HOURS',
      occurred_at,
      '1970-01-01'::timestamp
    ) AS bucket_start,
    replica_id,
    jsonb_agg(
      jsonb_build_object(
        'replicaId',
        rsh.replica_id,
        'occurredAt',
        rsh.occurred_at,
        'status',
        rsh.status,
        'reason',
        rsh.reason
      )
    ) AS offline_events
  FROM mz_internal.mz_cluster_replica_status_history AS rsh -- We assume the statuses for process 0 are the same as all processes
  WHERE process_id = '0'
    AND status = 'offline'
    AND mz_now() <= date_bin(
      '8 HOURS',
      occurred_at,
      '1970-01-01'::timestamp
    ) + INTERVAL '14 DAYS'
  GROUP BY bucket_start,
    replica_id
)
SELECT
  bucket_start,
  replica_id,
  max_memory.memory_percent,
  max_memory.occurred_at as max_memory_at,
  max_disk.disk_percent,
  max_disk.occurred_at as max_disk_at,
  max_memory_and_disk.memory_and_disk_percent as memory_and_disk_percent,
  max_memory_and_disk.memory_percent as max_memory_and_disk_memory_percent,
  max_memory_and_disk.disk_percent as max_memory_and_disk_disk_percent,
  max_memory_and_disk.occurred_at as max_memory_and_disk_at,
  max_heap.heap_percent,
  max_heap.occurred_at as max_heap_at,
  max_cpu.cpu_percent as max_cpu_percent,
  max_cpu.occurred_at as max_cpu_at,
  replica_offline_event_history.offline_events,
  bucket_start + INTERVAL '8 HOURS' as bucket_end,
  replica_name_history.new_name AS name,
  replica_history.cluster_id,
  replica_history.size
FROM max_memory
JOIN max_disk USING (bucket_start, replica_id)
JOIN max_cpu USING (bucket_start, replica_id)
JOIN max_memory_and_disk USING (bucket_start, replica_id)
JOIN max_heap USING (bucket_start, replica_id)
JOIN replica_history USING (replica_id)
CROSS JOIN LATERAL (
  SELECT new_name
  FROM mz_internal.mz_cluster_replica_name_history as replica_name_history
  WHERE replica_id = replica_name_history.id -- We treat NULLs as the beginning of time
    AND bucket_start + INTERVAL '8 HOURS' >= COALESCE(
      replica_name_history.occurred_at,
      '1970-01-01'::timestamp
    )
  ORDER BY replica_name_history.occurred_at DESC
  LIMIT '1'
) AS replica_name_history
LEFT JOIN replica_offline_event_history USING (bucket_start, replica_id)"#,
        access: vec![PUBLIC_SELECT],
        ontology: None,
    }
});
/**
 * Traces the blue/green deployment lineage in the audit log to determine all cluster
 * IDs that are logically the same cluster.
 * cluster_id: The ID of a cluster.
 * current_deployment_cluster_id: The cluster ID of the last cluster in
 *   cluster_id's blue/green lineage.
 * cluster_name: The name of the cluster.
 * The approach taken is as follows. First, find all extant clusters and add them
 * to the result set. Per cluster, we do the following:
 * 1. Find the most recent create or rename event. This moment represents when the
 *    cluster took on its final logical identity.
 * 2. Look for a cluster that had the same name (or the same name with `_dbt_deploy`
 *    appended) that was dropped within one minute of that moment. That cluster is
 *    almost certainly the logical predecessor of the current cluster. Add the cluster
 *    to the result set.
 * 3. Repeat the procedure until a cluster with no logical predecessor is discovered.
 * Limiting the search for a dropped cluster to a window of one minute is a heuristic,
 * but one that's likely to be pretty good one. If a name is reused after more
 * than one minute, that's a good sign that it wasn't an automatic blue/green
 * process, but someone turning on a new use case that happens to have the same
 * name as a previous but logically distinct use case.
 */
pub static MZ_CLUSTER_DEPLOYMENT_LINEAGE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_cluster_deployment_lineage",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_CLUSTER_DEPLOYMENT_LINEAGE_OID,
    desc: RelationDesc::builder()
        .with_column("cluster_id", SqlScalarType::String.nullable(true))
        .with_column(
            "current_deployment_cluster_id",
            SqlScalarType::String.nullable(false),
        )
        .with_column("cluster_name", SqlScalarType::String.nullable(false))
        .with_key(vec![0, 1, 2])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "cluster_id",
            "The ID of the cluster. Corresponds to `mz_clusters.id` (though the cluster may no longer exist).",
        ),
        (
            "current_deployment_cluster_id",
            "The cluster ID of the last cluster in `cluster_id`'s blue/green lineage (the cluster is guaranteed to exist).",
        ),
        ("cluster_name", "The name of the cluster"),
    ]),
    sql: r#"WITH MUTUALLY RECURSIVE cluster_events (
  cluster_id text,
  cluster_name text,
  event_type text,
  occurred_at timestamptz
) AS (
  SELECT coalesce(details->>'id', details->>'cluster_id') AS cluster_id,
    coalesce(details->>'name', details->>'new_name') AS cluster_name,
    event_type,
    occurred_at
  FROM mz_audit_events
  WHERE (
      event_type IN ('create', 'drop')
      OR (
        event_type = 'alter'
        AND details ? 'new_name'
      )
    )
    AND object_type = 'cluster'
    AND mz_now() < occurred_at + INTERVAL '30 days'
),
mz_cluster_deployment_lineage (
  cluster_id text,
  current_deployment_cluster_id text,
  cluster_name text
) AS (
  SELECT c.id,
    c.id,
    c.name
  FROM mz_clusters c
  WHERE c.id LIKE 'u%'
  UNION
  SELECT *
  FROM dropped_clusters
),
-- Closest create or rename event based on the current clusters in the result set
most_recent_create_or_rename (
  cluster_id text,
  current_deployment_cluster_id text,
  cluster_name text,
  occurred_at timestamptz
) AS (
  SELECT DISTINCT ON (e.cluster_id) e.cluster_id,
    c.current_deployment_cluster_id,
    e.cluster_name,
    e.occurred_at
  FROM mz_cluster_deployment_lineage c
    JOIN cluster_events e ON c.cluster_id = e.cluster_id
    AND c.cluster_name = e.cluster_name
  WHERE e.event_type <> 'drop'
  ORDER BY e.cluster_id,
    e.occurred_at DESC
),
-- Clusters that were dropped most recently within 1 minute of most_recent_create_or_rename
dropped_clusters (
  cluster_id text,
  current_deployment_cluster_id text,
  cluster_name text
) AS (
  SELECT DISTINCT ON (cr.cluster_id) e.cluster_id,
    cr.current_deployment_cluster_id,
    cr.cluster_name
  FROM most_recent_create_or_rename cr
    JOIN cluster_events e ON e.occurred_at BETWEEN cr.occurred_at - interval '1 minute'
    AND cr.occurred_at + interval '1 minute'
    AND (
      e.cluster_name = cr.cluster_name
      OR e.cluster_name = cr.cluster_name || '_dbt_deploy'
    )
  WHERE e.event_type = 'drop'
  ORDER BY cr.cluster_id,
    abs(
      extract(
        epoch
        FROM cr.occurred_at - e.occurred_at
      )
    )
)
SELECT *
FROM mz_cluster_deployment_lineage"#,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "cluster_deployment",
        description: "Cluster deployment lineage information",
        links: &const {
            [
                OntologyLink {
                    name: "deployment_of",
                    target: "cluster",
                    properties: LinkProperties::fk("cluster_id", "id", Cardinality::ManyToOne),
                },
                OntologyLink {
                    name: "current_deployment",
                    target: "cluster",
                    properties: LinkProperties::fk(
                        "current_deployment_cluster_id",
                        "id",
                        Cardinality::ManyToOne,
                    ),
                },
            ]
        },
        column_semantic_types: &[],
    }),
});

pub const MZ_SHOW_DATABASES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_databases_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_DATABASES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_databases (name)",
    is_retained_metrics_object: false,
};

pub const MZ_SHOW_SCHEMAS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_schemas_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_SCHEMAS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_schemas (database_id)",
    is_retained_metrics_object: false,
};

pub const MZ_SHOW_CONNECTIONS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_connections_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_CONNECTIONS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_connections (schema_id)",
    is_retained_metrics_object: false,
};

pub const MZ_SHOW_TABLES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_tables_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_TABLES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_tables (schema_id)",
    is_retained_metrics_object: false,
};

pub const MZ_SHOW_SOURCES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_sources_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_SOURCES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_sources (schema_id)",
    is_retained_metrics_object: false,
};

pub const MZ_SHOW_VIEWS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_views_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_VIEWS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_views (schema_id)",
    is_retained_metrics_object: false,
};

pub const MZ_SHOW_MATERIALIZED_VIEWS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_materialized_views_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_MATERIALIZED_VIEWS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_materialized_views (schema_id)",
    is_retained_metrics_object: false,
};

pub const MZ_SHOW_SINKS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_sinks_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_SINKS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_sinks (schema_id)",
    is_retained_metrics_object: false,
};

pub const MZ_SHOW_TYPES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_types_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_TYPES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_types (schema_id)",
    is_retained_metrics_object: false,
};

pub const MZ_SHOW_ROLES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_roles_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_ROLES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_roles (name)",
    is_retained_metrics_object: false,
};

pub const MZ_SHOW_ALL_OBJECTS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_all_objects_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_ALL_OBJECTS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_all_objects (schema_id)",
    is_retained_metrics_object: false,
};

pub const MZ_SHOW_INDEXES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_indexes_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_INDEXES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_indexes (schema_id)",
    is_retained_metrics_object: false,
};

pub const MZ_SHOW_COLUMNS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_columns_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_COLUMNS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_columns (id)",
    is_retained_metrics_object: false,
};

pub const MZ_SHOW_CLUSTERS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_clusters_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_CLUSTERS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_clusters (name)",
    is_retained_metrics_object: false,
};

pub const MZ_SHOW_CLUSTER_REPLICAS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_cluster_replicas_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_CLUSTER_REPLICAS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_cluster_replicas (cluster)",
    is_retained_metrics_object: false,
};

pub const MZ_SHOW_SECRETS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_secrets_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_SECRETS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_secrets (schema_id)",
    is_retained_metrics_object: false,
};

pub const MZ_CONSOLE_CLUSTER_UTILIZATION_OVERVIEW_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_console_cluster_utilization_overview_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_CONSOLE_CLUSTER_UTILIZATION_OVERVIEW_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_console_cluster_utilization_overview (cluster_id)",
    is_retained_metrics_object: false,
};

pub const MZ_CLUSTER_DEPLOYMENT_LINEAGE_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_cluster_deployment_lineage_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_CLUSTER_DEPLOYMENT_LINEAGE_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_cluster_deployment_lineage (cluster_id)",
    is_retained_metrics_object: false,
};

pub const MZ_SOURCE_STATUSES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_source_statuses_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SOURCE_STATUSES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_source_statuses (id)",
    is_retained_metrics_object: false,
};

pub const MZ_SINK_STATUSES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_sink_statuses_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SINK_STATUSES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_sink_statuses (id)",
    is_retained_metrics_object: false,
};

pub const MZ_SOURCE_STATUS_HISTORY_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_source_status_history_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SOURCE_STATUS_HISTORY_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_source_status_history (source_id)",
    is_retained_metrics_object: false,
};

pub const MZ_SINK_STATUS_HISTORY_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_sink_status_history_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SINK_STATUS_HISTORY_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_sink_status_history (sink_id)",
    is_retained_metrics_object: false,
};

// In both `mz_source_statistics` and `mz_sink_statistics` we cast the `SUM` of
// uint8's to `uint8` instead of leaving them as `numeric`. This is because we want to
// save index space, and we don't expect the sum to be > 2^63
// (even if a source with 2000 workers, that each produce 400 terabytes in a month ~ 2^61).
//
//
// These aggregations are just to make `GROUP BY` happy. Each id has a single row in the
// underlying relation.
//
// We append WITH_HISTORY because we want to build a separate view + index that doesn't
// retain history. This is because retaining its history causes MZ_SOURCE_STATISTICS_WITH_HISTORY_IND
// to hold all records/updates, which causes CPU and latency of querying it to spike.
pub static MZ_SOURCE_STATISTICS_WITH_HISTORY: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_source_statistics_with_history",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_SOURCE_STATISTICS_WITH_HISTORY_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("replica_id", SqlScalarType::String.nullable(true))
            .with_column("messages_received", SqlScalarType::UInt64.nullable(false))
            .with_column("bytes_received", SqlScalarType::UInt64.nullable(false))
            .with_column("updates_staged", SqlScalarType::UInt64.nullable(false))
            .with_column("updates_committed", SqlScalarType::UInt64.nullable(false))
            .with_column("records_indexed", SqlScalarType::UInt64.nullable(false))
            .with_column("bytes_indexed", SqlScalarType::UInt64.nullable(false))
            .with_column(
                "rehydration_latency",
                SqlScalarType::Interval.nullable(true),
            )
            .with_column(
                "snapshot_records_known",
                SqlScalarType::UInt64.nullable(true),
            )
            .with_column(
                "snapshot_records_staged",
                SqlScalarType::UInt64.nullable(true),
            )
            .with_column("snapshot_committed", SqlScalarType::Bool.nullable(false))
            .with_column("offset_known", SqlScalarType::UInt64.nullable(true))
            .with_column("offset_committed", SqlScalarType::UInt64.nullable(true))
            .with_key(vec![0, 1])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
WITH
    -- For each subsource, statistics are reported as its parent source
    subsource_to_parent AS
    (
        SELECT subsource.id AS id, parent.id AS report_id
        FROM mz_catalog.mz_sources AS subsource
            JOIN mz_internal.mz_object_dependencies AS dep ON subsource.id = dep.object_id
            JOIN mz_catalog.mz_sources AS parent ON parent.id = dep.referenced_object_id
        WHERE subsource.type = 'subsource'
    ),
    -- For each table from source, statistics are reported as its parent source
    table_to_parent AS
    (
        SELECT id, source_id AS report_id
        FROM mz_catalog.mz_tables
        WHERE source_id IS NOT NULL
    ),
    -- For each source and subsource, statistics are reported as itself
    source_refl AS
    (
        SELECT id, id AS report_id
        FROM mz_catalog.mz_sources
        WHERE type NOT IN ('progress', 'log')
    ),
    -- For each table from source, statistics are reported as itself
    table_refl AS
    (
        SELECT id, id AS report_id
        FROM mz_catalog.mz_tables
        WHERE source_id IS NOT NULL
    ),
    report_paths AS
    (
        SELECT id, report_id FROM subsource_to_parent
        UNION ALL SELECT id, report_id FROM table_to_parent
        UNION ALL SELECT id, report_id FROM source_refl
        UNION ALL SELECT id, report_id FROM table_refl
    )
SELECT
    report_paths.report_id AS id,
    replica_id,
    -- Counters
    SUM(messages_received)::uint8 AS messages_received,
    SUM(bytes_received)::uint8 AS bytes_received,
    SUM(updates_staged)::uint8 AS updates_staged,
    SUM(updates_committed)::uint8 AS updates_committed,
    -- Resetting Gauges
    SUM(records_indexed)::uint8 AS records_indexed,
    SUM(bytes_indexed)::uint8 AS bytes_indexed,
    -- Ensure we aggregate to NULL when not all workers are done rehydrating.
    CASE
        WHEN bool_or(rehydration_latency IS NULL) THEN NULL
        ELSE MAX(rehydration_latency)::interval
    END AS rehydration_latency,
    SUM(snapshot_records_known)::uint8 AS snapshot_records_known,
    SUM(snapshot_records_staged)::uint8 AS snapshot_records_staged,
    bool_and(snapshot_committed) as snapshot_committed,
    -- Gauges
    MAX(offset_known)::uint8 AS offset_known,
    MIN(offset_committed)::uint8 AS offset_committed
FROM mz_internal.mz_source_statistics_raw
    JOIN report_paths USING (id)
GROUP BY report_paths.report_id, replica_id",
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub const MZ_SOURCE_STATISTICS_WITH_HISTORY_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_source_statistics_with_history_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SOURCE_STATISTICS_WITH_HISTORY_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_source_statistics_with_history (id, replica_id)",
    is_retained_metrics_object: true,
};

// The non historical version of MZ_SOURCE_STATISTICS_WITH_HISTORY.
// Used to query MZ_SOURCE_STATISTICS at the current time.
pub static MZ_SOURCE_STATISTICS: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "mz_source_statistics",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::VIEW_MZ_SOURCE_STATISTICS_OID,
        // We need to add a redundant where clause for a new dataflow to be created.
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("replica_id", SqlScalarType::String.nullable(true))
            .with_column("messages_received", SqlScalarType::UInt64.nullable(false))
            .with_column("bytes_received", SqlScalarType::UInt64.nullable(false))
            .with_column("updates_staged", SqlScalarType::UInt64.nullable(false))
            .with_column("updates_committed", SqlScalarType::UInt64.nullable(false))
            .with_column("records_indexed", SqlScalarType::UInt64.nullable(false))
            .with_column("bytes_indexed", SqlScalarType::UInt64.nullable(false))
            .with_column(
                "rehydration_latency",
                SqlScalarType::Interval.nullable(true),
            )
            .with_column(
                "snapshot_records_known",
                SqlScalarType::UInt64.nullable(true),
            )
            .with_column(
                "snapshot_records_staged",
                SqlScalarType::UInt64.nullable(true),
            )
            .with_column("snapshot_committed", SqlScalarType::Bool.nullable(false))
            .with_column("offset_known", SqlScalarType::UInt64.nullable(true))
            .with_column("offset_committed", SqlScalarType::UInt64.nullable(true))
            .with_key(vec![0, 1])
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "id",
                "The ID of the source. Corresponds to `mz_catalog.mz_sources.id`.",
            ),
            (
                "replica_id",
                "The ID of a replica running the source. Corresponds to `mz_catalog.mz_cluster_replicas.id`.",
            ),
            (
                "messages_received",
                "The number of messages the source has received from the external system. Messages are counted in a source type-specific manner. Messages do not correspond directly to updates: some messages produce multiple updates, while other messages may be coalesced into a single update.",
            ),
            (
                "bytes_received",
                "The number of bytes the source has read from the external system. Bytes are counted in a source type-specific manner and may or may not include protocol overhead.",
            ),
            (
                "updates_staged",
                "The number of updates (insertions plus deletions) the source has written but not yet committed to the storage layer.",
            ),
            (
                "updates_committed",
                "The number of updates (insertions plus deletions) the source has committed to the storage layer.",
            ),
            (
                "records_indexed",
                "The number of individual records indexed in the source envelope state.",
            ),
            (
                "bytes_indexed",
                "The number of bytes stored in the source's internal index, if any.",
            ),
            (
                "rehydration_latency",
                "The amount of time it took for the source to rehydrate its internal index, if any, after the source last restarted.",
            ),
            (
                "snapshot_records_known",
                "The size of the source's snapshot, measured in number of records. See below to learn what constitutes a record.",
            ),
            (
                "snapshot_records_staged",
                "The number of records in the source's snapshot that Materialize has read. See below to learn what constitutes a record.",
            ),
            (
                "snapshot_committed",
                "Whether the source has committed the initial snapshot for a source.",
            ),
            (
                "offset_known",
                "The offset of the most recent data in the source's upstream service that Materialize knows about. See below to learn what constitutes an offset.",
            ),
            (
                "offset_committed",
                "The offset of the the data that Materialize has durably ingested. See below to learn what constitutes an offset.",
            ),
        ]),
        sql: "SELECT * FROM mz_internal.mz_source_statistics_with_history WHERE length(id) > 0",
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "source_statistics",
            description: "Aggregated source ingestion statistics",
            links: &const {
                [OntologyLink {
                    name: "statistics_of_source",
                    target: "source",
                    properties: LinkProperties::measures("id", "id", "ingestion_statistics"),
                }]
            },
            column_semantic_types: &const {
                [
                    ("id", SemanticType::CatalogItemId),
                    ("replica_id", SemanticType::ReplicaId),
                    ("messages_received", SemanticType::RecordCount),
                    ("bytes_received", SemanticType::ByteCount),
                    ("updates_staged", SemanticType::RecordCount),
                    ("updates_committed", SemanticType::RecordCount),
                    ("records_indexed", SemanticType::RecordCount),
                    ("bytes_indexed", SemanticType::ByteCount),
                    ("snapshot_records_known", SemanticType::RecordCount),
                    ("snapshot_records_staged", SemanticType::RecordCount),
                ]
            },
        }),
    }
});

pub const MZ_SOURCE_STATISTICS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_source_statistics_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SOURCE_STATISTICS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_source_statistics (id, replica_id)",
    is_retained_metrics_object: false,
};

pub static MZ_SINK_STATISTICS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_sink_statistics",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SINK_STATISTICS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("replica_id", SqlScalarType::String.nullable(true))
        .with_column("messages_staged", SqlScalarType::UInt64.nullable(false))
        .with_column("messages_committed", SqlScalarType::UInt64.nullable(false))
        .with_column("bytes_staged", SqlScalarType::UInt64.nullable(false))
        .with_column("bytes_committed", SqlScalarType::UInt64.nullable(false))
        .with_key(vec![0, 1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the sink. Corresponds to `mz_catalog.mz_sinks.id`.",
        ),
        (
            "replica_id",
            "The ID of a replica running the sink. Corresponds to `mz_catalog.mz_cluster_replicas.id`.",
        ),
        (
            "messages_staged",
            "The number of messages staged but possibly not committed to the sink.",
        ),
        (
            "messages_committed",
            "The number of messages committed to the sink.",
        ),
        (
            "bytes_staged",
            "The number of bytes staged but possibly not committed to the sink. This counts both keys and values, if applicable.",
        ),
        (
            "bytes_committed",
            "The number of bytes committed to the sink. This counts both keys and values, if applicable.",
        ),
    ]),
    sql: "
SELECT
    id,
    replica_id,
    SUM(messages_staged)::uint8 AS messages_staged,
    SUM(messages_committed)::uint8 AS messages_committed,
    SUM(bytes_staged)::uint8 AS bytes_staged,
    SUM(bytes_committed)::uint8 AS bytes_committed
FROM mz_internal.mz_sink_statistics_raw
GROUP BY id, replica_id",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "sink_statistics",
        description: "Aggregated sink export statistics",
        links: &const {
            [OntologyLink {
                name: "statistics_of_sink",
                target: "sink",
                properties: LinkProperties::measures("id", "id", "export_statistics"),
            }]
        },
        column_semantic_types: &const {
            [
                ("id", SemanticType::CatalogItemId),
                ("replica_id", SemanticType::ReplicaId),
                ("messages_staged", SemanticType::RecordCount),
                ("messages_committed", SemanticType::RecordCount),
                ("bytes_staged", SemanticType::ByteCount),
                ("bytes_committed", SemanticType::ByteCount),
            ]
        },
    }),
});

pub const MZ_SINK_STATISTICS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_sink_statistics_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SINK_STATISTICS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_sink_statistics (id, replica_id)",
    is_retained_metrics_object: true,
};

pub const MZ_CLUSTER_REPLICA_STATUSES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_cluster_replica_statuses_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_CLUSTER_REPLICA_STATUSES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_cluster_replica_statuses (replica_id)",
    is_retained_metrics_object: false,
};

pub const MZ_CLUSTER_REPLICA_STATUS_HISTORY_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_cluster_replica_status_history_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_CLUSTER_REPLICA_STATUS_HISTORY_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_cluster_replica_status_history (replica_id)",
    is_retained_metrics_object: false,
};

pub const MZ_CLUSTER_REPLICA_METRICS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_cluster_replica_metrics_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_CLUSTER_REPLICA_METRICS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_cluster_replica_metrics (replica_id)",
    is_retained_metrics_object: false,
};

pub const MZ_CLUSTER_REPLICA_METRICS_HISTORY_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_cluster_replica_metrics_history_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_CLUSTER_REPLICA_METRICS_HISTORY_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_cluster_replica_metrics_history (replica_id)",
    is_retained_metrics_object: false,
};

pub const MZ_CLUSTER_REPLICA_HISTORY_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_cluster_replica_history_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_CLUSTER_REPLICA_HISTORY_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_cluster_replica_history (dropped_at)",
    is_retained_metrics_object: true,
};

pub const MZ_CLUSTER_REPLICA_NAME_HISTORY_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_cluster_replica_name_history_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_CLUSTER_REPLICA_NAME_HISTORY_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_cluster_replica_name_history (id)",
    is_retained_metrics_object: false,
};

pub const MZ_OBJECT_LIFETIMES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_object_lifetimes_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_OBJECT_LIFETIMES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_object_lifetimes (id)",
    is_retained_metrics_object: false,
};

pub const MZ_OBJECT_HISTORY_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_object_history_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_OBJECT_HISTORY_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_object_history (id)",
    is_retained_metrics_object: false,
};

pub const MZ_OBJECT_DEPENDENCIES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_object_dependencies_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_OBJECT_DEPENDENCIES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_object_dependencies (object_id)",
    is_retained_metrics_object: true,
};

pub const MZ_COMPUTE_DEPENDENCIES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_compute_dependencies_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_COMPUTE_DEPENDENCIES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_compute_dependencies (dependency_id)",
    is_retained_metrics_object: false,
};

pub const MZ_OBJECT_TRANSITIVE_DEPENDENCIES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_object_transitive_dependencies_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_OBJECT_TRANSITIVE_DEPENDENCIES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_object_transitive_dependencies (object_id)",
    is_retained_metrics_object: false,
};

pub const MZ_FRONTIERS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_frontiers_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_FRONTIERS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_frontiers (object_id)",
    is_retained_metrics_object: false,
};

pub const MZ_WALLCLOCK_GLOBAL_LAG_RECENT_HISTORY_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_wallclock_global_lag_recent_history_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_WALLCLOCK_GLOBAL_LAG_RECENT_HISTORY_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_wallclock_global_lag_recent_history (object_id)",
    is_retained_metrics_object: false,
};

pub const MZ_RECENT_ACTIVITY_LOG_THINNED_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_recent_activity_log_thinned_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_RECENT_ACTIVITY_LOG_THINNED_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
-- sql_hash because we plan to join
-- this against mz_internal.mz_sql_text
ON mz_internal.mz_recent_activity_log_thinned (sql_hash)",
    is_retained_metrics_object: false,
};

pub const MZ_WEBHOOK_SOURCES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_webhook_sources_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_WEBHOOK_SOURCES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_webhook_sources (id)",
    is_retained_metrics_object: true,
};

pub const MZ_COMMENTS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_comments_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_COMMENTS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_comments (id)",
    is_retained_metrics_object: true,
};

pub static MZ_ANALYTICS: BuiltinConnection = BuiltinConnection {
    name: "mz_analytics",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::CONNECTION_MZ_ANALYTICS_OID,
    sql: "CREATE CONNECTION mz_internal.mz_analytics TO AWS (ASSUME ROLE ARN = '')",
    access: &[MzAclItem {
        grantee: MZ_SYSTEM_ROLE_ID,
        grantor: MZ_ANALYTICS_ROLE_ID,
        acl_mode: rbac::all_object_privileges(SystemObjectType::Object(ObjectType::Connection)),
    }],
    owner_id: &MZ_ANALYTICS_ROLE_ID,
    runtime_alterable: true,
};
