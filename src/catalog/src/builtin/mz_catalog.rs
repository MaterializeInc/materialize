// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Built-in catalog items for the `mz_catalog` schema.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use itertools::Itertools;
use mz_ore::collections::CollectionExt;
use mz_pgrepr::oid;
use mz_repr::namespaces::MZ_CATALOG_SCHEMA;
use mz_repr::{RelationDesc, SemanticType, SqlScalarType};
use mz_sql::ast::Statement;
use mz_sql::ast::display::{AstDisplay, escaped_string_literal};
use mz_sql::catalog::{
    CatalogType, CatalogTypeDetails, CatalogTypePgMetadata, NameReference, ObjectType,
};
use mz_sql::rbac;
use mz_sql::session::user::{MZ_SYSTEM_ROLE_ID, SUPPORT_USER_NAME, SYSTEM_USER_NAME};
use mz_storage_client::controller::IntrospectionType;

use super::{
    BuiltinIndex, BuiltinLog, BuiltinMaterializedView, BuiltinSource, BuiltinTable, BuiltinType,
    BuiltinView, Cardinality, LinkProperties, Ontology, OntologyLink, PUBLIC_SELECT,
};

pub const TYPE_LIST: BuiltinType<NameReference> = BuiltinType {
    name: "list",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_LIST_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
        pg_metadata: None,
    },
};

pub const TYPE_MAP: BuiltinType<NameReference> = BuiltinType {
    name: "map",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_MAP_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
        pg_metadata: None,
    },
};

pub const TYPE_ANYCOMPATIBLELIST: BuiltinType<NameReference> = BuiltinType {
    name: "anycompatiblelist",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_ANYCOMPATIBLELIST_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
        pg_metadata: None,
    },
};

pub const TYPE_ANYCOMPATIBLEMAP: BuiltinType<NameReference> = BuiltinType {
    name: "anycompatiblemap",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_ANYCOMPATIBLEMAP_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
        pg_metadata: None,
    },
};

pub const TYPE_UINT2: BuiltinType<NameReference> = BuiltinType {
    name: "uint2",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_UINT2_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::UInt16,
        array_id: None,
        pg_metadata: None,
    },
};

pub const TYPE_UINT2_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_uint2",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_UINT2_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_UINT2.name,
        },
        array_id: None,
        pg_metadata: None,
    },
};

pub const TYPE_UINT4: BuiltinType<NameReference> = BuiltinType {
    name: "uint4",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_UINT4_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::UInt32,
        array_id: None,
        pg_metadata: None,
    },
};

pub const TYPE_UINT4_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_uint4",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_UINT4_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_UINT4.name,
        },
        array_id: None,
        pg_metadata: None,
    },
};

pub const TYPE_UINT8: BuiltinType<NameReference> = BuiltinType {
    name: "uint8",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_UINT8_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::UInt64,
        array_id: None,
        pg_metadata: None,
    },
};

pub const TYPE_UINT8_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_uint8",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_UINT8_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_UINT8.name,
        },
        array_id: None,
        pg_metadata: None,
    },
};

pub const TYPE_MZ_TIMESTAMP: BuiltinType<NameReference> = BuiltinType {
    name: "mz_timestamp",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_MZ_TIMESTAMP_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::MzTimestamp,
        array_id: None,
        pg_metadata: None,
    },
};

pub const TYPE_MZ_TIMESTAMP_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_mz_timestamp",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_MZ_TIMESTAMP_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_MZ_TIMESTAMP.name,
        },
        array_id: None,
        pg_metadata: None,
    },
};

pub const TYPE_MZ_ACL_ITEM: BuiltinType<NameReference> = BuiltinType {
    name: "mz_aclitem",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_MZ_ACL_ITEM_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::MzAclItem,
        array_id: None,
        pg_metadata: None,
    },
};

pub const TYPE_MZ_ACL_ITEM_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_mz_aclitem",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_MZ_ACL_ITEM_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_MZ_ACL_ITEM.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub static MZ_ICEBERG_SINKS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_iceberg_sinks",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_ICEBERG_SINKS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("namespace", SqlScalarType::String.nullable(false))
        .with_column("table", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the sink."),
        (
            "namespace",
            "The namespace of the Iceberg table into which the sink is writing.",
        ),
        ("table", "The Iceberg table into which the sink is writing."),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "iceberg_sink",
        description: "Iceberg-specific sink configuration (namespace, table)",
        links: &const {
            [OntologyLink {
                name: "details_of",
                target: "sink",
                properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});

pub static MZ_KAFKA_SINKS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_kafka_sinks",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_KAFKA_SINKS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("topic", SqlScalarType::String.nullable(false))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the sink."),
        (
            "topic",
            "The name of the Kafka topic into which the sink is writing.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "kafka_sink",
        description: "Kafka-specific sink configuration (topic)",
        links: &const {
            [OntologyLink {
                name: "details_of",
                target: "sink",
                properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});
pub static MZ_KAFKA_CONNECTIONS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_kafka_connections",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_KAFKA_CONNECTIONS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column(
            "brokers",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false),
        )
        .with_column("sink_progress_topic", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the connection."),
        (
            "brokers",
            "The addresses of the Kafka brokers to connect to.",
        ),
        (
            "sink_progress_topic",
            "The name of the Kafka topic where any sinks associated with this connection will track their progress information and other metadata. The contents of this topic are unspecified.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "kafka_connection",
        description: "Kafka-specific connection configuration (brokers, progress topic)",
        links: &const {
            [OntologyLink {
                name: "details_of",
                target: "connection",
                properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});
pub static MZ_KAFKA_SOURCES: LazyLock<BuiltinMaterializedView> = LazyLock::new(|| {
    BuiltinMaterializedView {
        name: "mz_kafka_sources",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_KAFKA_SOURCES_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("group_id_prefix", SqlScalarType::String.nullable(false))
            .with_column("topic", SqlScalarType::String.nullable(false))
            .with_key(vec![0])
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "id",
                "The ID of the Kafka source. Corresponds to `mz_catalog.mz_sources.id`.",
            ),
            (
                "group_id_prefix",
                "The value of the `GROUP ID PREFIX` connection option.",
            ),
            (
                "topic",
                "The name of the Kafka topic the source is reading from.",
            ),
        ]),
        // NOTE: the `group_id_prefix` column is misnamed. It holds the
        // full computed `group.id` (see `KafkaSourceConnection::group_id`
        // / `KafkaConnection::id_base`), not just the user-supplied
        // prefix. The MV reproduces that behaviour verbatim.
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id,
    ASSERT NOT NULL group_id_prefix,
    ASSERT NOT NULL topic
) AS
SELECT
    mz_internal.parse_catalog_id(data->'key'->'gid') AS id,
    COALESCE(details->>'group_id_prefix', '')
        || 'materialize-' || mz_environment_id()
        || '-' || (details->>'connection_id')
        || '-' || mz_internal.parse_catalog_id(data->'value'->'global_id')
        AS group_id_prefix,
    details->>'topic' AS topic
FROM
    mz_internal.mz_catalog_raw,
    LATERAL (
        SELECT mz_internal.parse_catalog_create_sql(data->'value'->'definition'->'V1'->>'create_sql')
    ) AS l(parsed),
    LATERAL (
        SELECT mz_internal.parse_kafka_source_details(data->'value'->'definition'->'V1'->>'create_sql')
    ) AS d(details)
WHERE
    data->>'kind' = 'Item' AND
    parsed->>'source_type' = 'kafka'",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "kafka_source",
            description: "Kafka-specific source configuration (topic, group ID)",
            links: &const {
                [OntologyLink {
                    name: "details_of",
                    target: "source",
                    properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
                }]
            },
            column_semantic_types: &[("id", SemanticType::CatalogItemId)],
        }),
    }
});

pub static MZ_DATABASES: LazyLock<BuiltinMaterializedView> =
    LazyLock::new(|| BuiltinMaterializedView {
        name: "mz_databases",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_DATABASES_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("owner_id", SqlScalarType::String.nullable(false))
            .with_column(
                "privileges",
                SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
            )
            .with_key(vec![0])
            .with_key(vec![1])
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "Materialize's unique ID for the database."),
            ("oid", "A PostgreSQL-compatible OID for the database."),
            ("name", "The name of the database."),
            (
                "owner_id",
                "The role ID of the owner of the database. Corresponds to `mz_roles.id`.",
            ),
            ("privileges", "The privileges belonging to the database."),
        ]),
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id,
    ASSERT NOT NULL oid,
    ASSERT NOT NULL name,
    ASSERT NOT NULL owner_id,
    ASSERT NOT NULL privileges
) AS
SELECT
    mz_internal.parse_catalog_id(data->'key'->'id') AS id,
    (data->'value'->>'oid')::oid AS oid,
    data->'value'->>'name' AS name,
    mz_internal.parse_catalog_id(data->'value'->'owner_id') AS owner_id,
    mz_internal.parse_catalog_privileges(data->'value'->'privileges') AS privileges
FROM mz_internal.mz_catalog_raw
WHERE data->>'kind' = 'Database'",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "database",
            description: "A top-level namespace that contains schemas",
            links: &const {
                [OntologyLink {
                    name: "owned_by",
                    target: "role",
                    properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
                }]
            },
            column_semantic_types: &const {
                [
                    ("id", SemanticType::DatabaseId),
                    ("oid", SemanticType::OID),
                    ("owner_id", SemanticType::RoleId),
                ]
            },
        }),
    });

pub static MZ_SCHEMAS: LazyLock<BuiltinMaterializedView> =
    LazyLock::new(|| BuiltinMaterializedView {
        name: "mz_schemas",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_SCHEMAS_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("database_id", SqlScalarType::String.nullable(true))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("owner_id", SqlScalarType::String.nullable(false))
            .with_column(
                "privileges",
                SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
            )
            .with_key(vec![0])
            .with_key(vec![1])
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "Materialize's unique ID for the schema."),
            ("oid", "A PostgreSQL-compatible oid for the schema."),
            (
                "database_id",
                "The ID of the database containing the schema. Corresponds to `mz_databases.id`.",
            ),
            ("name", "The name of the schema."),
            (
                "owner_id",
                "The role ID of the owner of the schema. Corresponds to `mz_roles.id`.",
            ),
            ("privileges", "The privileges belonging to the schema."),
        ]),
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id,
    ASSERT NOT NULL oid,
    ASSERT NOT NULL name,
    ASSERT NOT NULL owner_id,
    ASSERT NOT NULL privileges
) AS
SELECT
    mz_internal.parse_catalog_id(data->'key'->'id') AS id,
    (data->'value'->>'oid')::oid AS oid,
    CASE WHEN data->'value'->'database_id' != 'null'
         THEN mz_internal.parse_catalog_id(data->'value'->'database_id')
    END AS database_id,
    data->'value'->>'name' AS name,
    mz_internal.parse_catalog_id(data->'value'->'owner_id') AS owner_id,
    mz_internal.parse_catalog_privileges(data->'value'->'privileges') AS privileges
FROM mz_internal.mz_catalog_raw
WHERE data->>'kind' = 'Schema'",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "schema",
            description: "A namespace within a database that contains objects",
            links: &const {
                [
                    OntologyLink {
                        name: "in_database",
                        target: "database",
                        properties: LinkProperties::fk_nullable(
                            "database_id",
                            "id",
                            Cardinality::ManyToOne,
                        ),
                    },
                    OntologyLink {
                        name: "owned_by",
                        target: "role",
                        properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
                    },
                ]
            },
            column_semantic_types: &const {
                [
                    ("id", SemanticType::SchemaId),
                    ("oid", SemanticType::OID),
                    ("database_id", SemanticType::DatabaseId),
                    ("owner_id", SemanticType::RoleId),
                ]
            },
        }),
    });

pub static MZ_COLUMNS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_columns",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_COLUMNS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false)) // not a key
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("position", SqlScalarType::UInt64.nullable(false))
        .with_column("nullable", SqlScalarType::Bool.nullable(false))
        .with_column("type", SqlScalarType::String.nullable(false))
        .with_column("default", SqlScalarType::String.nullable(true))
        .with_column("type_oid", SqlScalarType::Oid.nullable(false))
        .with_column("type_mod", SqlScalarType::Int32.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The unique ID of the table, source, or view containing the column.",
        ),
        ("name", "The name of the column."),
        (
            "position",
            "The 1-indexed position of the column in its containing table, source, or view.",
        ),
        ("nullable", "Can the column contain a `NULL` value?"),
        ("type", "The data type of the column."),
        ("default", "The default expression of the column."),
        (
            "type_oid",
            "The OID of the type of the column (references `mz_types`).",
        ),
        ("type_mod", "The packed type identifier of the column."),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "column",
        description: "A column of a relation, with its name, position, type, and nullability",
        links: &const {
            [OntologyLink {
                name: "belongs_to_relation",
                target: "object",
                properties: LinkProperties::ForeignKey {
                    source_column: "id",
                    target_column: "id",
                    cardinality: Cardinality::ManyToOne,
                    source_id_type: None,
                    requires_mapping: None,
                    nullable: false,
                    note: Some("id in mz_columns is the relation ID, not a unique column ID"),
                    extra_key_columns: None,
                },
            }]
        },
        column_semantic_types: &const {
            [
                ("id", SemanticType::CatalogItemId),
                ("type_oid", SemanticType::OID),
            ]
        },
    }),
});
// mz_indexes is generated dynamically in BUILTINS_STATIC via mz_catalog::make_mz_indexes()

/// Asserts that `name` is safe to embed unquoted inside a `'...'`-quoted SQL literal
/// or inside a `"..."`-quoted SQL identifier. Builtin index/log/object names are
/// concatenated into SQL fragments below, so a quote or backslash would produce
/// malformed SQL. Builtin names should always be plain ASCII identifiers.
fn assert_safe_builtin_name(name: &str, kind: &str) {
    assert!(
        !name.contains('\'') && !name.contains('"') && !name.contains('\\'),
        "builtin {kind} name {name:?} contains an unsupported character; \
         mz_indexes reconstructs SQL via string concatenation and assumes \
         names contain no quotes or backslashes"
    );
}

/// User-created indexes, sourced from `mz_catalog_raw` `Item` entries with
/// `parse_catalog_create_sql(...)` yielding `type = 'index'`.
const USER_INDEXES_CTE: &str = "\
    user_indexes AS (
        SELECT
            mz_internal.parse_catalog_id(data->'key'->'gid') AS id,
            (data->'value'->>'oid')::oid AS oid,
            data->'value'->>'name' AS name,
            parsed->>'on_id' AS on_id,
            parsed->>'cluster_id' AS cluster_id,
            mz_internal.parse_catalog_id(data->'value'->'owner_id') AS owner_id,
            data->'value'->'definition'->'V1'->>'create_sql' AS create_sql,
            mz_internal.redact_sql(data->'value'->'definition'->'V1'->>'create_sql') AS redacted_create_sql
        FROM
            mz_internal.mz_catalog_raw
            CROSS JOIN LATERAL (
                SELECT mz_internal.parse_catalog_create_sql(data->'value'->'definition'->'V1'->>'create_sql')
            ) AS l(parsed)
        WHERE
            data->>'kind' = 'Item' AND
            parsed->>'type' = 'index'
    )";

/// Resolves the catalog id of the `mz_catalog_server` cluster at query time so
/// the MV doesn't hardcode a system id literal.
const CATALOG_SERVER_CLUSTER_CTE: &str = "\
    catalog_server_cluster AS (
        SELECT mz_internal.parse_catalog_id(data->'key'->'id') AS id
        FROM mz_internal.mz_catalog_raw
        WHERE data->>'kind' = 'Cluster' AND data->'value'->>'name' = 'mz_catalog_server'
    )";

/// Helper CTEs over `GidMapping` rows used to look up the system id of a
/// builtin index by name and of the relation it indexes by (schema, name).
const GID_MAPPING_CTES: &str = "\
    builtin_index_gid_mappings AS (
        SELECT
            's' || (data->'value'->>'catalog_id') AS id,
            data->'key'->>'object_name' AS name
        FROM mz_internal.mz_catalog_raw
        WHERE
            data->>'kind' = 'GidMapping' AND
            data->'key'->>'object_type' = '6'
    ),
    on_gid_mappings AS (
        SELECT
            's' || (data->'value'->>'catalog_id') AS id,
            data->'key'->>'schema_name' AS schema_name,
            data->'key'->>'object_name' AS object_name
        FROM mz_internal.mz_catalog_raw
        WHERE data->>'kind' = 'GidMapping'
    )";

/// Generate the `mz_catalog.mz_indexes` builtin materialized view with builtin
/// index entries inlined as VALUES clauses.
///
/// Inlining the values means the MV's SQL fingerprint changes whenever a builtin
/// index or log is added or removed, which forces a `MigrationStep::replacement`
/// for `mz_indexes` and guarantees stale data is never silently served.
///
/// Includes user-created indexes (from `mz_catalog_raw` `Item` entries),
/// system builtin indexes (from `GidMapping` with object_type=6), and
/// introspection source indexes (from `IntrospectionSourceIndex` entries).
pub(super) fn make_mz_indexes(
    builtin_index_iter: impl Iterator<Item = &'static BuiltinIndex>,
    builtin_log_iter: impl Iterator<Item = &'static BuiltinLog>,
) -> BuiltinMaterializedView {
    let builtin_index_values = builtin_index_iter
        .map(|index| {
            assert_safe_builtin_name(index.name, "index");
            let create_sql_str = index.create_sql();
            let stmt = mz_sql::parse::parse(&create_sql_str)
                .unwrap_or_else(|e| panic!("invalid sql for builtin index {}: {e}", index.name))
                .into_element()
                .ast;
            let Statement::CreateIndex(idx_stmt) = stmt else {
                panic!("expected CreateIndex for builtin index {}", index.name);
            };
            let mz_sql::ast::RawItemName::Name(on_name) = idx_stmt.on_name else {
                panic!("expected Name for on_name in builtin index {}", index.name);
            };
            assert_eq!(
                on_name.0.len(),
                2,
                "expected schema.name format for on_name in builtin index {}",
                index.name
            );
            let on_schema = on_name.0[0].as_str();
            let on_name_str = on_name.0[1].as_str();
            assert_safe_builtin_name(on_schema, "index `on` schema");
            assert_safe_builtin_name(on_name_str, "index `on` object");
            let key_exprs = idx_stmt
                .key_parts
                .unwrap_or_else(|| {
                    panic!("builtin index {} must have explicit key parts", index.name)
                })
                .iter()
                .map(|e| e.to_ast_string_stable())
                .join(", ");
            // Unlike the identifier names above, key expressions are arbitrary
            // SQL (column refs, casts, string literals) that can legitimately
            // contain single quotes — so escape them rather than asserting
            // them away with `assert_safe_builtin_name`.
            let key_exprs_escaped = escaped_string_literal(&key_exprs);
            format!(
                "({}::oid, '{}', '{}', '{}', {key_exprs_escaped})",
                index.oid, index.name, on_schema, on_name_str
            )
        })
        .join(",");

    let log_col_values = builtin_log_iter
        .map(|log| {
            assert_safe_builtin_name(log.name, "log");
            let desc = log.variant.desc();
            let index_by = log.variant.index_by();
            let col_list = index_by
                .iter()
                .map(|&i| match desc.get_unambiguous_name(i) {
                    Some(name) => {
                        assert_safe_builtin_name(name, "log column");
                        format!("\"{}\"", name)
                    }
                    None => (i + 1).to_string(),
                })
                .join(", ");
            format!("('{}', '{}')", log.name, col_list)
        })
        .join(",");

    // Reconstructs `CREATE INDEX ... IN CLUSTER [<id>] ON [<id> AS "schema"."name"] (<keys>)`
    // from the (oid, name, on_schema, on_name, key_exprs) VALUES rows joined to
    // `GidMapping` lookups and the `mz_catalog_server` cluster id.
    let builtin_indexes_cte = format!("\
    builtin_indexes AS (
        SELECT *, mz_internal.redact_sql(create_sql) AS redacted_create_sql
        FROM (
            SELECT
                bigm.id AS id,
                biv.oid AS oid,
                biv.name AS name,
                om.id AS on_id,
                csc.id AS cluster_id,
                '{MZ_SYSTEM_ROLE_ID}' AS owner_id,
                'CREATE INDEX \"' || biv.name || '\" IN CLUSTER [' || csc.id || '] ON [' || om.id || ' AS \"' || biv.on_schema || '\".\"' || biv.on_name || '\"] (' || biv.key_exprs || ')' AS create_sql
            FROM (VALUES {builtin_index_values}) AS biv(oid, name, on_schema, on_name, key_exprs)
            JOIN builtin_index_gid_mappings bigm ON bigm.name = biv.name
            JOIN on_gid_mappings om ON om.schema_name = biv.on_schema AND om.object_name = biv.on_name
            CROSS JOIN catalog_server_cluster csc
        ) AS t
    )");

    let introspection_source_indexes_cte = format!("\
    introspection_source_indexes AS (
        SELECT *, mz_internal.redact_sql(create_sql) AS redacted_create_sql
        FROM (
            SELECT
                'si' || (isi.data->'value'->>'catalog_id') AS id,
                (isi.data->'value'->>'oid')::oid AS oid,
                idx_name || '_' || cluster_id || '_primary_idx' AS name,
                's' || (gm.data->'value'->>'catalog_id') AS on_id,
                cluster_id,
                '{MZ_SYSTEM_ROLE_ID}' AS owner_id,
                'CREATE INDEX \"' || idx_name || '_' || cluster_id || '_primary_idx\" IN CLUSTER [' || cluster_id || '] ON \"mz_introspection\".\"' || idx_name || '\" (' || lc.col_list || ')' AS create_sql
            FROM mz_internal.mz_catalog_raw AS isi
            CROSS JOIN LATERAL (
                SELECT isi.data->'key'->>'name', mz_internal.parse_catalog_id(isi.data->'key'->'cluster_id')
            ) AS l(idx_name, cluster_id)
            JOIN mz_internal.mz_catalog_raw AS gm ON
                gm.data->>'kind' = 'GidMapping' AND
                gm.data->'key'->>'object_type' = '2' AND
                gm.data->'key'->>'schema_name' = 'mz_introspection' AND
                gm.data->'key'->>'object_name' = idx_name
            JOIN (VALUES {log_col_values}) AS lc(log_name, col_list) ON lc.log_name = idx_name
            WHERE isi.data->>'kind' = 'ClusterIntrospectionSourceIndex'
        ) AS t
    )");

    let sql = format!(
        "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id,
    ASSERT NOT NULL oid,
    ASSERT NOT NULL name,
    ASSERT NOT NULL on_id,
    ASSERT NOT NULL cluster_id,
    ASSERT NOT NULL owner_id,
    ASSERT NOT NULL create_sql,
    ASSERT NOT NULL redacted_create_sql
) AS
WITH
{USER_INDEXES_CTE},
{CATALOG_SERVER_CLUSTER_CTE},
{GID_MAPPING_CTES},
{builtin_indexes_cte},
{introspection_source_indexes_cte}
SELECT * FROM user_indexes
UNION ALL
SELECT * FROM builtin_indexes
UNION ALL
SELECT * FROM introspection_source_indexes
"
    );

    BuiltinMaterializedView {
        name: "mz_indexes",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_INDEXES_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("on_id", SqlScalarType::String.nullable(false))
            .with_column("cluster_id", SqlScalarType::String.nullable(false))
            .with_column("owner_id", SqlScalarType::String.nullable(false))
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .with_column("redacted_create_sql", SqlScalarType::String.nullable(false))
            .with_key(vec![0])
            .with_key(vec![1])
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "Materialize's unique ID for the index."),
            ("oid", "A PostgreSQL-compatible OID for the index."),
            ("name", "The name of the index."),
            (
                "on_id",
                "The ID of the relation on which the index is built.",
            ),
            (
                "cluster_id",
                "The ID of the cluster in which the index is built.",
            ),
            (
                "owner_id",
                "The role ID of the owner of the index. Corresponds to `mz_roles.id`.",
            ),
            ("create_sql", "The `CREATE` SQL statement for the index."),
            (
                "redacted_create_sql",
                "The redacted `CREATE` SQL statement for the index.",
            ),
        ]),
        sql: Box::leak(sql.into_boxed_str()),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "index",
            description: "An in-memory index on a relation for fast lookups",
            links: &const {
                [
                    OntologyLink {
                        name: "owned_by",
                        target: "role",
                        properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "runs_on_cluster",
                        target: "cluster",
                        properties: LinkProperties::fk("cluster_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "indexes_relation",
                        target: "relation",
                        properties: LinkProperties::fk("on_id", "id", Cardinality::ManyToOne),
                    },
                ]
            },
            column_semantic_types: &const {
                [
                    ("id", SemanticType::CatalogItemId),
                    ("oid", SemanticType::OID),
                    ("on_id", SemanticType::CatalogItemId),
                    ("cluster_id", SemanticType::ClusterId),
                    ("owner_id", SemanticType::RoleId),
                    ("create_sql", SemanticType::SqlDefinition),
                    ("redacted_create_sql", SemanticType::RedactedSqlDefinition),
                ]
            },
        }),
    }
}
pub static MZ_INDEX_COLUMNS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_index_columns",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_INDEX_COLUMNS_OID,
    desc: RelationDesc::builder()
        .with_column("index_id", SqlScalarType::String.nullable(false))
        .with_column("index_position", SqlScalarType::UInt64.nullable(false))
        .with_column("on_position", SqlScalarType::UInt64.nullable(true))
        .with_column("on_expression", SqlScalarType::String.nullable(true))
        .with_column("nullable", SqlScalarType::Bool.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "index_id",
            "The ID of the index which contains this column. Corresponds to `mz_indexes.id`.",
        ),
        (
            "index_position",
            "The 1-indexed position of this column within the index. (The order of columns in an index does not necessarily match the order of columns in the relation on which the index is built.)",
        ),
        (
            "on_position",
            "If not `NULL`, specifies the 1-indexed position of a column in the relation on which this index is built that determines the value of this index column.",
        ),
        (
            "on_expression",
            "If not `NULL`, specifies a SQL expression that is evaluated to compute the value of this index column. The expression may contain references to any of the columns of the relation.",
        ),
        (
            "nullable",
            "Can this column of the index evaluate to `NULL`?",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "index_column",
        description: "A column or expression in an index, with its position",
        links: &const {
            [OntologyLink {
                name: "belongs_to_index",
                target: "index",
                properties: LinkProperties::fk("index_id", "id", Cardinality::ManyToOne),
            }]
        },
        column_semantic_types: &[("index_id", SemanticType::CatalogItemId)],
    }),
});
pub static MZ_TABLES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_tables",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_TABLES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("schema_id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("owner_id", SqlScalarType::String.nullable(false))
        .with_column(
            "privileges",
            SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
        )
        .with_column("create_sql", SqlScalarType::String.nullable(true))
        .with_column("redacted_create_sql", SqlScalarType::String.nullable(true))
        .with_column("source_id", SqlScalarType::String.nullable(true))
        .with_key(vec![0])
        .with_key(vec![1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "Materialize's unique ID for the table."),
        ("oid", "A PostgreSQL-compatible OID for the table."),
        (
            "schema_id",
            "The ID of the schema to which the table belongs. Corresponds to `mz_schemas.id`.",
        ),
        ("name", "The name of the table."),
        (
            "owner_id",
            "The role ID of the owner of the table. Corresponds to `mz_roles.id`.",
        ),
        ("privileges", "The privileges belonging to the table."),
        ("create_sql", "The `CREATE` SQL statement for the table."),
        (
            "redacted_create_sql",
            "The redacted `CREATE` SQL statement for the table.",
        ),
        (
            "source_id",
            "The ID of the source associated with the table, if any. Corresponds to `mz_sources.id`.",
        ),
    ]),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "table",
        description: "A user-writable table that can be inserted into and updated",
        links: &const {
            [
                OntologyLink {
                    name: "in_schema",
                    target: "schema",
                    properties: LinkProperties::fk("schema_id", "id", Cardinality::ManyToOne),
                },
                OntologyLink {
                    name: "owned_by",
                    target: "role",
                    properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
                },
                OntologyLink {
                    name: "created_by_source",
                    target: "source",
                    properties: LinkProperties::fk_nullable(
                        "source_id",
                        "id",
                        Cardinality::ManyToOne,
                    ),
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("id", SemanticType::CatalogItemId),
                ("oid", SemanticType::OID),
                ("schema_id", SemanticType::SchemaId),
                ("owner_id", SemanticType::RoleId),
                ("create_sql", SemanticType::SqlDefinition),
                ("redacted_create_sql", SemanticType::RedactedSqlDefinition),
                ("source_id", SemanticType::CatalogItemId),
            ]
        },
    }),
});

pub static MZ_CONNECTIONS: LazyLock<BuiltinMaterializedView> = LazyLock::new(|| {
    BuiltinMaterializedView {
        name: "mz_connections",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_CONNECTIONS_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_id", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("type", SqlScalarType::String.nullable(false))
            .with_column("owner_id", SqlScalarType::String.nullable(false))
            .with_column(
                "privileges",
                SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
            )
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .with_column("redacted_create_sql", SqlScalarType::String.nullable(false))
            .with_key(vec![0])
            .with_key(vec![1])
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "The unique ID of the connection."),
            ("oid", "A PostgreSQL-compatible OID for the connection."),
            (
                "schema_id",
                "The ID of the schema to which the connection belongs. Corresponds to `mz_schemas.id`.",
            ),
            ("name", "The name of the connection."),
            (
                "type",
                "The type of the connection: `confluent-schema-registry`, `kafka`, `postgres`, or `ssh-tunnel`.",
            ),
            (
                "owner_id",
                "The role ID of the owner of the connection. Corresponds to `mz_roles.id`.",
            ),
            ("privileges", "The privileges belonging to the connection."),
            (
                "create_sql",
                "The `CREATE` SQL statement for the connection.",
            ),
            (
                "redacted_create_sql",
                "The redacted `CREATE` SQL statement for the connection.",
            ),
        ]),
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id,
    ASSERT NOT NULL oid,
    ASSERT NOT NULL schema_id,
    ASSERT NOT NULL name,
    ASSERT NOT NULL type,
    ASSERT NOT NULL owner_id,
    ASSERT NOT NULL privileges,
    ASSERT NOT NULL create_sql,
    ASSERT NOT NULL redacted_create_sql
) AS
SELECT
    mz_internal.parse_catalog_id(data->'key'->'gid') AS id,
    (data->'value'->>'oid')::oid AS oid,
    mz_internal.parse_catalog_id(data->'value'->'schema_id') AS schema_id,
    data->'value'->>'name' AS name,
    mz_internal.parse_catalog_create_sql(data->'value'->'definition'->'V1'->>'create_sql')->>'connection_type' AS type,
    mz_internal.parse_catalog_id(data->'value'->'owner_id') AS owner_id,
    mz_internal.parse_catalog_privileges(data->'value'->'privileges') AS privileges,
    data->'value'->'definition'->'V1'->>'create_sql' AS create_sql,
    mz_internal.redact_sql(data->'value'->'definition'->'V1'->>'create_sql') AS redacted_create_sql
FROM mz_internal.mz_catalog_raw
WHERE
    data->>'kind' = 'Item' AND
    mz_internal.parse_catalog_create_sql(data->'value'->'definition'->'V1'->>'create_sql')->>'type' = 'connection'",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "connection",
            description: "A reusable connection configuration to an external system",
            links: &const { [
                OntologyLink {
                    name: "in_schema",
                    target: "schema",
                    properties: LinkProperties::fk("schema_id", "id", Cardinality::ManyToOne),
                },
                OntologyLink {
                    name: "owned_by",
                    target: "role",
                    properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
                },
            ] },
            column_semantic_types: &const {[("id", SemanticType::CatalogItemId), ("oid", SemanticType::OID), ("schema_id", SemanticType::SchemaId), ("type", SemanticType::ConnectionType), ("owner_id", SemanticType::RoleId), ("create_sql", SemanticType::SqlDefinition), ("redacted_create_sql", SemanticType::RedactedSqlDefinition)]},
        }),
    }
});

pub static MZ_SSH_TUNNEL_CONNECTIONS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_ssh_tunnel_connections",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_SSH_TUNNEL_CONNECTIONS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("public_key_1", SqlScalarType::String.nullable(false))
        .with_column("public_key_2", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the connection."),
        (
            "public_key_1",
            "The first public key associated with the SSH tunnel.",
        ),
        (
            "public_key_2",
            "The second public key associated with the SSH tunnel.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "ssh_tunnel_connection",
        description: "SSH tunnel connection with public keys",
        links: &const {
            [OntologyLink {
                name: "details_of",
                target: "connection",
                properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
            }]
        },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});
// mz_sources is generated dynamically in BUILTINS_STATIC via builtin::make_mz_sources()
// with builtin source/log entries inlined as VALUES. See builtin/builtin.rs.
pub static MZ_SINKS: LazyLock<BuiltinTable> = LazyLock::new(|| {
    BuiltinTable {
        name: "mz_sinks",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::TABLE_MZ_SINKS_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_id", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("type", SqlScalarType::String.nullable(false))
            .with_column("connection_id", SqlScalarType::String.nullable(true))
            .with_column("size", SqlScalarType::String.nullable(true))
            .with_column("envelope_type", SqlScalarType::String.nullable(true))
            // This `format` column is deprecated and replaced by the `key_format` and `value_format` columns
            // below. This should be removed in the future.
            .with_column("format", SqlScalarType::String.nullable(true))
            .with_column("key_format", SqlScalarType::String.nullable(true))
            .with_column("value_format", SqlScalarType::String.nullable(true))
            .with_column("cluster_id", SqlScalarType::String.nullable(false))
            .with_column("owner_id", SqlScalarType::String.nullable(false))
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .with_column("redacted_create_sql", SqlScalarType::String.nullable(false))
            .with_key(vec![0])
            .with_key(vec![1])
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "Materialize's unique ID for the sink."),
            ("oid", "A PostgreSQL-compatible OID for the sink."),
            (
                "schema_id",
                "The ID of the schema to which the sink belongs. Corresponds to `mz_schemas.id`.",
            ),
            ("name", "The name of the sink."),
            ("type", "The type of the sink: `kafka`."),
            (
                "connection_id",
                "The ID of the connection associated with the sink, if any. Corresponds to `mz_connections.id`.",
            ),
            ("size", "The size of the sink."),
            (
                "envelope_type",
                "The envelope of the sink: `upsert`, or `debezium`.",
            ),
            (
                "format",
                "*Deprecated* The format of the Kafka messages produced by the sink: `avro`, `json`, `text`, or `bytes`.",
            ),
            (
                "key_format",
                "The format of the Kafka message key for messages produced by the sink: `avro`, `json`, `bytes`, `text`, or `NULL`.",
            ),
            (
                "value_format",
                "The format of the Kafka message value for messages produced by the sink: `avro`, `json`, `text`, or `bytes`.",
            ),
            (
                "cluster_id",
                "The ID of the cluster maintaining the sink. Corresponds to `mz_clusters.id`.",
            ),
            (
                "owner_id",
                "The role ID of the owner of the sink. Corresponds to `mz_roles.id`.",
            ),
            ("create_sql", "The `CREATE` SQL statement for the sink."),
            (
                "redacted_create_sql",
                "The redacted `CREATE` SQL statement for the sink.",
            ),
        ]),
        is_retained_metrics_object: true,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "sink",
            description: "An export of data from Materialize to an external system",
            links: &const {
                [
                    OntologyLink {
                        name: "in_schema",
                        target: "schema",
                        properties: LinkProperties::fk("schema_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "owned_by",
                        target: "role",
                        properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "runs_on_cluster",
                        target: "cluster",
                        properties: LinkProperties::fk("cluster_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "uses_connection",
                        target: "connection",
                        properties: LinkProperties::fk_nullable(
                            "connection_id",
                            "id",
                            Cardinality::ManyToOne,
                        ),
                    },
                ]
            },
            column_semantic_types: &const {
                [
                    ("id", SemanticType::CatalogItemId),
                    ("oid", SemanticType::OID),
                    ("schema_id", SemanticType::SchemaId),
                    ("connection_id", SemanticType::CatalogItemId),
                    ("cluster_id", SemanticType::ClusterId),
                    ("owner_id", SemanticType::RoleId),
                    ("create_sql", SemanticType::SqlDefinition),
                    ("redacted_create_sql", SemanticType::RedactedSqlDefinition),
                ]
            },
        }),
    }
});
pub static MZ_VIEWS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_views",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_VIEWS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("schema_id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("definition", SqlScalarType::String.nullable(false))
        .with_column("owner_id", SqlScalarType::String.nullable(false))
        .with_column(
            "privileges",
            SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
        )
        .with_column("create_sql", SqlScalarType::String.nullable(false))
        .with_column("redacted_create_sql", SqlScalarType::String.nullable(false))
        .with_key(vec![0])
        .with_key(vec![1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "Materialize's unique ID for the view."),
        ("oid", "A PostgreSQL-compatible OID for the view."),
        (
            "schema_id",
            "The ID of the schema to which the view belongs. Corresponds to `mz_schemas.id`.",
        ),
        ("name", "The name of the view."),
        ("definition", "The view definition (a `SELECT` query)."),
        (
            "owner_id",
            "The role ID of the owner of the view. Corresponds to `mz_roles.id`.",
        ),
        ("privileges", "The privileges belonging to the view."),
        ("create_sql", "The `CREATE` SQL statement for the view."),
        (
            "redacted_create_sql",
            "The redacted `CREATE` SQL statement for the view.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "view",
        description: "A non-materialized view defined by a SQL query",
        links: &const {
            [
                OntologyLink {
                    name: "in_schema",
                    target: "schema",
                    properties: LinkProperties::fk("schema_id", "id", Cardinality::ManyToOne),
                },
                OntologyLink {
                    name: "owned_by",
                    target: "role",
                    properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("id", SemanticType::CatalogItemId),
                ("oid", SemanticType::OID),
                ("schema_id", SemanticType::SchemaId),
                ("definition", SemanticType::SqlDefinition),
                ("owner_id", SemanticType::RoleId),
                ("create_sql", SemanticType::SqlDefinition),
                ("redacted_create_sql", SemanticType::RedactedSqlDefinition),
            ]
        },
    }),
});

pub static MZ_MATERIALIZED_VIEWS: LazyLock<BuiltinMaterializedView> = LazyLock::new(|| {
    BuiltinMaterializedView {
        name: "mz_materialized_views",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_MATERIALIZED_VIEWS_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_id", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("cluster_id", SqlScalarType::String.nullable(false))
            .with_column("definition", SqlScalarType::String.nullable(false))
            .with_column("owner_id", SqlScalarType::String.nullable(false))
            .with_column(
                "privileges",
                SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
            )
            .with_column("create_sql", SqlScalarType::String.nullable(false))
            .with_column("redacted_create_sql", SqlScalarType::String.nullable(false))
            .with_key(vec![0])
            .with_key(vec![1])
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "Materialize's unique ID for the materialized view."),
            (
                "oid",
                "A PostgreSQL-compatible OID for the materialized view.",
            ),
            (
                "schema_id",
                "The ID of the schema to which the materialized view belongs. Corresponds to `mz_schemas.id`.",
            ),
            ("name", "The name of the materialized view."),
            (
                "cluster_id",
                "The ID of the cluster maintaining the materialized view. Corresponds to `mz_clusters.id`.",
            ),
            (
                "definition",
                "The materialized view definition (a `SELECT` query).",
            ),
            (
                "owner_id",
                "The role ID of the owner of the materialized view. Corresponds to `mz_roles.id`.",
            ),
            (
                "privileges",
                "The privileges belonging to the materialized view.",
            ),
            (
                "create_sql",
                "The `CREATE` SQL statement for the materialized view.",
            ),
            (
                "redacted_create_sql",
                "The redacted `CREATE` SQL statement for the materialized view.",
            ),
        ]),
        sql: Box::leak(format!("
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id,
    ASSERT NOT NULL oid,
    ASSERT NOT NULL schema_id,
    ASSERT NOT NULL name,
    ASSERT NOT NULL cluster_id,
    ASSERT NOT NULL definition,
    ASSERT NOT NULL owner_id,
    ASSERT NOT NULL privileges,
    ASSERT NOT NULL create_sql,
    ASSERT NOT NULL redacted_create_sql
) AS
WITH
    user_mvs AS (
        SELECT
            mz_internal.parse_catalog_id(data->'key'->'gid') AS id,
            (data->'value'->>'oid')::oid AS oid,
            mz_internal.parse_catalog_id(data->'value'->'schema_id') AS schema_id,
            data->'value'->>'name' AS name,
            mz_internal.parse_catalog_create_sql(data->'value'->'definition'->'V1'->>'create_sql')->>'cluster_id' AS cluster_id,
            mz_internal.parse_catalog_create_sql(data->'value'->'definition'->'V1'->>'create_sql')->>'definition' AS definition,
            mz_internal.parse_catalog_id(data->'value'->'owner_id') AS owner_id,
            mz_internal.parse_catalog_privileges(data->'value'->'privileges') AS privileges,
            data->'value'->'definition'->'V1'->>'create_sql' AS create_sql,
            mz_internal.redact_sql(data->'value'->'definition'->'V1'->>'create_sql') AS redacted_create_sql
        FROM mz_internal.mz_catalog_raw
        WHERE
            data->>'kind' = 'Item' AND
            mz_internal.parse_catalog_create_sql(data->'value'->'definition'->'V1'->>'create_sql')->>'type' = 'materialized-view'
    ),
    builtin_mappings AS (
        SELECT
            data->'key'->>'schema_name' AS schema_name,
            data->'key'->>'object_name' AS name,
            's' || (data->'value'->>'catalog_id') AS id
        FROM mz_internal.mz_catalog_raw
        WHERE
            data->>'kind' = 'GidMapping' AND
            data->'key'->>'object_type' = '5'
    ),
    builtin_mvs AS (
        SELECT
            m.id,
            mv.oid,
            s.id AS schema_id,
            mv.name,
            c.id AS cluster_id,
            mv.definition,
            '{MZ_SYSTEM_ROLE_ID}' AS owner_id,
            mv.privileges,
            mv.create_sql,
            mz_internal.redact_sql(mv.create_sql) AS redacted_create_sql
        FROM mz_internal.mz_builtin_materialized_views mv
        JOIN builtin_mappings m USING (schema_name, name)
        JOIN mz_schemas s ON s.name = mv.schema_name
        JOIN mz_clusters c ON c.name = mv.cluster_name
        WHERE s.database_id IS NULL
    )
SELECT * FROM user_mvs
UNION ALL
SELECT * FROM builtin_mvs").into_boxed_str()),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "mv",
            description: "A materialized view maintained incrementally on a cluster",
            links: &const { [
                OntologyLink { name: "in_schema", target: "schema", properties: LinkProperties::fk("schema_id", "id", Cardinality::ManyToOne) },
                OntologyLink { name: "owned_by", target: "role", properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne) },
                OntologyLink { name: "runs_on_cluster", target: "cluster", properties: LinkProperties::fk("cluster_id", "id", Cardinality::ManyToOne) },
            ] },
            column_semantic_types: &const {[("id", SemanticType::CatalogItemId), ("oid", SemanticType::OID), ("schema_id", SemanticType::SchemaId), ("cluster_id", SemanticType::ClusterId), ("definition", SemanticType::SqlDefinition), ("owner_id", SemanticType::RoleId), ("create_sql", SemanticType::SqlDefinition), ("redacted_create_sql", SemanticType::RedactedSqlDefinition)]},
        }),
    }
});
pub static MZ_TYPES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_types",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_TYPES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("schema_id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("category", SqlScalarType::String.nullable(false))
        .with_column("owner_id", SqlScalarType::String.nullable(false))
        .with_column(
            "privileges",
            SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
        )
        .with_column("create_sql", SqlScalarType::String.nullable(true))
        .with_column("redacted_create_sql", SqlScalarType::String.nullable(true))
        .with_key(vec![0])
        .with_key(vec![1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "Materialize's unique ID for the type."),
        ("oid", "A PostgreSQL-compatible OID for the type."),
        (
            "schema_id",
            "The ID of the schema to which the type belongs. Corresponds to `mz_schemas.id`.",
        ),
        ("name", "The name of the type."),
        ("category", "The category of the type."),
        (
            "owner_id",
            "The role ID of the owner of the type. Corresponds to `mz_roles.id`.",
        ),
        ("privileges", "The privileges belonging to the type."),
        ("create_sql", "The `CREATE` SQL statement for the type."),
        (
            "redacted_create_sql",
            "The redacted `CREATE` SQL statement for the type.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "type",
        description: "A named data type (base, array, list, map, or pseudo)",
        links: &const {
            [
                OntologyLink {
                    name: "in_schema",
                    target: "schema",
                    properties: LinkProperties::fk("schema_id", "id", Cardinality::ManyToOne),
                },
                OntologyLink {
                    name: "owned_by",
                    target: "role",
                    properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("id", SemanticType::CatalogItemId),
                ("oid", SemanticType::OID),
                ("schema_id", SemanticType::SchemaId),
                ("owner_id", SemanticType::RoleId),
                ("create_sql", SemanticType::SqlDefinition),
                ("redacted_create_sql", SemanticType::RedactedSqlDefinition),
            ]
        },
    }),
});
pub static MZ_ARRAY_TYPES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_array_types",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_ARRAY_TYPES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("element_id", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the array type."),
        ("element_id", "The ID of the array's element type."),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "array_type",
        description: "An array type with its element type",
        links: &const {
            [
                OntologyLink {
                    name: "detail_of",
                    target: "type",
                    properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
                },
                OntologyLink {
                    name: "has_element_type",
                    target: "type",
                    properties: LinkProperties::fk("element_id", "id", Cardinality::ManyToOne),
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("id", SemanticType::CatalogItemId),
                ("element_id", SemanticType::CatalogItemId),
            ]
        },
    }),
});
pub static MZ_BASE_TYPES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_base_types",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_BASE_TYPES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([("id", "The ID of the type.")]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "base_type",
        description: "A primitive/base data type",
        links: &const { [] },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});
pub static MZ_LIST_TYPES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_list_types",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_LIST_TYPES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("element_id", SqlScalarType::String.nullable(false))
        .with_column(
            "element_modifiers",
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::Int64),
                custom_id: None,
            }
            .nullable(true),
        )
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the list type."),
        ("element_id", "The IID of the list's element type."),
        (
            "element_modifiers",
            "The element type modifiers, or `NULL` if none.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "list_type",
        description: "A list type with its element type",
        links: &const {
            [
                OntologyLink {
                    name: "detail_of",
                    target: "type",
                    properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
                },
                OntologyLink {
                    name: "has_element_type",
                    target: "type",
                    properties: LinkProperties::fk("element_id", "id", Cardinality::ManyToOne),
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("id", SemanticType::CatalogItemId),
                ("element_id", SemanticType::CatalogItemId),
            ]
        },
    }),
});
pub static MZ_MAP_TYPES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_map_types",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_MAP_TYPES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("key_id", SqlScalarType::String.nullable(false))
        .with_column("value_id", SqlScalarType::String.nullable(false))
        .with_column(
            "key_modifiers",
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::Int64),
                custom_id: None,
            }
            .nullable(true),
        )
        .with_column(
            "value_modifiers",
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::Int64),
                custom_id: None,
            }
            .nullable(true),
        )
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the map type."),
        ("key_id", "The ID of the map's key type."),
        ("value_id", "The ID of the map's value type."),
        (
            "key_modifiers",
            "The key type modifiers, or `NULL` if none.",
        ),
        (
            "value_modifiers",
            "The value type modifiers, or `NULL` if none.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "map_type",
        description: "A map type with its key and value types",
        links: &const {
            [
                OntologyLink {
                    name: "detail_of",
                    target: "type",
                    properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
                },
                OntologyLink {
                    name: "has_key_type",
                    target: "type",
                    properties: LinkProperties::fk("key_id", "id", Cardinality::ManyToOne),
                },
                OntologyLink {
                    name: "has_value_type",
                    target: "type",
                    properties: LinkProperties::fk("value_id", "id", Cardinality::ManyToOne),
                },
            ]
        },
        column_semantic_types: &const {
            [
                ("id", SemanticType::CatalogItemId),
                ("key_id", SemanticType::CatalogItemId),
                ("value_id", SemanticType::CatalogItemId),
            ]
        },
    }),
});
pub static MZ_ROLES: LazyLock<BuiltinMaterializedView> = LazyLock::new(|| {
    // The two built-in super-roles are identified by name; their names live as
    // compile-time constants in `mz_sql::session::user`, so we interpolate to
    // keep the SQL in sync with the Rust definition.
    let sql = format!(
        "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id,
    ASSERT NOT NULL oid,
    ASSERT NOT NULL name,
    ASSERT NOT NULL inherit
) AS
SELECT
    mz_internal.parse_catalog_id(data->'key'->'id') AS id,
    (data->'value'->>'oid')::oid AS oid,
    data->'value'->>'name' AS name,
    (data->'value'->'attributes'->>'inherit')::bool AS inherit,
    COALESCE(
        (data->'value'->'attributes'->>'login')::bool,
        data->'value'->>'name' IN ('{system}', '{support}')
    ) AS rolcanlogin,
    COALESCE(
        (data->'value'->'attributes'->>'superuser')::bool,
        CASE WHEN data->'value'->>'name' IN ('{system}', '{support}') THEN true END
    ) AS rolsuper
FROM mz_internal.mz_catalog_raw
WHERE data->>'kind' = 'Role' AND data->'key'->'id' != '\"Public\"'::jsonb",
        system = SYSTEM_USER_NAME,
        support = SUPPORT_USER_NAME,
    );

    BuiltinMaterializedView {
        name: "mz_roles",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_ROLES_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("inherit", SqlScalarType::Bool.nullable(false))
            .with_column("rolcanlogin", SqlScalarType::Bool.nullable(true))
            .with_column("rolsuper", SqlScalarType::Bool.nullable(true))
            .with_key(vec![0])
            .with_key(vec![1])
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "Materialize's unique ID for the role."),
            ("oid", "A PostgreSQL-compatible OID for the role."),
            ("name", "The name of the role."),
            (
                "inherit",
                "Indicates whether the role has inheritance of privileges.",
            ),
            ("rolcanlogin", "Indicates whether the role can log in."),
            ("rolsuper", "Indicates whether the role is a superuser."),
        ]),
        sql: Box::leak(sql.into_boxed_str()),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "role",
            description: "A user or role for authentication and access control",
            links: &const { [] },
            column_semantic_types: &const { [("id", SemanticType::RoleId), ("oid", SemanticType::OID)] },
        }),
    }
});

pub static MZ_ROLE_MEMBERS: LazyLock<BuiltinMaterializedView> = LazyLock::new(|| {
    BuiltinMaterializedView {
        name: "mz_role_members",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_ROLE_MEMBERS_OID,
        desc: RelationDesc::builder()
            .with_column("role_id", SqlScalarType::String.nullable(false))
            .with_column("member", SqlScalarType::String.nullable(false))
            .with_column("grantor", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "role_id",
                "The ID of the role the `member` is a member of. Corresponds to `mz_roles.id`.",
            ),
            (
                "member",
                "The ID of the role that is a member of `role_id`. Corresponds to `mz_roles.id`.",
            ),
            (
                "grantor",
                "The ID of the role that granted membership of `member` to `role_id`. Corresponds to `mz_roles.id`.",
            ),
        ]),
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL role_id,
    ASSERT NOT NULL member,
    ASSERT NOT NULL grantor
) AS
SELECT
    mz_internal.parse_catalog_id(entry->'key') AS role_id,
    mz_internal.parse_catalog_id(data->'key'->'id') AS member,
    mz_internal.parse_catalog_id(entry->'value') AS grantor
FROM
    mz_internal.mz_catalog_raw,
    jsonb_array_elements(data->'value'->'membership'->'map') AS entry
WHERE data->>'kind' = 'Role'",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "role_membership",
            description: "A membership grant: one role is a member of another role",
            links: &const {
                [
                    OntologyLink {
                        name: "group_role",
                        target: "role",
                        properties: LinkProperties::fk("role_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "member_role",
                        target: "role",
                        properties: LinkProperties::fk("member", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "granted_by",
                        target: "role",
                        properties: LinkProperties::fk("grantor", "id", Cardinality::ManyToOne),
                    },
                ]
            },
            column_semantic_types: &const {
                [
                    ("role_id", SemanticType::RoleId),
                    ("member", SemanticType::RoleId),
                    ("grantor", SemanticType::RoleId),
                ]
            },
        }),
    }
});

pub static MZ_ROLE_PARAMETERS: LazyLock<BuiltinMaterializedView> = LazyLock::new(|| {
    BuiltinMaterializedView {
        name: "mz_role_parameters",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_ROLE_PARAMETERS_OID,
        desc: RelationDesc::builder()
            .with_column("role_id", SqlScalarType::String.nullable(false))
            .with_column("parameter_name", SqlScalarType::String.nullable(false))
            .with_column("parameter_value", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "role_id",
                "The ID of the role whose configuration parameter default is set. Corresponds to `mz_roles.id`.",
            ),
            (
                "parameter_name",
                "The configuration parameter name. One of the supported configuration parameters.",
            ),
            (
                "parameter_value",
                "The default value of the parameter for the given role. Can be either a single value, or a comma-separated list of values for configuration parameters that accept a list.",
            ),
        ]),
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL role_id,
    ASSERT NOT NULL parameter_name,
    ASSERT NOT NULL parameter_value
) AS
SELECT
    mz_internal.parse_catalog_id(data->'key'->'id') AS role_id,
    entry->>'key' AS parameter_name,
    CASE
        WHEN entry->'val' ? 'Flat' THEN entry->'val'->>'Flat'
        ELSE (
            SELECT pg_catalog.string_agg(t.elem, ', ' ORDER BY t.ord)
            FROM jsonb_array_elements_text(entry->'val'->'SqlSet')
                WITH ORDINALITY AS t(elem, ord)
        )
    END AS parameter_value
FROM
    mz_internal.mz_catalog_raw,
    jsonb_array_elements(data->'value'->'vars'->'entries') AS entry
WHERE data->>'kind' = 'Role'",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "role_parameter",
            description: "A session parameter default set for a role",
            links: &const {
                [OntologyLink {
                    name: "default_parameter_setting_of",
                    target: "role",
                    properties: LinkProperties::fk("role_id", "id", Cardinality::ManyToOne),
                }]
            },
            column_semantic_types: &[("role_id", SemanticType::RoleId)],
        }),
    }
});
pub static MZ_ROLE_AUTH: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_role_auth",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_ROLE_AUTH_OID,
    desc: RelationDesc::builder()
        .with_column("role_id", SqlScalarType::String.nullable(false))
        .with_column("role_oid", SqlScalarType::Oid.nullable(false))
        .with_column("password_hash", SqlScalarType::String.nullable(true))
        .with_column(
            "updated_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "role_id",
            "The ID of the role. Corresponds to `mz_roles.id`.",
        ),
        ("role_oid", "A PostgreSQL-compatible OID for the role."),
        (
            "password_hash",
            "The hashed password for the role, if any. Uses the `SCRAM-SHA-256` algorithm.",
        ),
        (
            "updated_at",
            "The time at which the password was last updated.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![rbac::owner_privilege(ObjectType::Table, MZ_SYSTEM_ROLE_ID)],
    ontology: None,
});
pub static MZ_PSEUDO_TYPES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_pseudo_types",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_PSEUDO_TYPES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([("id", "The ID of the type.")]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "pseudo_type",
        description: "A pseudo-type used in function signatures",
        links: &const { [] },
        column_semantic_types: &[("id", SemanticType::CatalogItemId)],
    }),
});
pub static MZ_FUNCTIONS: LazyLock<BuiltinTable> = LazyLock::new(|| {
    BuiltinTable {
        name: "mz_functions",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::TABLE_MZ_FUNCTIONS_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false)) // not a key!
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_id", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column(
                "argument_type_ids",
                SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false),
            )
            .with_column(
                "variadic_argument_type_id",
                SqlScalarType::String.nullable(true),
            )
            .with_column("return_type_id", SqlScalarType::String.nullable(true))
            .with_column("returns_set", SqlScalarType::Bool.nullable(false))
            .with_column("owner_id", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "Materialize's unique ID for the function."),
            ("oid", "A PostgreSQL-compatible OID for the function."),
            (
                "schema_id",
                "The ID of the schema to which the function belongs. Corresponds to `mz_schemas.id`.",
            ),
            ("name", "The name of the function."),
            (
                "argument_type_ids",
                "The ID of each argument's type. Each entry refers to `mz_types.id`.",
            ),
            (
                "variadic_argument_type_id",
                "The ID of the variadic argument's type, or `NULL` if the function does not have a variadic argument. Refers to `mz_types.id`.",
            ),
            (
                "return_type_id",
                "The returned value's type, or `NULL` if the function does not return a value. Refers to `mz_types.id`. Note that for table functions with > 1 column, this type corresponds to [`record`].",
            ),
            (
                "returns_set",
                "Whether the function returns a set, i.e. the function is a table function.",
            ),
            (
                "owner_id",
                "The role ID of the owner of the function. Corresponds to `mz_roles.id`.",
            ),
        ]),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "function",
            description: "A built-in or user-defined function",
            links: &const {
                [
                    OntologyLink {
                        name: "in_schema",
                        target: "schema",
                        properties: LinkProperties::fk("schema_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "owned_by",
                        target: "role",
                        properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "returns_type",
                        target: "type",
                        properties: LinkProperties::fk_nullable(
                            "return_type_id",
                            "id",
                            Cardinality::ManyToOne,
                        ),
                    },
                    OntologyLink {
                        name: "has_variadic_arg_type",
                        target: "type",
                        properties: LinkProperties::fk_nullable(
                            "variadic_argument_type_id",
                            "id",
                            Cardinality::ManyToOne,
                        ),
                    },
                ]
            },
            column_semantic_types: &const {
                [
                    ("id", SemanticType::CatalogItemId),
                    ("oid", SemanticType::OID),
                    ("schema_id", SemanticType::SchemaId),
                    ("variadic_argument_type_id", SemanticType::CatalogItemId),
                    ("return_type_id", SemanticType::CatalogItemId),
                    ("owner_id", SemanticType::RoleId),
                ]
            },
        }),
    }
});
pub static MZ_OPERATORS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_operators",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_OPERATORS_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column(
            "argument_type_ids",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false),
        )
        .with_column("return_type_id", SqlScalarType::String.nullable(true))
        .finish(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "operator",
        description: "A built-in SQL operator",
        links: &const {
            [OntologyLink {
                name: "returns_type",
                target: "type",
                properties: LinkProperties::fk_nullable(
                    "return_type_id",
                    "id",
                    Cardinality::ManyToOne,
                ),
            }]
        },
        column_semantic_types: &const {
            [
                ("oid", SemanticType::OID),
                ("return_type_id", SemanticType::CatalogItemId),
            ]
        },
    }),
});

pub static MZ_CLUSTERS: LazyLock<BuiltinMaterializedView> = LazyLock::new(|| {
    BuiltinMaterializedView {
        name: "mz_clusters",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_CLUSTERS_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("owner_id", SqlScalarType::String.nullable(false))
            .with_column(
                "privileges",
                SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
            )
            .with_column("managed", SqlScalarType::Bool.nullable(false))
            .with_column("size", SqlScalarType::String.nullable(true))
            .with_column("replication_factor", SqlScalarType::UInt32.nullable(true))
            .with_column("disk", SqlScalarType::Bool.nullable(true))
            .with_column(
                "availability_zones",
                SqlScalarType::List {
                    element_type: Box::new(SqlScalarType::String),
                    custom_id: None,
                }
                .nullable(true),
            )
            .with_column(
                "introspection_debugging",
                SqlScalarType::Bool.nullable(true),
            )
            .with_column(
                "introspection_interval",
                SqlScalarType::Interval.nullable(true),
            )
            .with_key(vec![0])
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "Materialize's unique ID for the cluster."),
            ("name", "The name of the cluster."),
            (
                "owner_id",
                "The role ID of the owner of the cluster. Corresponds to `mz_roles.id`.",
            ),
            ("privileges", "The privileges belonging to the cluster."),
            (
                "managed",
                "Whether the cluster is a managed cluster with automatically managed replicas.",
            ),
            (
                "size",
                "If the cluster is managed, the desired size of the cluster's replicas. `NULL` for unmanaged clusters.",
            ),
            (
                "replication_factor",
                "If the cluster is managed, the desired number of replicas of the cluster. `NULL` for unmanaged clusters.",
            ),
            (
                "disk",
                "**Unstable** If the cluster is managed, `true` if the replicas have the `DISK` option . `NULL` for unmanaged clusters.",
            ),
            (
                "availability_zones",
                "**Unstable** If the cluster is managed, the list of availability zones specified in `AVAILABILITY ZONES`. `NULL` for unmanaged clusters.",
            ),
            (
                "introspection_debugging",
                "Whether introspection of the gathering of the introspection data is enabled.",
            ),
            (
                "introspection_interval",
                "The interval at which to collect introspection data.",
            ),
        ]),
        // `config.variant` is a serde-tagged enum: `"Unmanaged"` (bare string)
        // or `{"Managed": {...}}`. Managed-only fields (size, replication_factor,
        // availability_zones, logging, schedule) live under `Managed`, and
        // resolve to NULL for unmanaged clusters.
        //
        // `disk` is computed as in
        // `CatalogState::cluster_replica_size_has_disk`:
        // `NOT swap_enabled AND disk_bytes != 0`. Both flags are sourced from
        // `mz_internal.mz_cluster_replica_size_internal`, which the catalog
        // populates for every size — including ones flagged `disabled`, which
        // the public `mz_cluster_replica_sizes` table omits. Using the internal
        // table directly preserves the prior behavior of indexing the in-memory
        // size map regardless of `disabled`.
        //
        // `availability_zones` is normalised to NULL when empty, matching
        // `pack_cluster_update`'s `None` for an empty `Vec`. `list_agg` over
        // `jsonb_array_elements_text WITH ORDINALITY` preserves the configured
        // order, matching the prior `push_list` of the `Vec`.
        //
        // `introspection_interval` is a Duration `{secs, nanos}` in the JSON;
        // we compose a string and cast to interval because Materialize has no
        // `make_interval`.
        //
        // The `kind = 'Cluster'` filter is pushed into a subquery on
        // `mz_catalog_raw` by hand. database-issues/8495 keeps the optimizer
        // from pushing a top-level `WHERE` below the LEFT JOIN, so without the
        // subquery the join scans the entire raw catalog.
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id,
    ASSERT NOT NULL name,
    ASSERT NOT NULL owner_id,
    ASSERT NOT NULL privileges,
    ASSERT NOT NULL managed
) AS
SELECT
    mz_internal.parse_catalog_id(data->'key'->'id') AS id,
    data->'value'->>'name' AS name,
    mz_internal.parse_catalog_id(data->'value'->'owner_id') AS owner_id,
    mz_internal.parse_catalog_privileges(data->'value'->'privileges') AS privileges,
    jsonb_typeof(data->'value'->'config'->'variant') = 'object' AS managed,
    data->'value'->'config'->'variant'->'Managed'->>'size' AS size,
    (data->'value'->'config'->'variant'->'Managed'->>'replication_factor')::uint4 AS replication_factor,
    CASE
        WHEN jsonb_typeof(data->'value'->'config'->'variant') = 'object' THEN
            NOT COALESCE(internal.swap_enabled, false)
            AND COALESCE(internal.disk_bytes, 0) != 0
    END AS disk,
    CASE
        WHEN jsonb_array_length(data->'value'->'config'->'variant'->'Managed'->'availability_zones') > 0 THEN
            (
                SELECT mz_catalog.list_agg(az.value ORDER BY az.ord)
                FROM jsonb_array_elements_text(data->'value'->'config'->'variant'->'Managed'->'availability_zones')
                     WITH ORDINALITY AS az(value, ord)
            )
    END AS availability_zones,
    (data->'value'->'config'->'variant'->'Managed'->'logging'->>'log_logging')::bool AS introspection_debugging,
    CASE
        WHEN data->'value'->'config'->'variant'->'Managed'->'logging'->'interval' != 'null'::jsonb THEN
            (
                (data->'value'->'config'->'variant'->'Managed'->'logging'->'interval'->>'secs')
                || ' seconds '
                || ((data->'value'->'config'->'variant'->'Managed'->'logging'->'interval'->>'nanos')::bigint / 1000)::text
                || ' microseconds'
            )::interval
    END AS introspection_interval
FROM (
    SELECT data FROM mz_internal.mz_catalog_raw WHERE data->>'kind' = 'Cluster'
) raw
LEFT JOIN mz_internal.mz_cluster_replica_size_internal internal
    ON internal.size = data->'value'->'config'->'variant'->'Managed'->>'size'",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "cluster",
            description: "A compute cluster that runs dataflows for sources, sinks, MVs, and indexes",
            links: &const {
                [
                    OntologyLink {
                        name: "owned_by",
                        target: "role",
                        properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "has_size",
                        target: "replica_size",
                        properties: LinkProperties::fk_nullable("size", "size", Cardinality::ManyToOne),
                    },
                ]
            },
            column_semantic_types: &const {
                [
                    ("id", SemanticType::ClusterId),
                    ("owner_id", SemanticType::RoleId),
                ]
            },
        }),
    }
});

pub static MZ_SECRETS: LazyLock<BuiltinMaterializedView> = LazyLock::new(|| {
    BuiltinMaterializedView {
        name: "mz_secrets",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_SECRETS_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_id", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("owner_id", SqlScalarType::String.nullable(false))
            .with_column(
                "privileges",
                SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
            )
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "The unique ID of the secret."),
            ("oid", "A PostgreSQL-compatible oid for the secret."),
            (
                "schema_id",
                "The ID of the schema to which the secret belongs. Corresponds to `mz_schemas.id`.",
            ),
            ("name", "The name of the secret."),
            (
                "owner_id",
                "The role ID of the owner of the secret. Corresponds to `mz_roles.id`.",
            ),
            ("privileges", "The privileges belonging to the secret."),
        ]),
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id,
    ASSERT NOT NULL oid,
    ASSERT NOT NULL schema_id,
    ASSERT NOT NULL name,
    ASSERT NOT NULL owner_id,
    ASSERT NOT NULL privileges
) AS
SELECT
    mz_internal.parse_catalog_id(data->'key'->'gid') AS id,
    (data->'value'->>'oid')::oid AS oid,
    mz_internal.parse_catalog_id(data->'value'->'schema_id') AS schema_id,
    data->'value'->>'name' AS name,
    mz_internal.parse_catalog_id(data->'value'->'owner_id') AS owner_id,
    mz_internal.parse_catalog_privileges(data->'value'->'privileges') AS privileges
FROM mz_internal.mz_catalog_raw
WHERE
    data->>'kind' = 'Item' AND
    mz_internal.parse_catalog_create_sql(data->'value'->'definition'->'V1'->>'create_sql')->>'type' = 'secret'",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "secret",
            description: "A user-defined secret containing sensitive configuration (e.g., credentials)",
            links: &const { [
                OntologyLink {
                    name: "in_schema",
                    target: "schema",
                    properties: LinkProperties::fk("schema_id", "id", Cardinality::ManyToOne),
                },
                OntologyLink {
                    name: "owned_by",
                    target: "role",
                    properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
                },
            ] },
            column_semantic_types: &const {[("id", SemanticType::CatalogItemId), ("oid", SemanticType::OID), ("schema_id", SemanticType::SchemaId), ("owner_id", SemanticType::RoleId)]},
        }),
    }
});

pub static MZ_CLUSTER_REPLICAS: LazyLock<BuiltinMaterializedView> = LazyLock::new(|| {
    BuiltinMaterializedView {
        name: "mz_cluster_replicas",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_CLUSTER_REPLICAS_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("cluster_id", SqlScalarType::String.nullable(false))
            .with_column("size", SqlScalarType::String.nullable(true))
            // `NULL` for un-orchestrated clusters and for replicas where the user
            // hasn't specified them.
            .with_column("availability_zone", SqlScalarType::String.nullable(true))
            .with_column("owner_id", SqlScalarType::String.nullable(false))
            .with_column("disk", SqlScalarType::Bool.nullable(true))
            .with_key(vec![0])
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "Materialize's unique ID for the cluster replica."),
            ("name", "The name of the cluster replica."),
            (
                "cluster_id",
                "The ID of the cluster to which the replica belongs. Corresponds to `mz_clusters.id`.",
            ),
            (
                "size",
                "The cluster replica's size, selected during creation.",
            ),
            (
                "availability_zone",
                "The availability zones the replica is provisioned in, comma-separated. `NULL` if nothing constrains the replica's placement.",
            ),
            (
                "owner_id",
                "The role ID of the owner of the cluster replica. Corresponds to `mz_roles.id`.",
            ),
            ("disk", "If the replica has a local disk."),
        ]),
        // `config.location` is a serde-tagged enum: `{"Unmanaged": {...}}` or
        // `{"Managed": {...}}`. For replicas with an unmanaged location all
        // orchestrator-facing fields (size, availability_zone, disk) are NULL.
        //
        // `availability_zone` surfaces the durable `Managed` location's
        // `availability_zones` list as a comma-separated string in stored
        // order. For a managed cluster that is the provisioned AVAILABILITY
        // ZONES pool. For an unmanaged cluster it is the single AVAILABILITY
        // ZONE pin. An empty list (nothing constrains placement) maps to NULL,
        // matching the list-to-NULL normalization the `mz_clusters`
        // `availability_zones` column uses.
        //
        // `disk` mirrors `cluster_replica_size_has_disk`, joining
        // `mz_cluster_replica_size_internal` for both `swap_enabled` and
        // `disk_bytes`. The catalog populates that table for every size
        // (including ones flagged `disabled`), so a managed replica pinned to
        // a disabled size still resolves correctly. If a row is somehow
        // missing, the COALESCEs resolve `disk` to `false` (treating an
        // unknown size as having no disk), not NULL.
        //
        // The `kind = 'ClusterReplica'` filter is pushed into a subquery on
        // `mz_catalog_raw` by hand. database-issues/8495 keeps the optimizer
        // from pushing a top-level `WHERE` below the LEFT JOIN.
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id,
    ASSERT NOT NULL name,
    ASSERT NOT NULL cluster_id,
    ASSERT NOT NULL owner_id
) AS
SELECT
    mz_internal.parse_catalog_id(data->'key'->'id') AS id,
    data->'value'->>'name' AS name,
    mz_internal.parse_catalog_id(data->'value'->'cluster_id') AS cluster_id,
    data->'value'->'config'->'location'->'Managed'->>'size' AS size,
    CASE
        WHEN jsonb_array_length(data->'value'->'config'->'location'->'Managed'->'availability_zones') > 0 THEN
            (
                SELECT pg_catalog.string_agg(az.value, ',' ORDER BY az.ord)
                FROM jsonb_array_elements_text(data->'value'->'config'->'location'->'Managed'->'availability_zones')
                     WITH ORDINALITY AS az(value, ord)
            )
    END AS availability_zone,
    mz_internal.parse_catalog_id(data->'value'->'owner_id') AS owner_id,
    CASE
        WHEN data->'value'->'config'->'location' ? 'Managed' THEN
            NOT COALESCE(internal.swap_enabled, false)
            AND COALESCE(internal.disk_bytes, 0) != 0
    END AS disk
FROM (
    SELECT data FROM mz_internal.mz_catalog_raw WHERE data->>'kind' = 'ClusterReplica'
) raw
LEFT JOIN mz_internal.mz_cluster_replica_size_internal internal
    ON internal.size = data->'value'->'config'->'location'->'Managed'->>'size'",
        is_retained_metrics_object: true,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "replica",
            description: "A physical replica of a cluster providing fault tolerance",
            links: &const {
                [
                    OntologyLink {
                        name: "owned_by",
                        target: "role",
                        properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "belongs_to_cluster",
                        target: "cluster",
                        properties: LinkProperties::fk("cluster_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "has_size",
                        target: "replica_size",
                        properties: LinkProperties::fk_nullable(
                            "size",
                            "size",
                            Cardinality::ManyToOne,
                        ),
                    },
                ]
            },
            column_semantic_types: &const {
                [
                    ("id", SemanticType::ReplicaId),
                    ("cluster_id", SemanticType::ClusterId),
                    ("owner_id", SemanticType::RoleId),
                ]
            },
        }),
    }
});

pub static MZ_CLUSTER_REPLICA_SIZES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_cluster_replica_sizes",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_CLUSTER_REPLICA_SIZES_OID,
    desc: RelationDesc::builder()
        .with_column("size", SqlScalarType::String.nullable(false))
        .with_column("processes", SqlScalarType::UInt64.nullable(false))
        .with_column("workers", SqlScalarType::UInt64.nullable(false))
        .with_column("cpu_nano_cores", SqlScalarType::UInt64.nullable(false))
        .with_column("memory_bytes", SqlScalarType::UInt64.nullable(false))
        .with_column("disk_bytes", SqlScalarType::UInt64.nullable(true))
        .with_column(
            "credits_per_hour",
            SqlScalarType::Numeric { max_scale: None }.nullable(false),
        )
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("size", "The human-readable replica size."),
        ("processes", "The number of processes in the replica."),
        (
            "workers",
            "The number of Timely Dataflow workers per process.",
        ),
        (
            "cpu_nano_cores",
            "The CPU allocation per process, in billionths of a vCPU core.",
        ),
        (
            "memory_bytes",
            "The RAM allocation per process, in billionths of a vCPU core.",
        ),
        ("disk_bytes", "The disk allocation per process."),
        (
            "credits_per_hour",
            "The number of compute credits consumed per hour.",
        ),
    ]),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "replica_size",
        description: "Available cluster replica sizes with CPU, memory, and credit cost",
        links: &const { [] },
        column_semantic_types: &const {
            [
                ("memory_bytes", SemanticType::ByteCount),
                ("disk_bytes", SemanticType::ByteCount),
                ("credits_per_hour", SemanticType::CreditRate),
            ]
        },
    }),
});

pub static MZ_AUDIT_EVENTS: LazyLock<BuiltinMaterializedView> = LazyLock::new(|| {
    BuiltinMaterializedView {
        name: "mz_audit_events",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_AUDIT_EVENTS_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("event_type", SqlScalarType::String.nullable(false))
            .with_column("object_type", SqlScalarType::String.nullable(false))
            .with_column("details", SqlScalarType::Jsonb.nullable(false))
            .with_column("user", SqlScalarType::String.nullable(true))
            .with_column(
                "occurred_at",
                SqlScalarType::TimestampTz { precision: None }.nullable(false),
            )
            .with_key(vec![0])
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "id",
                "Materialize's unique, monotonically increasing ID for the event.",
            ),
            (
                "event_type",
                "The type of the event: `create`, `drop`, `alter`, `grant`, `revoke`, or `comment`.",
            ),
            (
                "object_type",
                "The type of the affected object: `cluster`, `cluster-replica`, `connection`, `continual-task`, `database`, `func`, `index`, `materialized-view`, `network-policy`, `role`, `schema`, `secret`, `sink`, `source`, `system`, `table`, `type`, or `view`.",
            ),
            (
                "details",
                "Additional details about the event. The shape of the details varies based on `event_type` and `object_type`.",
            ),
            (
                "user",
                "The user who triggered the event, or `NULL` if triggered by the system.",
            ),
            (
                "occurred_at",
                "The time at which the event occurred. Guaranteed to be in order of event creation. Events created in the same transaction will have identical values.",
            ),
        ]),
        // `event_type` and `object_type` are stored in `mz_catalog_raw` as the
        // numeric `Serialize_repr` of `proto::audit_log_event_v1::{EventType,
        // ObjectType}`. The CASE expressions map them to the kebab strings
        // that `audit_log::{EventType, ObjectType}`'s Display impls produce.
        //
        // `event` is `AuditLogEvent::V1(AuditLogEventV1)`, externally tagged,
        // so we reach through `key.event.V1` for the inner struct.
        //
        // `user` is `Option<StringWrapper>`: JSON null when absent, otherwise
        // `{"inner": "<name>"}`. `->'user'->>'inner'` collapses both to a
        // PostgreSQL NULL or the text.
        //
        // `details` in `mz_catalog_raw` is the externally-tagged proto
        // `Details` enum. `parse_catalog_audit_log_details` reshapes it into
        // `audit_log::EventDetails::as_json`'s output, the format the prior
        // BuiltinTable populator wrote. See that function's docstring and
        // the round-trip test in `src/catalog/tests/audit_log_details.rs`.
        //
        // `occurred_at` is `EpochMillis` (u64). Dividing by 1000.0 and feeding
        // to `to_timestamp` matches `mz_ore::now::to_datetime`'s round-trip
        // through `chrono::DateTime::from_timestamp_millis`.
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL id,
    ASSERT NOT NULL event_type,
    ASSERT NOT NULL object_type,
    ASSERT NOT NULL details,
    ASSERT NOT NULL occurred_at
) AS
WITH ev AS (
    SELECT data->'key'->'event'->'V1' AS e
    FROM mz_internal.mz_catalog_raw
    WHERE data->>'kind' = 'AuditLog'
)
SELECT
    (e->>'id')::uint8                                                       AS id,
    CASE (e->>'event_type')
        WHEN '1' THEN 'create'
        WHEN '2' THEN 'drop'
        WHEN '3' THEN 'alter'
        WHEN '4' THEN 'grant'
        WHEN '5' THEN 'revoke'
        WHEN '6' THEN 'comment'
    END                                                                     AS event_type,
    CASE (e->>'object_type')
        WHEN '1'  THEN 'cluster'
        WHEN '2'  THEN 'cluster-replica'
        WHEN '3'  THEN 'connection'
        WHEN '4'  THEN 'database'
        WHEN '5'  THEN 'func'
        WHEN '6'  THEN 'index'
        WHEN '7'  THEN 'materialized-view'
        WHEN '8'  THEN 'role'
        WHEN '9'  THEN 'secret'
        WHEN '10' THEN 'schema'
        WHEN '11' THEN 'sink'
        WHEN '12' THEN 'source'
        WHEN '13' THEN 'table'
        WHEN '14' THEN 'type'
        WHEN '15' THEN 'view'
        WHEN '16' THEN 'system'
        WHEN '17' THEN 'continual-task'
        WHEN '18' THEN 'network-policy'
    END                                                                     AS object_type,
    mz_internal.parse_catalog_audit_log_details(e->'details')               AS details,
    e->'user'->>'inner'                                                     AS \"user\",
    -- `occurred_at` is serialized by `proto::EpochMillis` as
    -- `{\"millis\": <u64>}`; reach through `'millis'` to get the integer.
    to_timestamp(((e->'occurred_at'->>'millis')::float8) / 1000.0)          AS occurred_at
FROM ev",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "audit_event",
            description: "An audit log entry recording a DDL operation",
            links: &const { [] },
            column_semantic_types: &const {
                [
                    ("object_type", SemanticType::ObjectType),
                    ("occurred_at", SemanticType::WallclockTimestamp),
                ]
            },
        }),
    }
});

pub static MZ_EGRESS_IPS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_egress_ips",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_EGRESS_IPS_OID,
    desc: RelationDesc::builder()
        .with_column("egress_ip", SqlScalarType::String.nullable(false))
        .with_column("prefix_length", SqlScalarType::Int32.nullable(false))
        .with_column("cidr", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("egress_ip", "The start of the range of IP addresses."),
        (
            "prefix_length",
            "The number of leading bits in the CIDR netmask.",
        ),
        ("cidr", "The CIDR representation."),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "egress_ip",
        description: "IP addresses used for outbound connections from Materialize",
        links: &const { [] },
        column_semantic_types: &[],
    }),
});

pub static MZ_AWS_PRIVATELINK_CONNECTIONS: LazyLock<BuiltinTable> =
    LazyLock::new(|| BuiltinTable {
        name: "mz_aws_privatelink_connections",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::TABLE_MZ_AWS_PRIVATELINK_CONNECTIONS_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("principal", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "The ID of the connection."),
            (
                "principal",
                "The AWS Principal that Materialize will use to connect to the VPC endpoint.",
            ),
        ]),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "aws_privatelink_connection",
            description: "AWS PrivateLink connection configuration",
            links: &const {
                [OntologyLink {
                    name: "details_of",
                    target: "connection",
                    properties: LinkProperties::fk("id", "id", Cardinality::OneToOne),
                }]
            },
            column_semantic_types: &[("id", SemanticType::CatalogItemId)],
        }),
    });

pub static MZ_CLUSTER_REPLICA_FRONTIERS: LazyLock<BuiltinSource> =
    LazyLock::new(|| BuiltinSource {
        name: "mz_cluster_replica_frontiers",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::SOURCE_MZ_CLUSTER_REPLICA_FRONTIERS_OID,
        data_source: IntrospectionType::ReplicaFrontiers.into(),
        desc: RelationDesc::builder()
            .with_column("object_id", SqlScalarType::String.nullable(false))
            .with_column("replica_id", SqlScalarType::String.nullable(false))
            .with_column("write_frontier", SqlScalarType::MzTimestamp.nullable(true))
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "object_id",
                "The ID of the source, sink, index, materialized view, or subscription.",
            ),
            ("replica_id", "The ID of a cluster replica."),
            (
                "write_frontier",
                "The next timestamp at which the output may change.",
            ),
        ]),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: None,
    });

pub static MZ_CLUSTER_REPLICA_FRONTIERS_IND: LazyLock<BuiltinIndex> =
    LazyLock::new(|| BuiltinIndex {
        name: "mz_cluster_replica_frontiers_ind",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::INDEX_MZ_CLUSTER_REPLICA_FRONTIERS_IND_OID,
        sql: "IN CLUSTER mz_catalog_server ON mz_catalog.mz_cluster_replica_frontiers (object_id)",
        is_retained_metrics_object: false,
    });

pub static MZ_DEFAULT_PRIVILEGES: LazyLock<BuiltinMaterializedView> = LazyLock::new(|| {
    BuiltinMaterializedView {
        name: "mz_default_privileges",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_DEFAULT_PRIVILEGES_OID,
        desc: RelationDesc::builder()
            .with_column("role_id", SqlScalarType::String.nullable(false))
            .with_column("database_id", SqlScalarType::String.nullable(true))
            .with_column("schema_id", SqlScalarType::String.nullable(true))
            .with_column("object_type", SqlScalarType::String.nullable(false))
            .with_column("grantee", SqlScalarType::String.nullable(false))
            .with_column("privileges", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "role_id",
                "Privileges described in this row will be granted on objects created by `role_id`. The role ID `p` stands for the `PUBLIC` pseudo-role and applies to all roles.",
            ),
            (
                "database_id",
                "Privileges described in this row will be granted only on objects in the database identified by `database_id` if non-null.",
            ),
            (
                "schema_id",
                "Privileges described in this row will be granted only on objects in the schema identified by `schema_id` if non-null.",
            ),
            (
                "object_type",
                "Privileges described in this row will be granted only on objects of type `object_type`.",
            ),
            (
                "grantee",
                "Privileges described in this row will be granted to `grantee`. The role ID `p` stands for the `PUBLIC` pseudo-role and applies to all roles.",
            ),
            ("privileges", "The set of privileges that will be granted."),
        ]),
        // `object_type` in `mz_catalog_raw` is the numeric `Serialize_repr` form
        // of `mz_catalog_protos::ObjectType`. The CASE mirrors
        // `mz_sql::catalog::ObjectType`'s `Display` impl, lowercased, matching
        // the prior `pack_default_privileges_update` populator. Variant `16` is
        // reserved/unused; any new proto variant must add a branch here. The
        // `object_type_case_matches_proto_display` test in this crate checks
        // the mapping.
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL role_id,
    ASSERT NOT NULL object_type,
    ASSERT NOT NULL grantee,
    ASSERT NOT NULL privileges
) AS
SELECT
    mz_internal.parse_catalog_id(data->'key'->'role_id') AS role_id,
    CASE WHEN data->'key'->'database_id' != 'null'::jsonb
         THEN mz_internal.parse_catalog_id(data->'key'->'database_id') END AS database_id,
    CASE WHEN data->'key'->'schema_id' != 'null'::jsonb
         THEN mz_internal.parse_catalog_id(data->'key'->'schema_id') END AS schema_id,
    CASE data->'key'->>'object_type'
        WHEN '1'  THEN 'table'
        WHEN '2'  THEN 'view'
        WHEN '3'  THEN 'materialized view'
        WHEN '4'  THEN 'source'
        WHEN '5'  THEN 'sink'
        WHEN '6'  THEN 'index'
        WHEN '7'  THEN 'type'
        WHEN '8'  THEN 'role'
        WHEN '9'  THEN 'cluster'
        WHEN '10' THEN 'cluster replica'
        WHEN '11' THEN 'secret'
        WHEN '12' THEN 'connection'
        WHEN '13' THEN 'database'
        WHEN '14' THEN 'schema'
        WHEN '15' THEN 'function'
        -- variant 16 reserved/unused in mz_catalog_protos::ObjectType.
        WHEN '17' THEN 'network policy'
    END AS object_type,
    mz_internal.parse_catalog_id(data->'key'->'grantee') AS grantee,
    mz_internal.parse_catalog_acl_mode(data->'value'->'privileges') AS privileges
FROM mz_internal.mz_catalog_raw
WHERE data->>'kind' = 'DefaultPrivileges'",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "default_privilege",
            description: "A default privilege rule applied to newly created objects",
            links: &const {
                [
                    OntologyLink {
                        name: "default_priv_for_role",
                        target: "role",
                        properties: LinkProperties::fk("role_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "default_priv_in_database",
                        target: "database",
                        properties: LinkProperties::fk_nullable(
                            "database_id",
                            "id",
                            Cardinality::ManyToOne,
                        ),
                    },
                    OntologyLink {
                        name: "default_priv_in_schema",
                        target: "schema",
                        properties: LinkProperties::fk_nullable(
                            "schema_id",
                            "id",
                            Cardinality::ManyToOne,
                        ),
                    },
                    OntologyLink {
                        name: "default_priv_granted_to",
                        target: "role",
                        properties: LinkProperties::fk("grantee", "id", Cardinality::ManyToOne),
                    },
                ]
            },
            column_semantic_types: &const {
                [
                    ("role_id", SemanticType::RoleId),
                    ("database_id", SemanticType::DatabaseId),
                    ("schema_id", SemanticType::SchemaId),
                    ("object_type", SemanticType::ObjectType),
                    ("grantee", SemanticType::RoleId),
                ]
            },
        }),
    }
});

pub static MZ_SYSTEM_PRIVILEGES: LazyLock<BuiltinMaterializedView> = LazyLock::new(|| {
    BuiltinMaterializedView {
        name: "mz_system_privileges",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::MV_MZ_SYSTEM_PRIVILEGES_OID,
        desc: RelationDesc::builder()
            .with_column("privileges", SqlScalarType::MzAclItem.nullable(false))
            .finish(),
        column_comments: BTreeMap::from_iter([(
            "privileges",
            "The privileges belonging to the system.",
        )]),
        // The durable row holds `(grantee, grantor, acl_mode)`; we rebuild the
        // single-element JSON shape that `parse_catalog_privileges` expects and
        // unnest it to recover the `mz_aclitem`.
        sql: "
IN CLUSTER mz_catalog_server
WITH (
    ASSERT NOT NULL privileges
) AS
SELECT
    unnest(mz_internal.parse_catalog_privileges(
        jsonb_build_array(
            jsonb_build_object(
                'grantee',  data->'key'->'grantee',
                'grantor',  data->'key'->'grantor',
                'acl_mode', data->'value'->'acl_mode'
            )
        )
    )) AS privileges
FROM mz_internal.mz_catalog_raw
WHERE data->>'kind' = 'SystemPrivileges'",
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "system_privilege",
            description: "A system-level privilege grant",
            links: &const { [] },
            column_semantic_types: &[],
        }),
    }
});

pub static MZ_STORAGE_USAGE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_storage_usage",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::VIEW_MZ_STORAGE_USAGE_OID,
    desc: RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("size_bytes", SqlScalarType::UInt64.nullable(false))
        .with_column(
            "collection_timestamp",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_key(vec![0, 2])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "object_id",
            "The ID of the table, source, or materialized view.",
        ),
        (
            "size_bytes",
            "The number of storage bytes used by the object.",
        ),
        (
            "collection_timestamp",
            "The time at which storage usage of the object was assessed.",
        ),
    ]),
    sql: "
SELECT
    object_id,
    sum(size_bytes)::uint8 AS size_bytes,
    collection_timestamp
FROM
    mz_internal.mz_storage_shards
    JOIN mz_internal.mz_storage_usage_by_shard USING (shard_id)
GROUP BY object_id, collection_timestamp",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "storage_usage",
        description: "Historical storage usage per object over time",
        links: &const {
            [OntologyLink {
                name: "storage_usage_of",
                target: "object",
                properties: LinkProperties::fk("object_id", "id", Cardinality::ManyToOne),
            }]
        },
        column_semantic_types: &const {
            [
                ("object_id", SemanticType::CatalogItemId),
                ("size_bytes", SemanticType::ByteCount),
                ("collection_timestamp", SemanticType::WallclockTimestamp),
            ]
        },
    }),
});

pub static MZ_RECENT_STORAGE_USAGE: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
    name: "mz_recent_storage_usage",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::VIEW_MZ_RECENT_STORAGE_USAGE_OID,
    desc: RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("size_bytes", SqlScalarType::UInt64.nullable(true))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("object_id", "The ID of the table, source, or materialized view."),
        ("size_bytes", "The number of storage bytes used by the object in the most recent assessment."),
    ]),
    sql: "
WITH

recent_storage_usage_by_shard AS (
    SELECT shard_id, size_bytes, collection_timestamp
    FROM mz_internal.mz_storage_usage_by_shard
    -- Restricting to the last 6 hours makes it feasible to index the view.
    WHERE collection_timestamp + '6 hours' >= mz_now()
),

most_recent_collection_timestamp_by_shard AS (
    SELECT shard_id, max(collection_timestamp) AS collection_timestamp
    FROM recent_storage_usage_by_shard
    GROUP BY shard_id
)

SELECT
    object_id,
    sum(size_bytes)::uint8 AS size_bytes
FROM
    mz_internal.mz_storage_shards
    LEFT JOIN most_recent_collection_timestamp_by_shard
        ON mz_storage_shards.shard_id = most_recent_collection_timestamp_by_shard.shard_id
    LEFT JOIN recent_storage_usage_by_shard
        ON mz_storage_shards.shard_id = recent_storage_usage_by_shard.shard_id
        AND most_recent_collection_timestamp_by_shard.collection_timestamp = recent_storage_usage_by_shard.collection_timestamp
GROUP BY object_id",
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "recent_storage",
        description: "Most recent storage usage snapshot per object",
        links: &const { [
            OntologyLink { name: "recent_storage_of", target: "object", properties: LinkProperties::fk("object_id", "id", Cardinality::OneToOne) },
        ] },
        column_semantic_types: &const {[("object_id", SemanticType::CatalogItemId), ("size_bytes", SemanticType::ByteCount)]},
    }),
}
});

pub static MZ_RECENT_STORAGE_USAGE_IND: LazyLock<BuiltinIndex> = LazyLock::new(|| BuiltinIndex {
    name: "mz_recent_storage_usage_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_RECENT_STORAGE_USAGE_IND_OID,
    sql: "IN CLUSTER mz_catalog_server ON mz_catalog.mz_recent_storage_usage (object_id)",
    is_retained_metrics_object: false,
});

pub static MZ_RELATIONS: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "mz_relations",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::VIEW_MZ_RELATIONS_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_id", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("type", SqlScalarType::String.nullable(false))
            .with_column("owner_id", SqlScalarType::String.nullable(false))
            .with_column("cluster_id", SqlScalarType::String.nullable(true))
            .with_column("privileges", SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false))
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "Materialize's unique ID for the relation."),
            ("oid", "A PostgreSQL-compatible OID for the relation."),
            ("schema_id", "The ID of the schema to which the relation belongs. Corresponds to `mz_schemas.id`."),
            ("name", "The name of the relation."),
            ("type", "The type of the relation: either `table`, `source`, `view`, or `materialized-view`."),
            ("owner_id", "The role ID of the owner of the relation. Corresponds to `mz_roles.id`."),
            ("cluster_id", "The ID of the cluster maintaining the source, materialized view, index, or sink. Corresponds to `mz_clusters.id`. `NULL` for other object types."),
            ("privileges", "The privileges belonging to the relation."),
        ]),
        sql: "
      SELECT id, oid, schema_id, name, 'table' AS type, owner_id, NULL::text AS cluster_id, privileges FROM mz_catalog.mz_tables
UNION ALL SELECT id, oid, schema_id, name, 'source', owner_id, cluster_id, privileges FROM mz_catalog.mz_sources
UNION ALL SELECT id, oid, schema_id, name, 'view', owner_id, NULL::text, privileges FROM mz_catalog.mz_views
UNION ALL SELECT id, oid, schema_id, name, 'materialized-view', owner_id, cluster_id, privileges FROM mz_catalog.mz_materialized_views",
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "relation",
            description: "Union of all relation types: tables, sources, views, MVs (convenience view)",
            links: &const { [
                OntologyLink { name: "union_includes", target: "table", properties: LinkProperties::union_disc("type", "table") },
                OntologyLink { name: "union_includes", target: "source", properties: LinkProperties::union_disc("type", "source") },
                OntologyLink { name: "union_includes", target: "view", properties: LinkProperties::union_disc("type", "view") },
                OntologyLink { name: "union_includes", target: "mv", properties: LinkProperties::union_disc("type", "materialized-view") },
            ] },
            column_semantic_types: &const {[("id", SemanticType::CatalogItemId), ("oid", SemanticType::OID), ("schema_id", SemanticType::SchemaId), ("type", SemanticType::ObjectType), ("owner_id", SemanticType::RoleId), ("cluster_id", SemanticType::ClusterId)]},
        }),
    }
});

pub static MZ_OBJECTS: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "mz_objects",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::VIEW_MZ_OBJECTS_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::String.nullable(false))
            .with_column("oid", SqlScalarType::Oid.nullable(false))
            .with_column("schema_id", SqlScalarType::String.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("type", SqlScalarType::String.nullable(false))
            .with_column("owner_id", SqlScalarType::String.nullable(false))
            .with_column("cluster_id", SqlScalarType::String.nullable(true))
            .with_column("privileges", SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(true))
            .finish(),
        column_comments: BTreeMap::from_iter([
            ("id", "Materialize's unique ID for the object."),
            ("oid", "A PostgreSQL-compatible OID for the object."),
            ("schema_id", "The ID of the schema to which the object belongs. Corresponds to `mz_schemas.id`."),
            ("name", "The name of the object."),
            ("type", "The type of the object: one of `table`, `source`, `view`, `materialized-view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`."),
            ("owner_id", "The role ID of the owner of the object. Corresponds to `mz_roles.id`."),
            ("cluster_id", "The ID of the cluster maintaining the source, materialized view, index, or sink. Corresponds to `mz_clusters.id`. `NULL` for other object types."),
            ("privileges", "The privileges belonging to the object."),
        ]),
        sql:
        "SELECT id, oid, schema_id, name, type, owner_id, cluster_id, privileges FROM mz_catalog.mz_relations
UNION ALL
    SELECT id, oid, schema_id, name, 'sink', owner_id, cluster_id, NULL::mz_catalog.mz_aclitem[] FROM mz_catalog.mz_sinks
UNION ALL
    SELECT mz_indexes.id, mz_indexes.oid, mz_relations.schema_id, mz_indexes.name, 'index', mz_indexes.owner_id, mz_indexes.cluster_id, NULL::mz_catalog.mz_aclitem[]
    FROM mz_catalog.mz_indexes
    JOIN mz_catalog.mz_relations ON mz_indexes.on_id = mz_relations.id
UNION ALL
    SELECT id, oid, schema_id, name, 'connection', owner_id, NULL::text, privileges FROM mz_catalog.mz_connections
UNION ALL
    SELECT id, oid, schema_id, name, 'type', owner_id, NULL::text, privileges FROM mz_catalog.mz_types
UNION ALL
    SELECT id, oid, schema_id, name, 'function', owner_id, NULL::text, NULL::mz_catalog.mz_aclitem[] FROM mz_catalog.mz_functions
UNION ALL
    SELECT id, oid, schema_id, name, 'secret', owner_id, NULL::text, privileges FROM mz_catalog.mz_secrets
UNION ALL
    SELECT id, oid, schema_id, name, 'metric-sink', owner_id, NULL::text, privileges FROM mz_internal.mz_metric_sinks_raw",
        access: vec![PUBLIC_SELECT],
        ontology: Some(Ontology {
            entity_name: "object",
            description: "Union of all object types: relations, indexes, connections, etc. (convenience view)",
            links: &const {
                [
                    OntologyLink {
                        name: "union_includes",
                        target: "relation",
                        properties: LinkProperties::Union {
                            discriminator_column: None,
                            discriminator_value: None,
                            note: Some("covers all mz_relations rows (table, source, view, mv)"),
                        },
                    },
                    OntologyLink {
                        name: "union_includes",
                        target: "table",
                        properties: LinkProperties::union_disc("type", "table"),
                    },
                    OntologyLink {
                        name: "union_includes",
                        target: "source",
                        properties: LinkProperties::union_disc("type", "source"),
                    },
                    OntologyLink {
                        name: "union_includes",
                        target: "view",
                        properties: LinkProperties::union_disc("type", "view"),
                    },
                    OntologyLink {
                        name: "union_includes",
                        target: "mv",
                        properties: LinkProperties::union_disc("type", "materialized-view"),
                    },
                    OntologyLink {
                        name: "union_includes",
                        target: "sink",
                        properties: LinkProperties::union_disc("type", "sink"),
                    },
                    OntologyLink {
                        name: "union_includes",
                        target: "index",
                        properties: LinkProperties::union_disc("type", "index"),
                    },
                    OntologyLink {
                        name: "union_includes",
                        target: "connection",
                        properties: LinkProperties::union_disc("type", "connection"),
                    },
                    OntologyLink {
                        name: "union_includes",
                        target: "type",
                        properties: LinkProperties::union_disc("type", "type"),
                    },
                    OntologyLink {
                        name: "union_includes",
                        target: "function",
                        properties: LinkProperties::union_disc("type", "function"),
                    },
                    OntologyLink {
                        name: "union_includes",
                        target: "secret",
                        properties: LinkProperties::union_disc("type", "secret"),
                    },
                    OntologyLink {
                        name: "in_schema",
                        target: "schema",
                        properties: LinkProperties::fk("schema_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "owned_by",
                        target: "role",
                        properties: LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
                    },
                    OntologyLink {
                        name: "on_cluster",
                        target: "cluster",
                        properties: LinkProperties::fk_nullable(
                            "cluster_id",
                            "id",
                            Cardinality::ManyToOne,
                        ),
                    },
                ]
            },
            column_semantic_types: &const {
                [
                    ("id", SemanticType::CatalogItemId),
                    ("oid", SemanticType::OID),
                    ("schema_id", SemanticType::SchemaId),
                    ("type", SemanticType::ObjectType),
                    ("owner_id", SemanticType::RoleId),
                    ("cluster_id", SemanticType::ClusterId),
                ]
            },
        }),
    }
});

pub static MZ_TIMEZONE_ABBREVIATIONS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_timezone_abbreviations",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::VIEW_MZ_TIMEZONE_ABBREVIATIONS_OID,
    desc: RelationDesc::builder()
        .with_column("abbreviation", SqlScalarType::String.nullable(false))
        .with_column("utc_offset", SqlScalarType::Interval.nullable(true))
        .with_column("dst", SqlScalarType::Bool.nullable(true))
        .with_column("timezone_name", SqlScalarType::String.nullable(true))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("abbreviation", "The timezone abbreviation."),
        (
            "utc_offset",
            "The UTC offset of the timezone or `NULL` if fixed.",
        ),
        (
            "dst",
            "Whether the timezone is in daylight savings or `NULL` if fixed.",
        ),
        (
            "timezone_name",
            "The full name of the non-fixed timezone or `NULL` if not fixed.",
        ),
    ]),
    sql: format!(
        "SELECT * FROM ({}) _ (abbreviation, utc_offset, dst, timezone_name)",
        mz_pgtz::abbrev::MZ_CATALOG_TIMEZONE_ABBREVIATIONS_SQL,
    )
    .leak(),
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub static MZ_TIMEZONE_NAMES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_timezone_names",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::VIEW_MZ_TIMEZONE_NAMES_OID,
    desc: RelationDesc::builder()
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([("name", "The timezone name.")]),
    sql: format!(
        "SELECT * FROM ({}) _ (name)",
        mz_pgtz::timezone::MZ_CATALOG_TIMEZONE_NAMES_SQL,
    )
    .leak(),
    access: vec![PUBLIC_SELECT],
    ontology: None,
});

pub const MZ_DATABASES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_databases_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_DATABASES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_databases (name)",
    is_retained_metrics_object: false,
};

pub const MZ_SCHEMAS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_schemas_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_SCHEMAS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_schemas (database_id)",
    is_retained_metrics_object: false,
};

pub const MZ_CONNECTIONS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_connections_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_CONNECTIONS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_connections (schema_id)",
    is_retained_metrics_object: false,
};

pub const MZ_TABLES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_tables_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_TABLES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_tables (schema_id)",
    is_retained_metrics_object: false,
};

pub const MZ_TYPES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_types_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_TYPES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_types (schema_id)",
    is_retained_metrics_object: false,
};

pub const MZ_OBJECTS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_objects_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_OBJECTS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_objects (schema_id)",
    is_retained_metrics_object: false,
};

pub const MZ_COLUMNS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_columns_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_COLUMNS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_columns (name)",
    is_retained_metrics_object: false,
};

pub const MZ_SECRETS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_secrets_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_SECRETS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_secrets (name)",
    is_retained_metrics_object: false,
};

pub const MZ_VIEWS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_views_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_VIEWS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_views (schema_id)",
    is_retained_metrics_object: false,
};

pub const MZ_CLUSTERS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_clusters_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_CLUSTERS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_clusters (id)",
    is_retained_metrics_object: false,
};

pub const MZ_INDEXES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_indexes_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_INDEXES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_indexes (id)",
    is_retained_metrics_object: false,
};

pub const MZ_ROLES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_roles_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_ROLES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_roles (id)",
    is_retained_metrics_object: false,
};

pub const MZ_SOURCES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_sources_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_SOURCES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_sources (id)",
    is_retained_metrics_object: true,
};

pub const MZ_SINKS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_sinks_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_SINKS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_sinks (id)",
    is_retained_metrics_object: true,
};

pub const MZ_MATERIALIZED_VIEWS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_materialized_views_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_MATERIALIZED_VIEWS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_materialized_views (id)",
    is_retained_metrics_object: false,
};

pub const MZ_CLUSTER_REPLICAS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_cluster_replicas_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_CLUSTER_REPLICAS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_cluster_replicas (id)",
    is_retained_metrics_object: true,
};

pub const MZ_CLUSTER_REPLICA_SIZES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_cluster_replica_sizes_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_CLUSTER_REPLICA_SIZES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_cluster_replica_sizes (size)",
    is_retained_metrics_object: true,
};

pub const MZ_KAFKA_SOURCES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_kafka_sources_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_KAFKA_SOURCES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_kafka_sources (id)",
    is_retained_metrics_object: true,
};

#[cfg(test)]
mod tests {
    use mz_catalog_protos::objects::ObjectType as ProtoObjectType;
    use mz_sql::catalog::ObjectType as SqlObjectType;

    use crate::builtin::mz_catalog::MZ_DEFAULT_PRIVILEGES;

    /// Checks the `object_type` CASE in `MZ_DEFAULT_PRIVILEGES` against the
    /// proto enum and the SQL Display impl it's meant to mirror.
    ///
    /// The CASE maps `mz_catalog_protos::ObjectType`'s numeric `Serialize_repr`
    /// to `mz_sql::catalog::ObjectType`'s `Display`, lowercased. The match in
    /// `expected_for` is exhaustive, so a new proto variant won't compile until
    /// it's handled. A wrong string in the CASE fails the substring assertion.
    #[mz_ore::test]
    fn object_type_case_matches_proto_display() {
        // Returns `None` for proto variants that never appear in stored
        // `DefaultPrivilege` keys (currently just `Unknown`, the zero-value
        // sentinel). Match is exhaustive: a new variant forces an update.
        fn expected_for(proto: ProtoObjectType) -> Option<SqlObjectType> {
            match proto {
                ProtoObjectType::Unknown => None,
                ProtoObjectType::Table => Some(SqlObjectType::Table),
                ProtoObjectType::View => Some(SqlObjectType::View),
                ProtoObjectType::MaterializedView => Some(SqlObjectType::MaterializedView),
                ProtoObjectType::Source => Some(SqlObjectType::Source),
                ProtoObjectType::Sink => Some(SqlObjectType::Sink),
                ProtoObjectType::Index => Some(SqlObjectType::Index),
                ProtoObjectType::Type => Some(SqlObjectType::Type),
                ProtoObjectType::Role => Some(SqlObjectType::Role),
                ProtoObjectType::Cluster => Some(SqlObjectType::Cluster),
                ProtoObjectType::ClusterReplica => Some(SqlObjectType::ClusterReplica),
                ProtoObjectType::Secret => Some(SqlObjectType::Secret),
                ProtoObjectType::Connection => Some(SqlObjectType::Connection),
                ProtoObjectType::Database => Some(SqlObjectType::Database),
                ProtoObjectType::Schema => Some(SqlObjectType::Schema),
                ProtoObjectType::Func => Some(SqlObjectType::Func),
                ProtoObjectType::NetworkPolicy => Some(SqlObjectType::NetworkPolicy),
            }
        }

        // The proto enum has no `IntoEnumIterator`, so list every variant by
        // hand. A missed entry here just goes unchecked, but the exhaustive
        // match in `expected_for` won't compile when a new proto variant lands,
        // which is the actual drift defense.
        let variants: &[ProtoObjectType] = &[
            ProtoObjectType::Unknown,
            ProtoObjectType::Table,
            ProtoObjectType::View,
            ProtoObjectType::MaterializedView,
            ProtoObjectType::Source,
            ProtoObjectType::Sink,
            ProtoObjectType::Index,
            ProtoObjectType::Type,
            ProtoObjectType::Role,
            ProtoObjectType::Cluster,
            ProtoObjectType::ClusterReplica,
            ProtoObjectType::Secret,
            ProtoObjectType::Connection,
            ProtoObjectType::Database,
            ProtoObjectType::Schema,
            ProtoObjectType::Func,
            ProtoObjectType::NetworkPolicy,
        ];

        let sql = MZ_DEFAULT_PRIVILEGES.sql;
        for &proto in variants {
            // `ObjectType` is `#[repr(u8)]`; the discriminant matches the
            // `Serialize_repr` JSON value the SQL CASE arms compare against.
            #[allow(clippy::as_conversions)]
            let repr = proto as u8;
            match expected_for(proto) {
                Some(sql_ty) => {
                    let display = sql_ty.to_string().to_lowercase();
                    // The arms use variable whitespace between `WHEN '..'` and
                    // `THEN` to align columns; verify the WHEN and THEN strings
                    // appear together with at most one or two spaces between.
                    let one_space = format!("WHEN '{repr}' THEN '{display}'");
                    let two_spaces = format!("WHEN '{repr}'  THEN '{display}'");
                    assert!(
                        sql.contains(&one_space) || sql.contains(&two_spaces),
                        "missing CASE arm for `{proto:?}`: expected \
                         `WHEN '{repr}' THEN '{display}'` (or with double space) \
                         in MZ_DEFAULT_PRIVILEGES.sql",
                    );
                }
                None => {
                    let pattern = format!("WHEN '{repr}'");
                    assert!(
                        !sql.contains(&pattern),
                        "unexpected CASE arm for sentinel `{proto:?}`: \
                         found `{pattern}` in MZ_DEFAULT_PRIVILEGES.sql",
                    );
                }
            }
        }
    }
}
