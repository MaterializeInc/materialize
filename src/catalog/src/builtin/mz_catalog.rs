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

use mz_pgrepr::oid;
use mz_repr::namespaces::MZ_CATALOG_SCHEMA;
use mz_repr::{RelationDesc, SemanticType, SqlScalarType};
use mz_sql::catalog::{
    CatalogType, CatalogTypeDetails, CatalogTypePgMetadata, NameReference, ObjectType,
};
use mz_sql::rbac;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_storage_client::controller::IntrospectionType;

use super::{
    BuiltinIndex, BuiltinMaterializedView, BuiltinSource, BuiltinTable, BuiltinType, BuiltinView,
    Cardinality, LinkProperties, Ontology, OntologyLink, PUBLIC_SELECT,
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
pub static MZ_KAFKA_SOURCES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_kafka_sources",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_KAFKA_SOURCES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("group_id_prefix", SqlScalarType::String.nullable(false))
        .with_column("topic", SqlScalarType::String.nullable(false))
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
pub static MZ_INDEXES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_indexes",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_INDEXES_OID,
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
});
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
pub static MZ_SOURCES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_sources",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_SOURCES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("schema_id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("type", SqlScalarType::String.nullable(false))
        .with_column("connection_id", SqlScalarType::String.nullable(true))
        .with_column("size", SqlScalarType::String.nullable(true))
        .with_column("envelope_type", SqlScalarType::String.nullable(true))
        .with_column("key_format", SqlScalarType::String.nullable(true))
        .with_column("value_format", SqlScalarType::String.nullable(true))
        .with_column("cluster_id", SqlScalarType::String.nullable(true))
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
        ("id", "Materialize's unique ID for the source."),
        ("oid", "A PostgreSQL-compatible OID for the source."),
        (
            "schema_id",
            "The ID of the schema to which the source belongs. Corresponds to `mz_schemas.id`.",
        ),
        ("name", "The name of the source."),
        (
            "type",
            "The type of the source: `kafka`, `mysql`, `postgres`, `load-generator`, `progress`, or `subsource`.",
        ),
        (
            "connection_id",
            "The ID of the connection associated with the source, if any. Corresponds to `mz_connections.id`.",
        ),
        ("size", "*Deprecated* The size of the source."),
        (
            "envelope_type",
            "For Kafka sources, the envelope type: `none`, `upsert`, or `debezium`. `NULL` for other source types.",
        ),
        (
            "key_format",
            "For Kafka sources, the format of the Kafka message key: `avro`, `csv`, `regex`, `bytes`, `json`, `text`, or `NULL`.",
        ),
        (
            "value_format",
            "For Kafka sources, the format of the Kafka message value: `avro`, `csv`, `regex`, `bytes`, `json`, `text`. `NULL` for other source types.",
        ),
        (
            "cluster_id",
            "The ID of the cluster maintaining the source. Corresponds to `mz_clusters.id`.",
        ),
        (
            "owner_id",
            "The role ID of the owner of the source. Corresponds to `mz_roles.id`.",
        ),
        ("privileges", "The privileges granted on the source."),
        ("create_sql", "The `CREATE` SQL statement for the source."),
        (
            "redacted_create_sql",
            "The redacted `CREATE` SQL statement for the source.",
        ),
    ]),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "source",
        description: "An external data source ingested into Materialize (e.g., Kafka, Postgres)",
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
                    properties: LinkProperties::fk_nullable(
                        "cluster_id",
                        "id",
                        Cardinality::ManyToOne,
                    ),
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
                ("type", SemanticType::SourceType),
                ("connection_id", SemanticType::CatalogItemId),
                ("cluster_id", SemanticType::ClusterId),
                ("owner_id", SemanticType::RoleId),
                ("create_sql", SemanticType::SqlDefinition),
                ("redacted_create_sql", SemanticType::RedactedSqlDefinition),
            ]
        },
    }),
});
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
pub static MZ_ROLES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_roles",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_ROLES_OID,
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
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "role",
        description: "A user or role for authentication and access control",
        links: &const { [] },
        column_semantic_types: &const { [("id", SemanticType::RoleId), ("oid", SemanticType::OID)] },
    }),
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

pub static MZ_ROLE_PARAMETERS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_role_parameters",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_ROLE_PARAMETERS_OID,
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

pub static MZ_CLUSTERS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_clusters",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_CLUSTERS_OID,
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

pub static MZ_CLUSTER_REPLICAS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_cluster_replicas",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_CLUSTER_REPLICAS_OID,
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
            "The availability zone in which the cluster is running.",
        ),
        (
            "owner_id",
            "The role ID of the owner of the cluster replica. Corresponds to `mz_roles.id`.",
        ),
        ("disk", "If the replica has a local disk."),
    ]),
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
                    properties: LinkProperties::fk_nullable("size", "size", Cardinality::ManyToOne),
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

pub static MZ_AUDIT_EVENTS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_audit_events",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_AUDIT_EVENTS_OID,
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
            "The type of the event: `create`, `drop`, or `alter`.",
        ),
        (
            "object_type",
            "The type of the affected object: `cluster`, `cluster-replica`, `connection`, `database`, `function`, `index`, `materialized-view`, `role`, `schema`, `secret`, `sink`, `source`, `table`, `type`, or `view`.",
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

pub static MZ_DEFAULT_PRIVILEGES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_default_privileges",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_DEFAULT_PRIVILEGES_OID,
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
});

pub static MZ_SYSTEM_PRIVILEGES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_system_privileges",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_SYSTEM_PRIVILEGES_OID,
    desc: RelationDesc::builder()
        .with_column("privileges", SqlScalarType::MzAclItem.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([(
        "privileges",
        "The privileges belonging to the system.",
    )]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
    ontology: Some(Ontology {
        entity_name: "system_privilege",
        description: "A system-level privilege grant",
        links: &const { [] },
        column_semantic_types: &[],
    }),
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
    SELECT id, oid, schema_id, name, 'secret', owner_id, NULL::text, privileges FROM mz_catalog.mz_secrets",
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
