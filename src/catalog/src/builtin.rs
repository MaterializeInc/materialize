// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Built-in catalog items.
//!
//! Builtins exist in the `mz_catalog` ambient schema. They are automatically
//! installed into the catalog when it is opened. Their definitions are not
//! persisted in the catalog, but hardcoded in this module. This makes it easy
//! to add new builtins, or change the definition of existing builtins, in new
//! versions of Materialize.
//!
//! Builtin's names, columns, and types are part of the stable API of
//! Materialize. Be careful to maintain backwards compatibility when changing
//! definitions of existing builtins!
//!
//! More information about builtin system tables and types can be found in
//! <https://materialize.com/docs/sql/system-catalog/>.

mod builtin;
pub mod notice;
mod ontology;
mod pg_catalog;
pub use pg_catalog::*;
mod mz_catalog;
pub use mz_catalog::*;
mod mz_internal;
pub use mz_internal::*;
mod mz_introspection;
pub use mz_introspection::*;
mod information_schema;
pub use information_schema::*;

use std::collections::BTreeMap;
use std::hash::Hash;
use std::string::ToString;
use std::sync::LazyLock;

use mz_compute_client::logging::LogVariant;
use mz_ore::collections::HashMap;
use mz_pgrepr::oid;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::namespaces::{
    INFORMATION_SCHEMA, MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA, MZ_UNSAFE_SCHEMA, PG_CATALOG_SCHEMA,
};
use mz_repr::role_id::RoleId;
use mz_repr::{RelationDesc, SemanticType, SqlRelationType};
use mz_sql::catalog::RoleAttributesRaw;
use mz_sql::catalog::{
    CatalogItemType, CatalogTypeDetails, NameReference, ObjectType, SystemObjectType, TypeReference,
};
use mz_sql::rbac;
use mz_sql::session::user::{
    ANALYTICS_USER_NAME, JWT_SYNC_ROLE_NAME, MZ_ANALYTICS_ROLE_ID, MZ_JWT_SYNC_ROLE_ID,
    MZ_MONITOR_REDACTED_ROLE_ID, MZ_MONITOR_ROLE_ID, MZ_SUPPORT_ROLE_ID, MZ_SYSTEM_ROLE_ID,
    SUPPORT_USER_NAME, SYSTEM_USER_NAME,
};
use serde::Serialize;

use crate::durable::objects::SystemObjectDescription;
use crate::memory::objects::DataSourceDesc;

pub const BUILTIN_PREFIXES: &[&str] = &["mz_", "pg_", "external_"];
const BUILTIN_CLUSTER_REPLICA_NAME: &str = "r1";

/// A sentinel used in place of a fingerprint that indicates that a builtin
/// object is runtime alterable. Runtime alterable objects don't have meaningful
/// fingerprints because they may have been intentionally changed by the user
/// after creation.
// NOTE(benesch): ideally we'd use a fingerprint type that used a sum type
// rather than a loosely typed string to represent the runtime alterable
// state like so:
//
//     enum Fingerprint {
//         SqlText(String),
//         RuntimeAlterable,
//     }
//
// However, that would entail a complicated migration for the existing system object
// mapping collection stored on disk.
pub const RUNTIME_ALTERABLE_FINGERPRINT_SENTINEL: &str = "<RUNTIME-ALTERABLE>";

#[derive(Clone, Debug)]
pub enum Builtin<T: 'static + TypeReference> {
    Log(&'static BuiltinLog),
    Table(&'static BuiltinTable),
    View(&'static BuiltinView),
    MaterializedView(&'static BuiltinMaterializedView),
    Type(&'static BuiltinType<T>),
    Func(BuiltinFunc),
    Source(&'static BuiltinSource),
    Index(&'static BuiltinIndex),
    Connection(&'static BuiltinConnection),
}

impl<T: TypeReference> Builtin<T> {
    pub fn name(&self) -> &'static str {
        match self {
            Builtin::Log(log) => log.name,
            Builtin::Table(table) => table.name,
            Builtin::View(view) => view.name,
            Builtin::MaterializedView(mv) => mv.name,
            Builtin::Type(typ) => typ.name,
            Builtin::Func(func) => func.name,
            Builtin::Source(coll) => coll.name,
            Builtin::Index(index) => index.name,
            Builtin::Connection(connection) => connection.name,
        }
    }

    pub fn schema(&self) -> &'static str {
        match self {
            Builtin::Log(log) => log.schema,
            Builtin::Table(table) => table.schema,
            Builtin::View(view) => view.schema,
            Builtin::MaterializedView(mv) => mv.schema,
            Builtin::Type(typ) => typ.schema,
            Builtin::Func(func) => func.schema,
            Builtin::Source(coll) => coll.schema,
            Builtin::Index(index) => index.schema,
            Builtin::Connection(connection) => connection.schema,
        }
    }

    pub fn catalog_item_type(&self) -> CatalogItemType {
        match self {
            Builtin::Log(_) => CatalogItemType::Source,
            Builtin::Source(_) => CatalogItemType::Source,
            Builtin::Table(_) => CatalogItemType::Table,
            Builtin::View(_) => CatalogItemType::View,
            Builtin::MaterializedView(_) => CatalogItemType::MaterializedView,
            Builtin::Type(_) => CatalogItemType::Type,
            Builtin::Func(_) => CatalogItemType::Func,
            Builtin::Index(_) => CatalogItemType::Index,
            Builtin::Connection(_) => CatalogItemType::Connection,
        }
    }

    /// Whether the object can be altered at runtime by its owner.
    pub fn runtime_alterable(&self) -> bool {
        match self {
            Builtin::Connection(c) => c.runtime_alterable,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Hash, Serialize)]
pub struct BuiltinLog {
    pub variant: LogVariant,
    pub name: &'static str,
    pub schema: &'static str,
    pub oid: u32,
    /// ACL items to apply to the object
    pub access: Vec<MzAclItem>,
    #[serde(default)]
    pub ontology: Option<Ontology>,
}

/// Ontology metadata for a builtin catalog object.
///
/// When present on a builtin, it marks it as an ontology entity with an explicit
/// `entity_name`, `description`, and optional per-column semantic type annotations.
///
/// ## Why `column_semantic_types` lives here and not in `RelationDesc`
///
/// Semantic types are pure catalog-level metadata: they annotate what an ID
/// column *means* (e.g. "this is a ClusterId") without affecting the Arrow
/// data type used for encoding. Keeping them in `RelationDesc` would cause
/// persist schema mismatches during zero-downtime upgrades: the old binary
/// registers a schema without semantic types, the new binary tries to register
/// a schema with them, and `register_schema` returns `None` because the schemas
/// are not `PartialEq`. Since the only consumers of semantic types are the
/// ontology views (which already have access to `Ontology`), storing them here
/// is both correct and avoids the schema-evolution problem entirely.
#[derive(Clone, Hash, Debug, PartialEq, Eq, Serialize)]
pub struct Ontology {
    /// The ontology entity name (e.g., "database", "table", "mv"). Names a
    /// single row of this relation, so prefer singular event/object nouns
    /// (e.g., "replica_status_event" not "replica_status_history").
    pub entity_name: &'static str,
    /// One-line description of this entity.
    pub description: &'static str,
    /// Relationships originating from this entity (foreign keys, unions,
    /// mappings, dependencies, metrics).
    pub links: &'static [OntologyLink],
    /// Per-column semantic type annotations: `(column_name, SemanticType)`.
    /// Only columns that carry a meaningful semantic type need to appear here.
    pub column_semantic_types: &'static [(&'static str, SemanticType)],
}

/// Cardinality of an ontology link.
#[derive(
    Clone,
    Copy,
    Debug,
    Hash,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize
)]
#[serde(rename_all = "snake_case")]
pub enum Cardinality {
    OneToOne,
    ManyToOne,
}

/// Helper used by serde to skip serializing `false` boolean fields.
fn is_false(v: &bool) -> bool {
    !v
}

/// Typed properties for an ontology link. Serialized to the `properties` JSONB
/// column in `mz_ontology_link_types`. The `kind` field is inlined from the
/// enum variant name via `#[serde(tag = "kind")]`.
///
/// Choosing the right variant matters:
///
/// - [`LinkProperties::ForeignKey`]: the source entity has a column whose
///   value is an ID that directly references a row in the target entity.
///   Use this when there is an explicit FK column (e.g. `schema_id` ->
///   `schema`).
/// - [`LinkProperties::DependsOn`]: this entity logically depends on the
///   target entity via a graph-edge table (e.g. `mz_compute_dependencies`
///   records that a compute object depends on another object). The
///   `source_column` is the column **in this entity** that holds the
///   dependent's ID; `target_column` is the column in the target entity
///   being depended upon. Use this for dependency-graph tables, **not**
///   `ForeignKey`.
/// - [`LinkProperties::Union`]: the source entity is a superset view that
///   contains the target entity as a subset, optionally filtered by a
///   discriminator column.
/// - [`LinkProperties::MapsTo`]: the source entity provides an ID translation
///   to the target entity, possibly via an intermediate table or across ID
///   namespaces.
/// - [`LinkProperties::Measures`]: the source entity records metric
///   measurements about the target entity.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LinkProperties {
    /// A foreign-key relationship: `source_column` in the source entity
    /// references `target_column` in the target entity.
    ForeignKey {
        /// Column in the source entity that holds the reference.
        source_column: &'static str,
        /// Column in the target entity being referenced (usually `id`).
        target_column: &'static str,
        /// How many source rows may reference a single target row.
        cardinality: Cardinality,
        /// Semantic type of the source column, if it carries an ID that
        /// requires type-aware resolution (e.g. `CatalogItemId`, `GlobalId`).
        #[serde(skip_serializing_if = "Option::is_none")]
        source_id_type: Option<mz_repr::SemanticType>,
        /// Intermediate mapping relation needed when `source_id_type` does not
        /// directly match the target entity's ID type (e.g.
        /// `mz_internal.mz_object_global_ids` to go from `GlobalId` to catalog
        /// object).
        #[serde(skip_serializing_if = "Option::is_none")]
        requires_mapping: Option<&'static str>,
        /// True when the source column may be NULL (the reference is optional).
        #[serde(default, skip_serializing_if = "is_false")]
        nullable: bool,
        /// Free-form annotation for cases that need extra context.
        #[serde(skip_serializing_if = "Option::is_none")]
        note: Option<&'static str>,
        /// Additional `(source_column, target_column)` pairs that together with
        /// `source_column`/`target_column` form a composite join key. Used for
        /// `_per_worker` entities whose primary key is `(id, worker_id)`, and
        /// for message-count raw relations whose join key includes worker IDs.
        /// Serialized as an array; omitted when `None`.
        #[serde(skip_serializing_if = "Option::is_none")]
        extra_key_columns: Option<&'static [(&'static str, &'static str)]>,
    },
    /// A union relationship: the source entity is a superset view that includes
    /// the target entity, optionally filtered by a discriminator column/value.
    Union {
        /// Column used to discriminate between subtypes (e.g. `type`).
        #[serde(skip_serializing_if = "Option::is_none")]
        discriminator_column: Option<&'static str>,
        /// Value of `discriminator_column` that selects the target entity.
        #[serde(skip_serializing_if = "Option::is_none")]
        discriminator_value: Option<&'static str>,
        /// Free-form annotation for cases that need extra context.
        #[serde(skip_serializing_if = "Option::is_none")]
        note: Option<&'static str>,
    },
    /// A mapping relationship: the source entity maps to the target entity,
    /// optionally via an intermediate table and/or with an ID-type conversion.
    MapsTo {
        /// Column in the source entity that holds the ID to map from.
        source_column: &'static str,
        /// Column in the target entity being mapped to.
        target_column: &'static str,
        /// Intermediate relation used to perform the mapping.
        #[serde(skip_serializing_if = "Option::is_none")]
        via: Option<&'static str>,
        /// Semantic type of the source ID before mapping.
        #[serde(skip_serializing_if = "Option::is_none")]
        from_type: Option<mz_repr::SemanticType>,
        /// Semantic type of the target ID after mapping.
        #[serde(skip_serializing_if = "Option::is_none")]
        to_type: Option<mz_repr::SemanticType>,
        /// Free-form annotation for cases that need extra context.
        #[serde(skip_serializing_if = "Option::is_none")]
        note: Option<&'static str>,
    },
    /// A dependency relationship: this entity directly depends on the
    /// target entity (e.g. a materialization that references an object).
    DependsOn {
        /// Column in this entity that holds the dependency ID.
        source_column: &'static str,
        /// Column in the target entity being depended upon (usually `id`).
        target_column: &'static str,
        /// Semantic type of the source column.
        #[serde(skip_serializing_if = "Option::is_none")]
        source_id_type: Option<mz_repr::SemanticType>,
        /// Intermediate mapping relation needed when `source_id_type` does not
        /// directly match the target entity's ID type (e.g. GlobalId →
        /// `mz_internal.mz_object_global_ids` to reach a catalog object).
        #[serde(skip_serializing_if = "Option::is_none")]
        requires_mapping: Option<&'static str>,
    },
    /// A metric relationship: the source entity records measurements of a named
    /// metric on the target entity.
    Measures {
        /// Column in the source entity that references the target entity.
        source_column: &'static str,
        /// Column in the target entity being measured (usually `id`).
        target_column: &'static str,
        /// Name of the metric being measured (e.g. `cpu_time_ns`).
        metric: &'static str,
        /// Semantic type of the source column, if ID-type resolution is needed.
        #[serde(skip_serializing_if = "Option::is_none")]
        source_id_type: Option<mz_repr::SemanticType>,
        /// Intermediate mapping relation needed when the source ID type differs
        /// from the target entity's ID type.
        #[serde(skip_serializing_if = "Option::is_none")]
        requires_mapping: Option<&'static str>,
        /// Free-form annotation for cases that need extra context.
        #[serde(skip_serializing_if = "Option::is_none")]
        note: Option<&'static str>,
        /// Additional `(source_column, target_column)` pairs that together with
        /// `source_column`/`target_column` form a composite join key.
        #[serde(skip_serializing_if = "Option::is_none")]
        extra_key_columns: Option<&'static [(&'static str, &'static str)]>,
    },
}

impl LinkProperties {
    /// Basic foreign-key link with no optional fields set.
    pub const fn fk(
        source_column: &'static str,
        target_column: &'static str,
        cardinality: Cardinality,
    ) -> Self {
        Self::ForeignKey {
            source_column,
            target_column,
            cardinality,
            source_id_type: None,
            requires_mapping: None,
            nullable: false,
            note: None,
            extra_key_columns: None,
        }
    }

    /// Foreign-key link where the source column may be NULL.
    pub const fn fk_nullable(
        source_column: &'static str,
        target_column: &'static str,
        cardinality: Cardinality,
    ) -> Self {
        Self::ForeignKey {
            source_column,
            target_column,
            cardinality,
            source_id_type: None,
            requires_mapping: None,
            nullable: true,
            note: None,
            extra_key_columns: None,
        }
    }

    /// Foreign-key link whose source column carries a typed ID (e.g.
    /// `CatalogItemId`) but does not require an intermediate mapping table.
    pub const fn fk_typed(
        source_column: &'static str,
        target_column: &'static str,
        cardinality: Cardinality,
        source_id_type: mz_repr::SemanticType,
    ) -> Self {
        Self::ForeignKey {
            source_column,
            target_column,
            cardinality,
            source_id_type: Some(source_id_type),
            requires_mapping: None,
            nullable: false,
            note: None,
            extra_key_columns: None,
        }
    }

    /// Foreign-key link whose source column carries a typed ID that requires
    /// an intermediate mapping table to resolve (e.g. `GlobalId` →
    /// `mz_internal.mz_object_global_ids`).
    pub const fn fk_mapped(
        source_column: &'static str,
        target_column: &'static str,
        cardinality: Cardinality,
        source_id_type: mz_repr::SemanticType,
        requires_mapping: &'static str,
    ) -> Self {
        Self::ForeignKey {
            source_column,
            target_column,
            cardinality,
            source_id_type: Some(source_id_type),
            requires_mapping: Some(requires_mapping),
            nullable: false,
            note: None,
            extra_key_columns: None,
        }
    }

    /// Foreign-key link with a composite join key. `extra_key_columns` lists
    /// additional `(source_column, target_column)` pairs beyond the primary
    /// `source_column`/`target_column` pair. Examples:
    /// - `&[("worker_id", "worker_id")]` for `_per_worker` entities
    /// - `&[("from_worker_id", "worker_id")]` for message-count raw relations
    pub const fn fk_composite(
        source_column: &'static str,
        target_column: &'static str,
        cardinality: Cardinality,
        extra_key_columns: &'static [(&'static str, &'static str)],
    ) -> Self {
        Self::ForeignKey {
            source_column,
            target_column,
            cardinality,
            source_id_type: None,
            requires_mapping: None,
            nullable: false,
            note: None,
            extra_key_columns: Some(extra_key_columns),
        }
    }

    /// Union link filtered by a discriminator column/value pair.
    pub const fn union_disc(
        discriminator_column: &'static str,
        discriminator_value: &'static str,
    ) -> Self {
        Self::Union {
            discriminator_column: Some(discriminator_column),
            discriminator_value: Some(discriminator_value),
            note: None,
        }
    }

    /// Basic measures link with no optional fields set.
    pub const fn measures(
        source_column: &'static str,
        target_column: &'static str,
        metric: &'static str,
    ) -> Self {
        Self::Measures {
            source_column,
            target_column,
            metric,
            source_id_type: None,
            requires_mapping: None,
            note: None,
            extra_key_columns: None,
        }
    }

    /// Measures link with a composite join key.
    pub const fn measures_composite(
        source_column: &'static str,
        target_column: &'static str,
        metric: &'static str,
        extra_key_columns: &'static [(&'static str, &'static str)],
    ) -> Self {
        Self::Measures {
            source_column,
            target_column,
            metric,
            source_id_type: None,
            requires_mapping: None,
            note: None,
            extra_key_columns: Some(extra_key_columns),
        }
    }

    /// Measures link whose source ID requires an intermediate mapping table.
    pub const fn measures_mapped(
        source_column: &'static str,
        target_column: &'static str,
        metric: &'static str,
        source_id_type: mz_repr::SemanticType,
        requires_mapping: &'static str,
    ) -> Self {
        Self::Measures {
            source_column,
            target_column,
            metric,
            source_id_type: Some(source_id_type),
            requires_mapping: Some(requires_mapping),
            note: None,
            extra_key_columns: None,
        }
    }
}

/// A directed relationship from one ontology entity to another.
///
/// Each link has a `name` (the relationship label, e.g. `"owned_by"`), a
/// `target` entity name, and a [`LinkProperties`] variant that captures the
/// kind of relationship.
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize)]
pub struct OntologyLink {
    /// Relationship name describing the relationship FROM this entity TO the
    /// target (e.g., `"owned_by"` means "this entity is owned by the target",
    /// `"depends_on"` means "this entity depends on the target"). When the
    /// same name appears on multiple links of the same entity, all links
    /// share that relationship role (e.g., several `"union_includes"` links).
    pub name: &'static str,
    /// Target entity name (e.g., "role", "schema").
    pub target: &'static str,
    /// Typed properties for the `properties` JSONB column.
    pub properties: LinkProperties,
}

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub struct BuiltinTable {
    pub name: &'static str,
    pub schema: &'static str,
    pub oid: u32,
    pub desc: RelationDesc,
    pub column_comments: BTreeMap<&'static str, &'static str>,
    /// Whether the table's retention policy is controlled by
    /// the system variable `METRICS_RETENTION`
    pub is_retained_metrics_object: bool,
    /// ACL items to apply to the object
    pub access: Vec<MzAclItem>,
    /// Ontology metadata. None means this builtin is not an ontology entity.
    pub ontology: Option<Ontology>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuiltinSource {
    pub name: &'static str,
    pub schema: &'static str,
    pub oid: u32,
    pub desc: RelationDesc,
    pub column_comments: BTreeMap<&'static str, &'static str>,
    pub data_source: DataSourceDesc,
    /// Whether the source's retention policy is controlled by
    /// the system variable `METRICS_RETENTION`
    pub is_retained_metrics_object: bool,
    /// ACL items to apply to the object
    pub access: Vec<MzAclItem>,
    /// Ontology metadata. None means this builtin is not an ontology entity.
    pub ontology: Option<Ontology>,
}

#[derive(Hash, Debug)]
pub struct BuiltinView {
    pub name: &'static str,
    pub schema: &'static str,
    pub oid: u32,
    pub desc: RelationDesc,
    pub column_comments: BTreeMap<&'static str, &'static str>,
    pub sql: &'static str,
    /// ACL items to apply to the object
    pub access: Vec<MzAclItem>,
    /// Ontology metadata. None means this builtin is not an ontology entity.
    pub ontology: Option<Ontology>,
}

impl BuiltinView {
    pub fn create_sql(&self) -> String {
        format!("CREATE VIEW {}.{} AS {}", self.schema, self.name, self.sql)
    }
}

#[derive(Hash, Debug)]
pub struct BuiltinMaterializedView {
    pub name: &'static str,
    pub schema: &'static str,
    pub oid: u32,
    pub desc: RelationDesc,
    pub column_comments: BTreeMap<&'static str, &'static str>,
    /// SQL fragment for the MV, following `CREATE MATERIALIZED VIEW [name]`
    ///
    /// Format: `IN CLUSTER [cluster_name] AS [query]`
    pub sql: &'static str,
    /// Whether the MV's retention policy is controlled by
    /// the system variable `METRICS_RETENTION`
    pub is_retained_metrics_object: bool,
    /// ACL items to apply to the object
    pub access: Vec<MzAclItem>,
    /// Ontology metadata. None means this builtin is not an ontology entity.
    pub ontology: Option<Ontology>,
}

impl BuiltinMaterializedView {
    pub fn create_sql(&self) -> String {
        format!(
            "CREATE MATERIALIZED VIEW {}.{} {}",
            self.schema, self.name, self.sql
        )
    }
}

#[derive(Debug)]
pub struct BuiltinType<T: TypeReference> {
    pub name: &'static str,
    pub schema: &'static str,
    pub oid: u32,
    pub details: CatalogTypeDetails<T>,
}

#[derive(Clone, Debug)]
pub struct BuiltinFunc {
    pub schema: &'static str,
    pub name: &'static str,
    pub inner: &'static mz_sql::func::Func,
}

/// Note: When creating a built-in index, it's usually best to choose a key that has only one
/// component. For example, if you created an index
/// `ON mz_internal.mz_object_lifetimes (id, object_type)`, then this index couldn't be used for a
/// lookup for `WHERE object_type = ...`, and neither for joins keyed on just `id`.
/// See <https://materialize.com/docs/transform-data/optimization/#matching-multi-column-indexes-to-multi-column-where-clauses>
#[derive(Debug)]
pub struct BuiltinIndex {
    pub name: &'static str,
    pub schema: &'static str,
    pub oid: u32,
    /// SQL fragment for the index, following `CREATE INDEX [name]`
    ///
    /// Format: `IN CLUSTER [cluster_name] ON [table_name] ([column_exprs])`
    pub sql: &'static str,
    pub is_retained_metrics_object: bool,
}

impl BuiltinIndex {
    pub fn create_sql(&self) -> String {
        format!("CREATE INDEX {}\n{}", self.name, self.sql)
    }
}

#[derive(Hash, Debug)]
pub struct BuiltinConnection {
    pub name: &'static str,
    pub schema: &'static str,
    pub oid: u32,
    pub sql: &'static str,
    pub access: &'static [MzAclItem],
    pub owner_id: &'static RoleId,
    /// Whether the object can be altered at runtime by its owner.
    ///
    /// Note that when `runtime_alterable` is true, changing the `sql` in future
    /// versions does not trigger a migration.
    pub runtime_alterable: bool,
}

#[derive(Clone, Debug)]
pub struct BuiltinRole {
    pub id: RoleId,
    /// Name of the builtin role.
    ///
    /// IMPORTANT: Must start with a prefix from [`BUILTIN_PREFIXES`].
    pub name: &'static str,
    pub oid: u32,
    pub attributes: RoleAttributesRaw,
}

#[derive(Clone, Debug)]
pub struct BuiltinCluster {
    /// Name of the cluster.
    ///
    /// IMPORTANT: Must start with a prefix from [`BUILTIN_PREFIXES`].
    pub name: &'static str,
    pub privileges: &'static [MzAclItem],
    pub owner_id: &'static RoleId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuiltinClusterReplica {
    /// Name of the compute replica.
    pub name: &'static str,
    /// Name of the cluster that this replica belongs to.
    pub cluster_name: &'static str,
}

/// Uniquely identifies the definition of a builtin object.
pub trait Fingerprint {
    fn fingerprint(&self) -> String;
}

impl<T: TypeReference> Fingerprint for &Builtin<T> {
    fn fingerprint(&self) -> String {
        match self {
            Builtin::Log(log) => log.fingerprint(),
            Builtin::Table(table) => table.fingerprint(),
            Builtin::View(view) => view.fingerprint(),
            Builtin::MaterializedView(mv) => mv.fingerprint(),
            Builtin::Type(typ) => typ.fingerprint(),
            Builtin::Func(func) => func.fingerprint(),
            Builtin::Source(coll) => coll.fingerprint(),
            Builtin::Index(index) => index.fingerprint(),
            Builtin::Connection(connection) => connection.fingerprint(),
        }
    }
}

// Types and Funcs never change fingerprints so we just return constant 0
impl<T: TypeReference> Fingerprint for &BuiltinType<T> {
    fn fingerprint(&self) -> String {
        "".to_string()
    }
}

impl Fingerprint for &BuiltinFunc {
    fn fingerprint(&self) -> String {
        "".to_string()
    }
}

impl Fingerprint for &BuiltinLog {
    fn fingerprint(&self) -> String {
        self.variant.desc().fingerprint()
    }
}

impl Fingerprint for &BuiltinTable {
    fn fingerprint(&self) -> String {
        self.desc.fingerprint()
    }
}

impl Fingerprint for &BuiltinView {
    fn fingerprint(&self) -> String {
        self.sql.to_string()
    }
}

impl Fingerprint for &BuiltinSource {
    fn fingerprint(&self) -> String {
        self.desc.fingerprint()
    }
}

impl Fingerprint for &BuiltinMaterializedView {
    fn fingerprint(&self) -> String {
        self.create_sql()
    }
}

impl Fingerprint for &BuiltinIndex {
    fn fingerprint(&self) -> String {
        self.create_sql()
    }
}

impl Fingerprint for &BuiltinConnection {
    fn fingerprint(&self) -> String {
        self.sql.to_string()
    }
}

impl Fingerprint for RelationDesc {
    fn fingerprint(&self) -> String {
        self.typ().fingerprint()
    }
}

impl Fingerprint for SqlRelationType {
    fn fingerprint(&self) -> String {
        serde_json::to_string(self).expect("serialization cannot fail")
    }
}

pub(super) const PUBLIC_SELECT: MzAclItem = MzAclItem {
    grantee: RoleId::Public,
    grantor: MZ_SYSTEM_ROLE_ID,
    acl_mode: AclMode::SELECT,
};

pub(super) const SUPPORT_SELECT: MzAclItem = MzAclItem {
    grantee: MZ_SUPPORT_ROLE_ID,
    grantor: MZ_SYSTEM_ROLE_ID,
    acl_mode: AclMode::SELECT,
};

pub(super) const ANALYTICS_SELECT: MzAclItem = MzAclItem {
    grantee: MZ_ANALYTICS_ROLE_ID,
    grantor: MZ_SYSTEM_ROLE_ID,
    acl_mode: AclMode::SELECT,
};

pub(super) const MONITOR_SELECT: MzAclItem = MzAclItem {
    grantee: MZ_MONITOR_ROLE_ID,
    grantor: MZ_SYSTEM_ROLE_ID,
    acl_mode: AclMode::SELECT,
};

pub(super) const MONITOR_REDACTED_SELECT: MzAclItem = MzAclItem {
    grantee: MZ_MONITOR_REDACTED_ROLE_ID,
    grantor: MZ_SYSTEM_ROLE_ID,
    acl_mode: AclMode::SELECT,
};

pub static MZ_CATALOG_RAW_DESCRIPTION: LazyLock<SystemObjectDescription> =
    LazyLock::new(|| SystemObjectDescription {
        schema_name: MZ_CATALOG_RAW.schema.to_string(),
        object_type: CatalogItemType::Source,
        object_name: MZ_CATALOG_RAW.name.to_string(),
    });

pub static MZ_STORAGE_USAGE_BY_SHARD_DESCRIPTION: LazyLock<SystemObjectDescription> =
    LazyLock::new(|| SystemObjectDescription {
        schema_name: MZ_STORAGE_USAGE_BY_SHARD.schema.to_string(),
        object_type: CatalogItemType::Table,
        object_name: MZ_STORAGE_USAGE_BY_SHARD.name.to_string(),
    });

/// Identifies [`MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY`] for the schema-migration
/// guard in `builtin_schema_migration.rs`, which forbids migrating this table
/// because its startup pruner assumes it is the only source of retractions.
pub static MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY_DESCRIPTION: LazyLock<SystemObjectDescription> =
    LazyLock::new(|| SystemObjectDescription {
        schema_name: MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY.schema.to_string(),
        object_type: CatalogItemType::Table,
        object_name: MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY.name.to_string(),
    });
pub const MZ_SYSTEM_ROLE: BuiltinRole = BuiltinRole {
    id: MZ_SYSTEM_ROLE_ID,
    name: SYSTEM_USER_NAME,
    oid: oid::ROLE_MZ_SYSTEM_OID,
    attributes: RoleAttributesRaw::new().with_all(),
};

pub const MZ_SUPPORT_ROLE: BuiltinRole = BuiltinRole {
    id: MZ_SUPPORT_ROLE_ID,
    name: SUPPORT_USER_NAME,
    oid: oid::ROLE_MZ_SUPPORT_OID,
    attributes: RoleAttributesRaw::new(),
};

pub const MZ_ANALYTICS_ROLE: BuiltinRole = BuiltinRole {
    id: MZ_ANALYTICS_ROLE_ID,
    name: ANALYTICS_USER_NAME,
    oid: oid::ROLE_MZ_ANALYTICS_OID,
    attributes: RoleAttributesRaw::new(),
};

/// This role can `SELECT` from various query history objects,
/// e.g. `mz_prepared_statement_history`.
pub const MZ_MONITOR_ROLE: BuiltinRole = BuiltinRole {
    id: MZ_MONITOR_ROLE_ID,
    name: "mz_monitor",
    oid: oid::ROLE_MZ_MONITOR_OID,
    attributes: RoleAttributesRaw::new(),
};

/// This role is like [`MZ_MONITOR_ROLE`], but can only query
/// the redacted versions of the objects.
pub const MZ_MONITOR_REDACTED: BuiltinRole = BuiltinRole {
    id: MZ_MONITOR_REDACTED_ROLE_ID,
    name: "mz_monitor_redacted",
    oid: oid::ROLE_MZ_MONITOR_REDACTED_OID,
    attributes: RoleAttributesRaw::new(),
};

/// Sentinel role used as the grantor for JWT group-sync-managed
/// role memberships. Never logged into directly.
pub const MZ_JWT_SYNC_ROLE: BuiltinRole = BuiltinRole {
    id: MZ_JWT_SYNC_ROLE_ID,
    name: JWT_SYNC_ROLE_NAME,
    oid: oid::ROLE_MZ_JWT_SYNC_OID,
    attributes: RoleAttributesRaw::new(),
};

pub const MZ_SYSTEM_CLUSTER: BuiltinCluster = BuiltinCluster {
    name: SYSTEM_USER_NAME,
    owner_id: &MZ_SYSTEM_ROLE_ID,
    privileges: &[
        MzAclItem {
            grantee: MZ_SUPPORT_ROLE_ID,
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: AclMode::USAGE,
        },
        rbac::owner_privilege(ObjectType::Cluster, MZ_SYSTEM_ROLE_ID),
    ],
};

pub const MZ_SYSTEM_CLUSTER_REPLICA: BuiltinClusterReplica = BuiltinClusterReplica {
    name: BUILTIN_CLUSTER_REPLICA_NAME,
    cluster_name: MZ_SYSTEM_CLUSTER.name,
};

pub const MZ_CATALOG_SERVER_CLUSTER: BuiltinCluster = BuiltinCluster {
    name: "mz_catalog_server",
    owner_id: &MZ_SYSTEM_ROLE_ID,
    privileges: &[
        MzAclItem {
            grantee: RoleId::Public,
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: AclMode::USAGE,
        },
        MzAclItem {
            grantee: MZ_SUPPORT_ROLE_ID,
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: AclMode::USAGE.union(AclMode::CREATE),
        },
        rbac::owner_privilege(ObjectType::Cluster, MZ_SYSTEM_ROLE_ID),
    ],
};

pub const MZ_CATALOG_SERVER_CLUSTER_REPLICA: BuiltinClusterReplica = BuiltinClusterReplica {
    name: BUILTIN_CLUSTER_REPLICA_NAME,
    cluster_name: MZ_CATALOG_SERVER_CLUSTER.name,
};

pub const MZ_PROBE_CLUSTER: BuiltinCluster = BuiltinCluster {
    name: "mz_probe",
    owner_id: &MZ_SYSTEM_ROLE_ID,
    privileges: &[
        MzAclItem {
            grantee: MZ_SUPPORT_ROLE_ID,
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: AclMode::USAGE,
        },
        MzAclItem {
            grantee: MZ_MONITOR_ROLE_ID,
            grantor: MZ_SYSTEM_ROLE_ID,
            acl_mode: AclMode::USAGE,
        },
        rbac::owner_privilege(ObjectType::Cluster, MZ_SYSTEM_ROLE_ID),
    ],
};
pub const MZ_PROBE_CLUSTER_REPLICA: BuiltinClusterReplica = BuiltinClusterReplica {
    name: BUILTIN_CLUSTER_REPLICA_NAME,
    cluster_name: MZ_PROBE_CLUSTER.name,
};

pub const MZ_SUPPORT_CLUSTER: BuiltinCluster = BuiltinCluster {
    name: "mz_support",
    owner_id: &MZ_SUPPORT_ROLE_ID,
    privileges: &[
        MzAclItem {
            grantee: MZ_SYSTEM_ROLE_ID,
            grantor: MZ_SUPPORT_ROLE_ID,
            acl_mode: rbac::all_object_privileges(SystemObjectType::Object(ObjectType::Cluster)),
        },
        rbac::owner_privilege(ObjectType::Cluster, MZ_SUPPORT_ROLE_ID),
    ],
};

pub const MZ_ANALYTICS_CLUSTER: BuiltinCluster = BuiltinCluster {
    name: "mz_analytics",
    owner_id: &MZ_ANALYTICS_ROLE_ID,
    privileges: &[
        MzAclItem {
            grantee: MZ_SYSTEM_ROLE_ID,
            grantor: MZ_ANALYTICS_ROLE_ID,
            acl_mode: rbac::all_object_privileges(SystemObjectType::Object(ObjectType::Cluster)),
        },
        rbac::owner_privilege(ObjectType::Cluster, MZ_ANALYTICS_ROLE_ID),
    ],
};

/// List of all builtin objects sorted topologically by dependency.
pub static BUILTINS_STATIC: LazyLock<Vec<Builtin<NameReference>>> = LazyLock::new(|| {
    let mut builtin_types = vec![
        Builtin::Type(&TYPE_ANY),
        Builtin::Type(&TYPE_ANYARRAY),
        Builtin::Type(&TYPE_ANYELEMENT),
        Builtin::Type(&TYPE_ANYNONARRAY),
        Builtin::Type(&TYPE_ANYRANGE),
        Builtin::Type(&TYPE_BOOL),
        Builtin::Type(&TYPE_BOOL_ARRAY),
        Builtin::Type(&TYPE_BYTEA),
        Builtin::Type(&TYPE_BYTEA_ARRAY),
        Builtin::Type(&TYPE_BPCHAR),
        Builtin::Type(&TYPE_BPCHAR_ARRAY),
        Builtin::Type(&TYPE_CHAR),
        Builtin::Type(&TYPE_CHAR_ARRAY),
        Builtin::Type(&TYPE_DATE),
        Builtin::Type(&TYPE_DATE_ARRAY),
        Builtin::Type(&TYPE_FLOAT4),
        Builtin::Type(&TYPE_FLOAT4_ARRAY),
        Builtin::Type(&TYPE_FLOAT8),
        Builtin::Type(&TYPE_FLOAT8_ARRAY),
        Builtin::Type(&TYPE_INT4),
        Builtin::Type(&TYPE_INT4_ARRAY),
        Builtin::Type(&TYPE_INT8),
        Builtin::Type(&TYPE_INT8_ARRAY),
        Builtin::Type(&TYPE_INTERVAL),
        Builtin::Type(&TYPE_INTERVAL_ARRAY),
        Builtin::Type(&TYPE_JSONB),
        Builtin::Type(&TYPE_JSONB_ARRAY),
        Builtin::Type(&TYPE_LIST),
        Builtin::Type(&TYPE_MAP),
        Builtin::Type(&TYPE_NAME),
        Builtin::Type(&TYPE_NAME_ARRAY),
        Builtin::Type(&TYPE_NUMERIC),
        Builtin::Type(&TYPE_NUMERIC_ARRAY),
        Builtin::Type(&TYPE_OID),
        Builtin::Type(&TYPE_OID_ARRAY),
        Builtin::Type(&TYPE_RECORD),
        Builtin::Type(&TYPE_RECORD_ARRAY),
        Builtin::Type(&TYPE_REGCLASS),
        Builtin::Type(&TYPE_REGCLASS_ARRAY),
        Builtin::Type(&TYPE_REGPROC),
        Builtin::Type(&TYPE_REGPROC_ARRAY),
        Builtin::Type(&TYPE_REGTYPE),
        Builtin::Type(&TYPE_REGTYPE_ARRAY),
        Builtin::Type(&TYPE_INT2),
        Builtin::Type(&TYPE_INT2_ARRAY),
        Builtin::Type(&TYPE_TEXT),
        Builtin::Type(&TYPE_TEXT_ARRAY),
        Builtin::Type(&TYPE_TIME),
        Builtin::Type(&TYPE_TIME_ARRAY),
        Builtin::Type(&TYPE_TIMESTAMP),
        Builtin::Type(&TYPE_TIMESTAMP_ARRAY),
        Builtin::Type(&TYPE_TIMESTAMPTZ),
        Builtin::Type(&TYPE_TIMESTAMPTZ_ARRAY),
        Builtin::Type(&TYPE_UUID),
        Builtin::Type(&TYPE_UUID_ARRAY),
        Builtin::Type(&TYPE_VARCHAR),
        Builtin::Type(&TYPE_VARCHAR_ARRAY),
        Builtin::Type(&TYPE_INT2_VECTOR),
        Builtin::Type(&TYPE_INT2_VECTOR_ARRAY),
        Builtin::Type(&TYPE_ANYCOMPATIBLE),
        Builtin::Type(&TYPE_ANYCOMPATIBLEARRAY),
        Builtin::Type(&TYPE_ANYCOMPATIBLENONARRAY),
        Builtin::Type(&TYPE_ANYCOMPATIBLELIST),
        Builtin::Type(&TYPE_ANYCOMPATIBLEMAP),
        Builtin::Type(&TYPE_ANYCOMPATIBLERANGE),
        Builtin::Type(&TYPE_UINT2),
        Builtin::Type(&TYPE_UINT2_ARRAY),
        Builtin::Type(&TYPE_UINT4),
        Builtin::Type(&TYPE_UINT4_ARRAY),
        Builtin::Type(&TYPE_UINT8),
        Builtin::Type(&TYPE_UINT8_ARRAY),
        Builtin::Type(&TYPE_MZ_TIMESTAMP),
        Builtin::Type(&TYPE_MZ_TIMESTAMP_ARRAY),
        Builtin::Type(&TYPE_INT4_RANGE),
        Builtin::Type(&TYPE_INT4_RANGE_ARRAY),
        Builtin::Type(&TYPE_INT8_RANGE),
        Builtin::Type(&TYPE_INT8_RANGE_ARRAY),
        Builtin::Type(&TYPE_DATE_RANGE),
        Builtin::Type(&TYPE_DATE_RANGE_ARRAY),
        Builtin::Type(&TYPE_NUM_RANGE),
        Builtin::Type(&TYPE_NUM_RANGE_ARRAY),
        Builtin::Type(&TYPE_TS_RANGE),
        Builtin::Type(&TYPE_TS_RANGE_ARRAY),
        Builtin::Type(&TYPE_TSTZ_RANGE),
        Builtin::Type(&TYPE_TSTZ_RANGE_ARRAY),
        Builtin::Type(&TYPE_MZ_ACL_ITEM),
        Builtin::Type(&TYPE_MZ_ACL_ITEM_ARRAY),
        Builtin::Type(&TYPE_ACL_ITEM),
        Builtin::Type(&TYPE_ACL_ITEM_ARRAY),
        Builtin::Type(&TYPE_INTERNAL),
    ];

    let mut builtin_funcs = Vec::new();
    for (schema, funcs) in &[
        (PG_CATALOG_SCHEMA, &*mz_sql::func::PG_CATALOG_BUILTINS),
        (
            INFORMATION_SCHEMA,
            &*mz_sql::func::INFORMATION_SCHEMA_BUILTINS,
        ),
        (MZ_CATALOG_SCHEMA, &*mz_sql::func::MZ_CATALOG_BUILTINS),
        (MZ_INTERNAL_SCHEMA, &*mz_sql::func::MZ_INTERNAL_BUILTINS),
        (MZ_UNSAFE_SCHEMA, &*mz_sql::func::MZ_UNSAFE_BUILTINS),
    ] {
        for (name, func) in funcs.iter() {
            builtin_funcs.push(Builtin::Func(BuiltinFunc {
                name,
                schema,
                inner: func,
            }));
        }
    }

    let mut builtin_items = vec![
        Builtin::Source(&MZ_CATALOG_RAW),
        Builtin::Log(&MZ_ARRANGEMENT_SHARING_RAW),
        Builtin::Log(&MZ_ARRANGEMENT_BATCHES_RAW),
        Builtin::Log(&MZ_ARRANGEMENT_RECORDS_RAW),
        Builtin::Log(&MZ_ARRANGEMENT_BATCHER_RECORDS_RAW),
        Builtin::Log(&MZ_ARRANGEMENT_BATCHER_SIZE_RAW),
        Builtin::Log(&MZ_ARRANGEMENT_BATCHER_CAPACITY_RAW),
        Builtin::Log(&MZ_ARRANGEMENT_BATCHER_ALLOCATIONS_RAW),
        Builtin::Log(&MZ_DATAFLOW_CHANNELS_PER_WORKER),
        Builtin::Log(&MZ_DATAFLOW_OPERATORS_PER_WORKER),
        Builtin::Log(&MZ_DATAFLOW_ADDRESSES_PER_WORKER),
        Builtin::Log(&MZ_DATAFLOW_OPERATOR_REACHABILITY_RAW),
        Builtin::Log(&MZ_COMPUTE_EXPORTS_PER_WORKER),
        Builtin::Log(&MZ_COMPUTE_DATAFLOW_GLOBAL_IDS_PER_WORKER),
        Builtin::Log(&MZ_CLUSTER_PROMETHEUS_METRICS),
        Builtin::Log(&MZ_MESSAGE_COUNTS_RECEIVED_RAW),
        Builtin::Log(&MZ_MESSAGE_COUNTS_SENT_RAW),
        Builtin::Log(&MZ_MESSAGE_BATCH_COUNTS_RECEIVED_RAW),
        Builtin::Log(&MZ_MESSAGE_BATCH_COUNTS_SENT_RAW),
        Builtin::Log(&MZ_ACTIVE_PEEKS_PER_WORKER),
        Builtin::Log(&MZ_PEEK_DURATIONS_HISTOGRAM_RAW),
        Builtin::Log(&MZ_ARRANGEMENT_HEAP_CAPACITY_RAW),
        Builtin::Log(&MZ_ARRANGEMENT_HEAP_ALLOCATIONS_RAW),
        Builtin::Log(&MZ_ARRANGEMENT_HEAP_SIZE_RAW),
        Builtin::Log(&MZ_SCHEDULING_ELAPSED_RAW),
        Builtin::Log(&MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM_RAW),
        Builtin::Log(&MZ_SCHEDULING_PARKS_HISTOGRAM_RAW),
        Builtin::Log(&MZ_COMPUTE_FRONTIERS_PER_WORKER),
        Builtin::Log(&MZ_COMPUTE_IMPORT_FRONTIERS_PER_WORKER),
        Builtin::Log(&MZ_COMPUTE_ERROR_COUNTS_RAW),
        Builtin::Log(&MZ_COMPUTE_HYDRATION_TIMES_PER_WORKER),
        Builtin::Log(&MZ_COMPUTE_OPERATOR_HYDRATION_STATUSES_PER_WORKER),
        Builtin::Table(&MZ_KAFKA_SINKS),
        Builtin::MaterializedView(&MZ_KAFKA_CONNECTIONS),
        Builtin::MaterializedView(&MZ_KAFKA_SOURCES),
        Builtin::Table(&MZ_OBJECT_DEPENDENCIES),
        Builtin::Table(&MZ_ICEBERG_SINKS),
        Builtin::MaterializedView(&MZ_DATABASES),
        Builtin::MaterializedView(&MZ_SCHEMAS),
        Builtin::Table(&MZ_COLUMNS),
        // mz_indexes is generated dynamically below with inlined builtin VALUES.
        Builtin::Table(&MZ_INDEX_COLUMNS),
        Builtin::Table(&MZ_TABLES),
        // mz_sources is generated dynamically below with inlined builtin VALUES.
        Builtin::Table(&MZ_SOURCE_REFERENCES),
        Builtin::MaterializedView(&MZ_POSTGRES_SOURCES),
        Builtin::MaterializedView(&MZ_POSTGRES_SOURCE_TABLES),
        Builtin::MaterializedView(&MZ_MYSQL_SOURCE_TABLES),
        Builtin::MaterializedView(&MZ_SQL_SERVER_SOURCE_TABLES),
        Builtin::MaterializedView(&MZ_KAFKA_SOURCE_TABLES),
        Builtin::Table(&MZ_SINKS),
        Builtin::Table(&MZ_VIEWS),
        Builtin::Table(&MZ_TYPES),
        Builtin::Table(&MZ_TYPE_PG_METADATA),
        Builtin::Table(&MZ_ARRAY_TYPES),
        Builtin::Table(&MZ_BASE_TYPES),
        Builtin::Table(&MZ_LIST_TYPES),
        Builtin::Table(&MZ_MAP_TYPES),
        Builtin::MaterializedView(&MZ_ROLES),
        Builtin::Table(&MZ_ROLE_AUTH),
        Builtin::MaterializedView(&MZ_ROLE_MEMBERS),
        Builtin::MaterializedView(&MZ_ROLE_PARAMETERS),
        Builtin::Table(&MZ_PSEUDO_TYPES),
        Builtin::Table(&MZ_FUNCTIONS),
        Builtin::Table(&MZ_OPERATORS),
        Builtin::Table(&MZ_AGGREGATES),
        Builtin::Table(&MZ_CLUSTER_REPLICA_SIZES),
        Builtin::Table(&MZ_CLUSTER_REPLICA_SIZE_INTERNAL),
        Builtin::MaterializedView(&MZ_CLUSTERS),
        Builtin::MaterializedView(&MZ_CLUSTER_WORKLOAD_CLASSES),
        Builtin::MaterializedView(&MZ_CLUSTER_SCHEDULES),
        Builtin::MaterializedView(&MZ_CLUSTER_RECONFIGURATIONS),
        Builtin::MaterializedView(&MZ_CLUSTER_AUTO_SCALING_STRATEGIES),
        Builtin::MaterializedView(&MZ_SECRETS),
        Builtin::MaterializedView(&MZ_CONNECTIONS),
        Builtin::MaterializedView(&MZ_SSH_TUNNEL_CONNECTIONS),
        Builtin::MaterializedView(&MZ_CLUSTER_REPLICAS),
        Builtin::Source(&MZ_CLUSTER_REPLICA_METRICS_HISTORY),
        Builtin::View(&MZ_CLUSTER_REPLICA_METRICS),
        Builtin::Source(&MZ_CLUSTER_REPLICA_STATUS_HISTORY),
        Builtin::View(&MZ_CLUSTER_REPLICA_STATUSES),
        Builtin::MaterializedView(&MZ_INTERNAL_CLUSTER_REPLICAS),
        Builtin::MaterializedView(&MZ_PENDING_CLUSTER_REPLICAS),
        Builtin::MaterializedView(&MZ_AUDIT_EVENTS),
        Builtin::Table(&MZ_STORAGE_USAGE_BY_SHARD),
        Builtin::Table(&MZ_EGRESS_IPS),
        Builtin::Table(&MZ_AWS_PRIVATELINK_CONNECTIONS),
        Builtin::Table(&MZ_AWS_CONNECTIONS),
        Builtin::Table(&MZ_SUBSCRIPTIONS),
        Builtin::Table(&MZ_SESSIONS),
        Builtin::MaterializedView(&MZ_OVERRIDDEN_SYSTEM_PARAMETERS),
        Builtin::MaterializedView(&MZ_CLUSTER_SYSTEM_PARAMETERS),
        Builtin::MaterializedView(&MZ_REPLICA_SYSTEM_PARAMETERS),
        Builtin::MaterializedView(&MZ_DEFAULT_PRIVILEGES),
        Builtin::MaterializedView(&MZ_SYSTEM_PRIVILEGES),
        Builtin::MaterializedView(&MZ_COMMENTS),
        Builtin::Table(&MZ_WEBHOOKS_SOURCES),
        Builtin::Table(&MZ_HISTORY_RETENTION_STRATEGIES),
        Builtin::MaterializedView(&MZ_MATERIALIZED_VIEWS),
        Builtin::Table(&MZ_MATERIALIZED_VIEW_REFRESH_STRATEGIES),
        Builtin::MaterializedView(&MZ_NETWORK_POLICIES),
        Builtin::MaterializedView(&MZ_NETWORK_POLICY_RULES),
        Builtin::Table(&MZ_LICENSE_KEYS),
        Builtin::Table(&MZ_REPLACEMENTS),
        Builtin::View(&MZ_RELATIONS),
        Builtin::View(&MZ_OBJECT_OID_ALIAS),
        Builtin::View(&MZ_OBJECTS),
        Builtin::View(&MZ_OBJECT_FULLY_QUALIFIED_NAMES),
        Builtin::View(&MZ_OBJECTS_ID_NAMESPACE_TYPES),
        Builtin::View(&MZ_OBJECT_HISTORY),
        Builtin::View(&MZ_OBJECT_LIFETIMES),
        Builtin::Table(&MZ_OBJECT_GLOBAL_IDS),
        Builtin::View(&MZ_ARRANGEMENT_SHARING_PER_WORKER),
        Builtin::View(&MZ_ARRANGEMENT_SHARING),
        Builtin::View(&MZ_ARRANGEMENT_SIZES_PER_WORKER),
        Builtin::View(&MZ_ARRANGEMENT_SIZES),
        Builtin::View(&MZ_DATAFLOWS_PER_WORKER),
        Builtin::View(&MZ_DATAFLOWS),
        Builtin::View(&MZ_DATAFLOW_ADDRESSES),
        Builtin::View(&MZ_DATAFLOW_CHANNELS),
        Builtin::View(&MZ_DATAFLOW_OPERATORS),
        Builtin::View(&MZ_DATAFLOW_GLOBAL_IDS),
        Builtin::View(&MZ_COMPUTE_EXPORTS),
        Builtin::View(&MZ_MAPPABLE_OBJECTS),
        Builtin::View(&MZ_DATAFLOW_OPERATOR_DATAFLOWS_PER_WORKER),
        Builtin::View(&MZ_DATAFLOW_OPERATOR_DATAFLOWS),
        Builtin::View(&MZ_OBJECT_TRANSITIVE_DEPENDENCIES),
        Builtin::View(&MZ_DATAFLOW_OPERATOR_REACHABILITY_PER_WORKER),
        Builtin::View(&MZ_DATAFLOW_OPERATOR_REACHABILITY),
        Builtin::View(&MZ_CLUSTER_REPLICA_UTILIZATION),
        Builtin::View(&MZ_CLUSTER_REPLICA_UTILIZATION_HISTORY),
        Builtin::View(&MZ_DATAFLOW_OPERATOR_PARENTS_PER_WORKER),
        Builtin::View(&MZ_DATAFLOW_OPERATOR_PARENTS),
        Builtin::View(&MZ_DATAFLOW_ARRANGEMENT_SIZES),
        Builtin::View(&MZ_EXPECTED_GROUP_SIZE_ADVICE),
        Builtin::View(&MZ_COMPUTE_FRONTIERS),
        Builtin::View(&MZ_DATAFLOW_CHANNEL_OPERATORS_PER_WORKER),
        Builtin::View(&MZ_DATAFLOW_CHANNEL_OPERATORS),
        Builtin::View(&MZ_COMPUTE_IMPORT_FRONTIERS),
        Builtin::View(&MZ_MESSAGE_COUNTS_PER_WORKER),
        Builtin::View(&MZ_MESSAGE_COUNTS),
        Builtin::View(&MZ_ACTIVE_PEEKS),
        Builtin::View(&MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM_PER_WORKER),
        Builtin::View(&MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM),
        Builtin::View(&MZ_RECORDS_PER_DATAFLOW_OPERATOR_PER_WORKER),
        Builtin::View(&MZ_RECORDS_PER_DATAFLOW_OPERATOR),
        Builtin::View(&MZ_RECORDS_PER_DATAFLOW_PER_WORKER),
        Builtin::View(&MZ_RECORDS_PER_DATAFLOW),
        Builtin::View(&MZ_PEEK_DURATIONS_HISTOGRAM_PER_WORKER),
        Builtin::View(&MZ_PEEK_DURATIONS_HISTOGRAM),
        Builtin::View(&MZ_SCHEDULING_ELAPSED_PER_WORKER),
        Builtin::View(&MZ_SCHEDULING_ELAPSED),
        Builtin::View(&MZ_SCHEDULING_PARKS_HISTOGRAM_PER_WORKER),
        Builtin::View(&MZ_SCHEDULING_PARKS_HISTOGRAM),
        Builtin::View(&MZ_SHOW_ALL_OBJECTS),
        Builtin::View(&MZ_SHOW_COLUMNS),
        Builtin::View(&MZ_SHOW_CLUSTERS),
        Builtin::View(&MZ_SHOW_SECRETS),
        Builtin::View(&MZ_SHOW_DATABASES),
        Builtin::View(&MZ_SHOW_SCHEMAS),
        Builtin::View(&MZ_SHOW_TABLES),
        Builtin::View(&MZ_SHOW_VIEWS),
        Builtin::View(&MZ_SHOW_TYPES),
        Builtin::View(&MZ_SHOW_ROLES),
        Builtin::View(&MZ_SHOW_CONNECTIONS),
        Builtin::View(&MZ_SHOW_SOURCES),
        Builtin::View(&MZ_SHOW_SINKS),
        Builtin::View(&MZ_SHOW_MATERIALIZED_VIEWS),
        Builtin::View(&MZ_SHOW_INDEXES),
        Builtin::View(&MZ_CLUSTER_REPLICA_HISTORY),
        Builtin::View(&MZ_CLUSTER_REPLICA_NAME_HISTORY),
        Builtin::View(&MZ_TIMEZONE_NAMES),
        Builtin::View(&MZ_TIMEZONE_ABBREVIATIONS),
        Builtin::View(&PG_NAMESPACE_ALL_DATABASES),
        Builtin::Index(&PG_NAMESPACE_ALL_DATABASES_IND),
        Builtin::View(&PG_NAMESPACE),
        Builtin::View(&PG_CLASS_ALL_DATABASES),
        Builtin::Index(&PG_CLASS_ALL_DATABASES_IND),
        Builtin::View(&PG_CLASS),
        Builtin::View(&PG_DEPEND),
        Builtin::View(&PG_DATABASE),
        Builtin::View(&PG_INDEX),
        Builtin::View(&PG_TYPE_ALL_DATABASES),
        Builtin::Index(&PG_TYPE_ALL_DATABASES_IND),
        Builtin::View(&PG_TYPE),
        Builtin::View(&PG_DESCRIPTION_ALL_DATABASES),
        Builtin::Index(&PG_DESCRIPTION_ALL_DATABASES_IND),
        Builtin::View(&PG_DESCRIPTION),
        Builtin::View(&PG_ATTRIBUTE_ALL_DATABASES),
        Builtin::Index(&PG_ATTRIBUTE_ALL_DATABASES_IND),
        Builtin::View(&PG_ATTRIBUTE),
        Builtin::View(&PG_PROC),
        Builtin::View(&PG_OPERATOR),
        Builtin::View(&PG_RANGE),
        Builtin::View(&PG_ENUM),
        Builtin::View(&PG_ATTRDEF_ALL_DATABASES),
        Builtin::Index(&PG_ATTRDEF_ALL_DATABASES_IND),
        Builtin::View(&PG_ATTRDEF),
        Builtin::View(&PG_SETTINGS),
        Builtin::View(&PG_AUTH_MEMBERS),
        Builtin::View(&PG_CONSTRAINT),
        Builtin::View(&PG_TABLES),
        Builtin::View(&PG_TABLESPACE),
        Builtin::View(&PG_ACCESS_METHODS),
        Builtin::View(&PG_LOCKS),
        Builtin::View(&PG_AUTHID_CORE),
        Builtin::Index(&PG_AUTHID_CORE_IND),
        Builtin::View(&PG_AUTHID),
        Builtin::View(&PG_ROLES),
        Builtin::View(&PG_USER),
        Builtin::View(&PG_VIEWS),
        Builtin::View(&PG_MATVIEWS),
        Builtin::View(&PG_COLLATION),
        Builtin::View(&PG_POLICY),
        Builtin::View(&PG_INHERITS),
        Builtin::View(&PG_AGGREGATE),
        Builtin::View(&PG_TRIGGER),
        Builtin::View(&PG_REWRITE),
        Builtin::View(&PG_EXTENSION),
        Builtin::View(&PG_EVENT_TRIGGER),
        Builtin::View(&PG_LANGUAGE),
        Builtin::View(&PG_SHDESCRIPTION),
        Builtin::View(&PG_INDEXES),
        Builtin::View(&PG_TIMEZONE_ABBREVS),
        Builtin::View(&PG_TIMEZONE_NAMES),
        Builtin::View(&INFORMATION_SCHEMA_APPLICABLE_ROLES),
        Builtin::View(&INFORMATION_SCHEMA_COLUMNS),
        Builtin::View(&INFORMATION_SCHEMA_ENABLED_ROLES),
        Builtin::View(&INFORMATION_SCHEMA_KEY_COLUMN_USAGE),
        Builtin::View(&INFORMATION_SCHEMA_REFERENTIAL_CONSTRAINTS),
        Builtin::View(&INFORMATION_SCHEMA_ROUTINES),
        Builtin::View(&INFORMATION_SCHEMA_SCHEMATA),
        Builtin::View(&INFORMATION_SCHEMA_TABLES),
        Builtin::View(&INFORMATION_SCHEMA_TABLE_CONSTRAINTS),
        Builtin::View(&INFORMATION_SCHEMA_TABLE_PRIVILEGES),
        Builtin::View(&INFORMATION_SCHEMA_ROLE_TABLE_GRANTS),
        Builtin::View(&INFORMATION_SCHEMA_TRIGGERS),
        Builtin::View(&INFORMATION_SCHEMA_VIEWS),
        Builtin::View(&INFORMATION_SCHEMA_CHARACTER_SETS),
        Builtin::View(&MZ_SHOW_ROLE_MEMBERS),
        Builtin::View(&MZ_SHOW_MY_ROLE_MEMBERS),
        Builtin::View(&MZ_SHOW_SYSTEM_PRIVILEGES),
        Builtin::View(&MZ_SHOW_MY_SYSTEM_PRIVILEGES),
        Builtin::View(&MZ_SHOW_CLUSTER_PRIVILEGES),
        Builtin::View(&MZ_SHOW_MY_CLUSTER_PRIVILEGES),
        Builtin::View(&MZ_SHOW_DATABASE_PRIVILEGES),
        Builtin::View(&MZ_SHOW_MY_DATABASE_PRIVILEGES),
        Builtin::View(&MZ_SHOW_SCHEMA_PRIVILEGES),
        Builtin::View(&MZ_SHOW_MY_SCHEMA_PRIVILEGES),
        Builtin::View(&MZ_SHOW_OBJECT_PRIVILEGES),
        Builtin::View(&MZ_SHOW_MY_OBJECT_PRIVILEGES),
        Builtin::View(&MZ_SHOW_ALL_PRIVILEGES),
        Builtin::View(&MZ_SHOW_ALL_MY_PRIVILEGES),
        Builtin::View(&MZ_SHOW_DEFAULT_PRIVILEGES),
        Builtin::View(&MZ_SHOW_MY_DEFAULT_PRIVILEGES),
        Builtin::Source(&MZ_SINK_STATUS_HISTORY),
        Builtin::View(&MZ_SINK_STATUSES),
        Builtin::Source(&MZ_SOURCE_STATUS_HISTORY),
        Builtin::Source(&MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY),
        Builtin::View(&MZ_AWS_PRIVATELINK_CONNECTION_STATUSES),
        Builtin::Source(&MZ_STATEMENT_EXECUTION_HISTORY),
        Builtin::View(&MZ_STATEMENT_EXECUTION_HISTORY_REDACTED),
        Builtin::Source(&MZ_PREPARED_STATEMENT_HISTORY),
        Builtin::Source(&MZ_SESSION_HISTORY),
        Builtin::Source(&MZ_SQL_TEXT),
        Builtin::View(&MZ_SQL_TEXT_REDACTED),
        Builtin::View(&MZ_RECENT_SQL_TEXT),
        Builtin::View(&MZ_RECENT_SQL_TEXT_REDACTED),
        Builtin::Index(&MZ_RECENT_SQL_TEXT_IND),
        Builtin::View(&MZ_ACTIVITY_LOG_THINNED),
        Builtin::View(&MZ_RECENT_ACTIVITY_LOG_THINNED),
        Builtin::View(&MZ_RECENT_ACTIVITY_LOG),
        Builtin::View(&MZ_RECENT_ACTIVITY_LOG_REDACTED),
        Builtin::Index(&MZ_RECENT_ACTIVITY_LOG_THINNED_IND),
        Builtin::View(&MZ_SOURCE_STATUSES),
        Builtin::Source(&MZ_STATEMENT_LIFECYCLE_HISTORY),
        Builtin::Source(&MZ_STORAGE_SHARDS),
        Builtin::Source(&MZ_SOURCE_STATISTICS_RAW),
        Builtin::Source(&MZ_SINK_STATISTICS_RAW),
        Builtin::View(&MZ_SOURCE_STATISTICS_WITH_HISTORY),
        Builtin::Index(&MZ_SOURCE_STATISTICS_WITH_HISTORY_IND),
        Builtin::View(&MZ_SOURCE_STATISTICS),
        Builtin::Index(&MZ_SOURCE_STATISTICS_IND),
        Builtin::View(&MZ_SINK_STATISTICS),
        Builtin::Index(&MZ_SINK_STATISTICS_IND),
        Builtin::View(&MZ_STORAGE_USAGE),
        Builtin::Source(&MZ_FRONTIERS),
        Builtin::View(&MZ_GLOBAL_FRONTIERS),
        Builtin::Source(&MZ_WALLCLOCK_LAG_HISTORY),
        Builtin::View(&MZ_WALLCLOCK_GLOBAL_LAG_HISTORY),
        Builtin::View(&MZ_WALLCLOCK_GLOBAL_LAG_RECENT_HISTORY),
        Builtin::View(&MZ_WALLCLOCK_GLOBAL_LAG),
        Builtin::Source(&MZ_WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RAW),
        Builtin::View(&MZ_WALLCLOCK_GLOBAL_LAG_HISTOGRAM),
        Builtin::Source(&MZ_MATERIALIZED_VIEW_REFRESHES),
        Builtin::Source(&MZ_COMPUTE_DEPENDENCIES),
        Builtin::View(&MZ_MATERIALIZATION_DEPENDENCIES),
        Builtin::View(&MZ_MATERIALIZATION_LAG),
        Builtin::View(&MZ_CONSOLE_CLUSTER_UTILIZATION_OVERVIEW),
        Builtin::View(&MZ_CONSOLE_CLUSTER_UTILIZATION_OVERVIEW_3H),
        Builtin::View(&MZ_CONSOLE_CLUSTER_UTILIZATION_OVERVIEW_24H),
        Builtin::View(&MZ_COMPUTE_ERROR_COUNTS_PER_WORKER),
        Builtin::View(&MZ_COMPUTE_ERROR_COUNTS),
        Builtin::Source(&MZ_COMPUTE_ERROR_COUNTS_RAW_UNIFIED),
        Builtin::Source(&MZ_COMPUTE_HYDRATION_TIMES),
        Builtin::Source(&MZ_OBJECT_ARRANGEMENT_SIZES_UNIFIED),
        Builtin::Index(&MZ_OBJECT_ARRANGEMENT_SIZES_IND),
        Builtin::Table(&MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY),
        Builtin::Index(&MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY_OBJECT_IND),
        Builtin::Index(&MZ_OBJECT_ARRANGEMENT_SIZE_HISTORY_TS_IND),
        Builtin::Log(&MZ_COMPUTE_LIR_MAPPING_PER_WORKER),
        Builtin::View(&MZ_LIR_MAPPING),
        Builtin::Source(&MZ_COMPUTE_OPERATOR_HYDRATION_STATUSES),
        Builtin::Source(&MZ_CLUSTER_REPLICA_FRONTIERS),
        Builtin::View(&MZ_COMPUTE_HYDRATION_STATUSES),
        Builtin::View(&MZ_HYDRATION_STATUSES),
        Builtin::Index(&MZ_HYDRATION_STATUSES_IND),
        Builtin::View(&MZ_SHOW_CLUSTER_REPLICAS),
        Builtin::View(&MZ_SHOW_NETWORK_POLICIES),
        Builtin::View(&MZ_CLUSTER_DEPLOYMENT_LINEAGE),
        Builtin::Index(&MZ_SHOW_DATABASES_IND),
        Builtin::Index(&MZ_SHOW_SCHEMAS_IND),
        Builtin::Index(&MZ_SHOW_CONNECTIONS_IND),
        Builtin::Index(&MZ_SHOW_TABLES_IND),
        Builtin::Index(&MZ_SHOW_SOURCES_IND),
        Builtin::Index(&MZ_SHOW_VIEWS_IND),
        Builtin::Index(&MZ_SHOW_MATERIALIZED_VIEWS_IND),
        Builtin::Index(&MZ_SHOW_SINKS_IND),
        Builtin::Index(&MZ_SHOW_TYPES_IND),
        Builtin::Index(&MZ_SHOW_ALL_OBJECTS_IND),
        Builtin::Index(&MZ_SHOW_INDEXES_IND),
        Builtin::Index(&MZ_SHOW_COLUMNS_IND),
        Builtin::Index(&MZ_SHOW_CLUSTERS_IND),
        Builtin::Index(&MZ_SHOW_CLUSTER_REPLICAS_IND),
        Builtin::Index(&MZ_SHOW_SECRETS_IND),
        Builtin::Index(&MZ_SHOW_ROLES_IND),
        Builtin::Index(&MZ_CLUSTERS_IND),
        Builtin::Index(&MZ_CLUSTER_RECONFIGURATIONS_IND),
        Builtin::Index(&MZ_CLUSTER_AUTO_SCALING_STRATEGIES_IND),
        Builtin::Index(&MZ_INDEXES_IND),
        Builtin::Index(&MZ_ROLES_IND),
        Builtin::Index(&MZ_SOURCES_IND),
        Builtin::Index(&MZ_SINKS_IND),
        Builtin::Index(&MZ_MATERIALIZED_VIEWS_IND),
        Builtin::Index(&MZ_SOURCE_STATUSES_IND),
        Builtin::Index(&MZ_SOURCE_STATUS_HISTORY_IND),
        Builtin::Index(&MZ_SINK_STATUSES_IND),
        Builtin::Index(&MZ_SINK_STATUS_HISTORY_IND),
        Builtin::Index(&MZ_CLUSTER_REPLICAS_IND),
        Builtin::Index(&MZ_CLUSTER_REPLICA_SIZES_IND),
        Builtin::Index(&MZ_CLUSTER_REPLICA_SIZE_INTERNAL_IND),
        Builtin::Index(&MZ_CLUSTER_REPLICA_STATUSES_IND),
        Builtin::Index(&MZ_CLUSTER_REPLICA_STATUS_HISTORY_IND),
        Builtin::Index(&MZ_CLUSTER_REPLICA_METRICS_IND),
        Builtin::Index(&MZ_CLUSTER_REPLICA_METRICS_HISTORY_IND),
        Builtin::Index(&MZ_CLUSTER_REPLICA_HISTORY_IND),
        Builtin::Index(&MZ_CLUSTER_REPLICA_NAME_HISTORY_IND),
        Builtin::Index(&MZ_OBJECT_LIFETIMES_IND),
        Builtin::Index(&MZ_OBJECT_HISTORY_IND),
        Builtin::Index(&MZ_OBJECT_DEPENDENCIES_IND),
        Builtin::Index(&MZ_COMPUTE_DEPENDENCIES_IND),
        Builtin::Index(&MZ_OBJECT_TRANSITIVE_DEPENDENCIES_IND),
        Builtin::Index(&MZ_FRONTIERS_IND),
        Builtin::Index(&MZ_WALLCLOCK_GLOBAL_LAG_RECENT_HISTORY_IND),
        Builtin::Index(&MZ_KAFKA_SOURCES_IND),
        Builtin::Index(&MZ_WEBHOOK_SOURCES_IND),
        Builtin::Index(&MZ_COMMENTS_IND),
        Builtin::Index(&MZ_DATABASES_IND),
        Builtin::Index(&MZ_SCHEMAS_IND),
        Builtin::Index(&MZ_CONNECTIONS_IND),
        Builtin::Index(&MZ_TABLES_IND),
        Builtin::Index(&MZ_TYPES_IND),
        Builtin::Index(&MZ_OBJECTS_IND),
        Builtin::Index(&MZ_COLUMNS_IND),
        Builtin::Index(&MZ_SECRETS_IND),
        Builtin::Index(&MZ_VIEWS_IND),
        Builtin::Index(&MZ_CONSOLE_CLUSTER_UTILIZATION_OVERVIEW_IND),
        Builtin::Index(&MZ_CONSOLE_CLUSTER_UTILIZATION_OVERVIEW_3H_IND),
        Builtin::Index(&MZ_CONSOLE_CLUSTER_UTILIZATION_OVERVIEW_24H_IND),
        Builtin::Index(&MZ_CLUSTER_DEPLOYMENT_LINEAGE_IND),
        Builtin::Index(&MZ_CLUSTER_REPLICA_FRONTIERS_IND),
        Builtin::Index(&MZ_COMPUTE_HYDRATION_TIMES_IND),
        Builtin::View(&MZ_RECENT_STORAGE_USAGE),
        Builtin::Index(&MZ_RECENT_STORAGE_USAGE_IND),
        Builtin::Connection(&MZ_ANALYTICS),
        Builtin::View(&MZ_INDEX_ADVICE),
        Builtin::View(&MZ_MCP_DATA_PRODUCTS),
        Builtin::View(&MZ_MCP_DATA_PRODUCT_DETAILS),
    ];

    builtin_items.extend(notice::builtins());

    // Generate mz_sources with builtin source/log entries inlined as VALUES so
    // that its SQL fingerprint changes whenever a builtin source is added or
    // removed, forcing an explicit MigrationStep::replacement.
    //
    // Must happen BEFORE ontology::generate_views so that mz_sources's ontology
    // annotation (entity_name = "source") is visible to the ontology index views.
    // All sources/logs are already present in builtin_items at this point.
    {
        let source_iter = builtin_items.iter().filter_map(|b| match b {
            Builtin::Source(x) => Some(*x),
            _ => None,
        });
        let log_iter = builtin_items.iter().filter_map(|b| match b {
            Builtin::Log(x) => Some(*x),
            _ => None,
        });
        let mz_sources = builtin::make_mz_sources(source_iter, log_iter);
        let mz_sources_ref: &'static BuiltinMaterializedView = Box::leak(Box::new(mz_sources));
        // Insert at the original position of the old static MZ_SOURCES —
        // right before mz_source_references — to preserve stable IDs for
        // all items that follow it in the list.
        let insert_pos = builtin_items
            .iter()
            .position(|b| matches!(b, Builtin::Table(t) if t.name == "mz_source_references"))
            .expect("mz_source_references must be present in builtin_items");
        builtin_items.insert(insert_pos, Builtin::MaterializedView(mz_sources_ref));
    }

    // Generate mz_indexes with builtin index/log entries inlined as VALUES so
    // that its SQL fingerprint changes whenever a builtin index or log is added or
    // removed, forcing an explicit MigrationStep::replacement.
    //
    // Must happen AFTER all builtin indexes and logs have been pushed into
    // builtin_items, so that make_mz_indexes sees the complete set. Must happen
    // BEFORE ontology::generate_views so the ontology generator sees mz_indexes
    // as a materialized view participating in catalog ontology, rather than
    // being absent from builtin_items.
    {
        let index_iter = builtin_items.iter().filter_map(|b| match b {
            Builtin::Index(x) => Some(*x),
            _ => None,
        });
        let log_iter = builtin_items.iter().filter_map(|b| match b {
            Builtin::Log(x) => Some(*x),
            _ => None,
        });
        let mz_indexes = mz_catalog::make_mz_indexes(index_iter, log_iter);
        let mz_indexes_ref: &'static BuiltinMaterializedView = Box::leak(Box::new(mz_indexes));
        let insert_pos = builtin_items
            .iter()
            .position(|b| matches!(b, Builtin::Table(t) if t.name == "mz_index_columns"))
            .expect("mz_index_columns must be present in builtin_items");
        builtin_items.insert(insert_pos, Builtin::MaterializedView(mz_indexes_ref));
    }

    // Generate ontology views by enumerating existing builtins.
    builtin_items.extend(ontology::generate_views(&builtin_items));

    // Generate builtin relations reporting builtin objects last, since they need a complete view
    // of all other builtins.
    let mut builtin_builtins = builtin::builtins(&builtin_items).collect();

    // Construct the full list of builtins, retaining dependency order.
    let mut builtins = Vec::new();
    builtins.append(&mut builtin_types);
    builtins.append(&mut builtin_funcs);
    builtins.append(&mut builtin_builtins);
    builtins.append(&mut builtin_items);

    builtins
});
pub const BUILTIN_ROLES: &[&BuiltinRole] = &[
    &MZ_SYSTEM_ROLE,
    &MZ_SUPPORT_ROLE,
    &MZ_ANALYTICS_ROLE,
    &MZ_MONITOR_ROLE,
    &MZ_MONITOR_REDACTED,
    &MZ_JWT_SYNC_ROLE,
];
pub const BUILTIN_CLUSTERS: &[&BuiltinCluster] = &[
    &MZ_SYSTEM_CLUSTER,
    &MZ_CATALOG_SERVER_CLUSTER,
    &MZ_PROBE_CLUSTER,
    &MZ_SUPPORT_CLUSTER,
    &MZ_ANALYTICS_CLUSTER,
];
pub const BUILTIN_CLUSTER_REPLICAS: &[&BuiltinClusterReplica] = &[
    &MZ_SYSTEM_CLUSTER_REPLICA,
    &MZ_CATALOG_SERVER_CLUSTER_REPLICA,
    &MZ_PROBE_CLUSTER_REPLICA,
];

#[allow(non_snake_case)]
pub mod BUILTINS {
    use super::*;

    pub fn logs() -> impl Iterator<Item = &'static BuiltinLog> {
        BUILTINS_STATIC.iter().filter_map(|b| match b {
            Builtin::Log(log) => Some(*log),
            _ => None,
        })
    }

    pub fn types() -> impl Iterator<Item = &'static BuiltinType<NameReference>> {
        BUILTINS_STATIC.iter().filter_map(|b| match b {
            Builtin::Type(typ) => Some(*typ),
            _ => None,
        })
    }

    pub fn views() -> impl Iterator<Item = &'static BuiltinView> {
        BUILTINS_STATIC.iter().filter_map(|b| match b {
            Builtin::View(view) => Some(*view),
            _ => None,
        })
    }

    pub fn materialized_views() -> impl Iterator<Item = &'static BuiltinMaterializedView> {
        BUILTINS_STATIC.iter().filter_map(|b| match b {
            Builtin::MaterializedView(mv) => Some(*mv),
            _ => None,
        })
    }

    pub fn funcs() -> impl Iterator<Item = &'static BuiltinFunc> {
        BUILTINS_STATIC.iter().filter_map(|b| match b {
            Builtin::Func(func) => Some(func),
            _ => None,
        })
    }

    pub fn iter() -> impl Iterator<Item = &'static Builtin<NameReference>> {
        BUILTINS_STATIC.iter()
    }
}

pub static BUILTIN_LOG_LOOKUP: LazyLock<HashMap<&'static str, &'static BuiltinLog>> =
    LazyLock::new(|| BUILTINS::logs().map(|log| (log.name, log)).collect());
/// Keys are builtin object description, values are the builtin index when sorted by dependency and
/// the builtin itself.
pub static BUILTIN_LOOKUP: LazyLock<
    HashMap<SystemObjectDescription, (usize, &'static Builtin<NameReference>)>,
> = LazyLock::new(|| {
    BUILTINS_STATIC
        .iter()
        .enumerate()
        .map(|(idx, builtin)| {
            (
                SystemObjectDescription {
                    schema_name: builtin.schema().to_string(),
                    object_type: builtin.catalog_item_type(),
                    object_name: builtin.name().to_string(),
                },
                (idx, builtin),
            )
        })
        .collect()
});

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use mz_pgrepr::oid::FIRST_MATERIALIZE_OID;
    use mz_sql_parser::ast::visit::{self, Visit};
    use mz_sql_parser::ast::{Raw, RawItemName, UnresolvedItemName};

    use super::*;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn test_builtin_type_schema() {
        for typ in BUILTINS::types() {
            if typ.oid < FIRST_MATERIALIZE_OID {
                assert_eq!(
                    typ.schema, PG_CATALOG_SCHEMA,
                    "{typ:?} should be in {PG_CATALOG_SCHEMA} schema"
                );
            } else {
                // `mz_pgrepr::Type` resolution relies on all non-PG types existing in the
                // mz_catalog schema.
                assert_eq!(
                    typ.schema, MZ_CATALOG_SCHEMA,
                    "{typ:?} should be in {MZ_CATALOG_SCHEMA} schema"
                );
            }
        }
    }

    /// Visitor that collects the last component of all referenced
    /// item names from a SQL AST.
    struct ItemNameCollector {
        names: BTreeSet<String>,
    }

    impl<'ast> Visit<'ast, Raw> for ItemNameCollector {
        fn visit_item_name(&mut self, name: &'ast <Raw as mz_sql_parser::ast::AstInfo>::ItemName) {
            let unresolved: &UnresolvedItemName = match name {
                RawItemName::Name(n) | RawItemName::Id(_, n, _) => n,
            };
            let parts = &unresolved.0;
            if !parts.is_empty() {
                let obj_name = parts[parts.len() - 1].as_str().to_string();
                self.names.insert(obj_name);
            }
            visit::visit_item_name(self, name);
        }
    }

    /// Tests that `BUILTINS_STATIC` is ordered respecting dependencies:
    /// if builtin A references builtin B in its SQL, then B must appear
    /// before A in the list. (This ordering is assumed by, e.g.,
    /// `sort_updates` during catalog migrations.)
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn test_builtins_static_dependency_order() {
        // Build a map from name -> (schema, index) for all builtins.
        // We look up by just the name (last component) to catch
        // unqualified references in SQL.
        let mut builtin_by_name: BTreeMap<&str, (&str, usize)> = BTreeMap::new();
        let mut duplicate_names = Vec::new();
        for (idx, builtin) in BUILTINS_STATIC.iter().enumerate() {
            if let Some((prev_schema, prev_idx)) =
                builtin_by_name.insert(builtin.name(), (builtin.schema(), idx))
            {
                // Only flag duplicates across different schemas.
                // Same-schema duplicates (e.g., range types that
                // appear as both Type and Func) are fine because
                // they resolve to the same schema.
                if prev_schema != builtin.schema() {
                    duplicate_names.push(format!(
                        "name {:?} appears in both {}.{} (index \
                         {}) and {}.{} (index {})",
                        builtin.name(),
                        prev_schema,
                        builtin.name(),
                        prev_idx,
                        builtin.schema(),
                        builtin.name(),
                        idx,
                    ));
                }
            }
        }
        assert!(
            duplicate_names.is_empty(),
            "BUILTINS_STATIC has duplicate names across different \
             schemas (this test needs adjustment if such duplicates \
             are intentional):\n{}",
            duplicate_names.join("\n"),
        );

        // Get the `CREATE ...` SQL for builtins that have it.
        let get_create_sql = |builtin: &Builtin<NameReference>| -> Option<String> {
            match builtin {
                Builtin::View(v) => Some(v.create_sql()),
                Builtin::MaterializedView(mv) => Some(mv.create_sql()),
                Builtin::Index(idx) => Some(idx.create_sql()),
                _ => None,
            }
        };

        // For each SQL-bearing builtin, parse its SQL, walk the AST to
        // find referenced item names, and check that all referenced
        // builtins appear earlier in BUILTINS_STATIC.
        let mut violations = Vec::new();
        for (idx, builtin) in BUILTINS_STATIC.iter().enumerate() {
            let create_sql = match get_create_sql(builtin) {
                Some(sql) => sql,
                None => continue,
            };

            let stmts = mz_sql_parser::parser::parse_statements(&create_sql).unwrap_or_else(|e| {
                panic!(
                    "failed to parse SQL for {}.{}: \
                         {e}\nSQL: {create_sql}",
                    builtin.schema(),
                    builtin.name(),
                )
            });

            let mut collector = ItemNameCollector {
                names: BTreeSet::new(),
            };
            for stmt in &stmts {
                collector.visit_statement(&stmt.ast);
            }

            for ref_name in &collector.names {
                if let Some(&(ref_schema, dep_idx)) = builtin_by_name.get(ref_name.as_str()) {
                    if dep_idx > idx {
                        violations.push(format!(
                            "{}.{} (index {}) references \
                             {}.{} (index {}), but the \
                             dependency appears later in \
                             BUILTINS_STATIC",
                            builtin.schema(),
                            builtin.name(),
                            idx,
                            ref_schema,
                            ref_name,
                            dep_idx,
                        ));
                    }
                }
            }
        }

        assert!(
            violations.is_empty(),
            "BUILTINS_STATIC has dependency ordering violations:\n{}",
            violations.join("\n"),
        );
    }

    /// Validates ontology metadata consistency:
    /// - Every link target references an entity that exists.
    /// - No duplicate entity names.
    /// - Every annotated builtin has a non-empty entity_name and description.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn test_ontology_consistency() {
        // Collect all entity names from builtins with ontology annotations.
        let mut entity_names: BTreeSet<String> = BTreeSet::new();
        let mut duplicate_entities = Vec::new();

        for builtin in BUILTINS_STATIC.iter() {
            let ontology = match builtin {
                Builtin::Table(t) => t.ontology.as_ref(),
                Builtin::View(v) => v.ontology.as_ref(),
                Builtin::MaterializedView(mv) => mv.ontology.as_ref(),
                Builtin::Source(s) => s.ontology.as_ref(),
                Builtin::Log(l) => l.ontology.as_ref(),
                _ => None,
            };
            if let Some(ont) = ontology {
                assert!(
                    !ont.entity_name.is_empty(),
                    "builtin {} has empty ontology entity_name",
                    builtin.name()
                );
                assert!(
                    !ont.description.is_empty(),
                    "builtin {} ({}) has empty ontology description",
                    builtin.name(),
                    ont.entity_name
                );
                if !entity_names.insert(ont.entity_name.to_string()) {
                    duplicate_entities.push(format!(
                        "duplicate entity_name {:?} on builtin {}",
                        ont.entity_name,
                        builtin.name()
                    ));
                }
            }
        }
        assert!(
            duplicate_entities.is_empty(),
            "ontology has duplicate entity names:\n{}",
            duplicate_entities.join("\n"),
        );

        // Validate link targets reference existing entities.
        let mut bad_targets = Vec::new();
        for builtin in BUILTINS_STATIC.iter() {
            let ontology = match builtin {
                Builtin::Table(t) => t.ontology.as_ref(),
                Builtin::View(v) => v.ontology.as_ref(),
                Builtin::MaterializedView(mv) => mv.ontology.as_ref(),
                Builtin::Source(s) => s.ontology.as_ref(),
                Builtin::Log(l) => l.ontology.as_ref(),
                _ => None,
            };
            if let Some(ont) = ontology {
                for link in ont.links {
                    if !entity_names.contains(link.target) {
                        bad_targets.push(format!(
                            "entity {:?} link {:?} targets {:?} which is not a known entity",
                            ont.entity_name, link.name, link.target
                        ));
                    }
                }
            }
        }
        assert!(
            bad_targets.is_empty(),
            "ontology has links targeting unknown entities:\n{}",
            bad_targets.join("\n"),
        );

        // Semantic type annotations are typed (SemanticType enum), so validity
        // is guaranteed at compile time — no runtime check needed.

        // Validate that every "reference" column (one whose semantic type implies
        // a FK relationship) is covered by an OntologyLink on entities that
        // already have at least one FK-style link.
        //
        // Scope: only entities that have started FK annotation (at least one
        // link with a source_column). Entities with only union/maps_to links,
        // or no links at all, are not yet fully annotated and are skipped to
        // avoid noise.
        //
        // Exemptions:
        // - Column at index 0 named "id": almost always the entity's own PK,
        //   not a FK (e.g. mz_objects.id, mz_functions.id).
        // - Columns in the relation's declared key set.
        //
        // "Reference" types are ID types that imply a FK. Discriminators
        // (ObjectType, ConnectionType, SourceType), OID, and metric types
        // (ByteCount, etc.) are excluded.
        let reference_sem_types: BTreeSet<SemanticType> = BTreeSet::from([
            SemanticType::CatalogItemId,
            SemanticType::GlobalId,
            SemanticType::ClusterId,
            SemanticType::ReplicaId,
            SemanticType::SchemaId,
            SemanticType::DatabaseId,
            SemanticType::RoleId,
            SemanticType::NetworkPolicyId,
        ]);

        let mut uncovered_fk_cols = Vec::new();
        for builtin in BUILTINS_STATIC.iter() {
            let desc_storage;
            let (name, desc, ontology): (&str, &RelationDesc, Option<&Ontology>) = match builtin {
                Builtin::Table(t) => (t.name, &t.desc, t.ontology.as_ref()),
                Builtin::View(v) => (v.name, &v.desc, v.ontology.as_ref()),
                Builtin::MaterializedView(mv) => (mv.name, &mv.desc, mv.ontology.as_ref()),
                Builtin::Source(s) => (s.name, &s.desc, s.ontology.as_ref()),
                Builtin::Log(l) => {
                    desc_storage = l.variant.desc();
                    (l.name, &desc_storage, l.ontology.as_ref())
                }
                _ => continue,
            };
            let Some(ont) = ontology else { continue };

            // Collect all source_column values declared by existing links.
            let linked_cols: BTreeSet<&str> = ont
                .links
                .iter()
                .filter_map(|link| match &link.properties {
                    LinkProperties::ForeignKey { source_column, .. } => Some(*source_column),
                    LinkProperties::Measures { source_column, .. } => Some(*source_column),
                    LinkProperties::DependsOn { source_column, .. } => Some(*source_column),
                    LinkProperties::MapsTo { source_column, .. } => Some(*source_column),
                    LinkProperties::Union { .. } => None,
                })
                .collect();

            // Skip entities that have no FK-style links yet — they are either
            // unannotated or use only union/maps_to links. Only enforce
            // coverage on entities that have started FK annotation.
            if linked_cols.is_empty() {
                continue;
            }

            // Column indices that are part of the declared key set.
            let pk_indices: BTreeSet<usize> = desc.typ().keys.iter().flatten().copied().collect();

            for (col_name, sem) in ont.column_semantic_types {
                if !reference_sem_types.contains(sem) {
                    continue;
                }
                let Some(idx) = desc.iter_names().position(|n| n.as_str() == *col_name) else {
                    continue;
                };
                // Exempt the entity's own primary identifier: column 0 named
                // "id" is by convention the entity's own PK (not a FK), even
                // when no explicit with_key() is declared on the relation.
                if idx == 0 && *col_name == "id" {
                    continue;
                }
                if pk_indices.contains(&idx) {
                    continue;
                }
                if linked_cols.contains(*col_name) {
                    continue;
                }
                uncovered_fk_cols.push(format!(
                    "entity {:?} (builtin {}) column {:?} has semantic type {:?} but no OntologyLink covers it (add a link with source_column: {:?})",
                    ont.entity_name, name, col_name, sem, col_name
                ));
            }
        }
        assert!(
            uncovered_fk_cols.is_empty(),
            "ontology entities have FK-typed columns with no OntologyLink:\n{}",
            uncovered_fk_cols.join("\n"),
        );

        // Validate that every source_column in a link actually names a column
        // in the entity's RelationDesc. This catches stale annotations after
        // column renames or removals. With typed LinkProperties this is mostly
        // belt-and-suspenders since the type system enforces field presence, but
        // we still need to verify the string value matches a real column.
        let mut bad_source_cols = Vec::new();
        for builtin in BUILTINS_STATIC.iter() {
            let desc_storage;
            let (name, desc, ontology): (&str, &RelationDesc, Option<&Ontology>) = match builtin {
                Builtin::Table(t) => (t.name, &t.desc, t.ontology.as_ref()),
                Builtin::View(v) => (v.name, &v.desc, v.ontology.as_ref()),
                Builtin::MaterializedView(mv) => (mv.name, &mv.desc, mv.ontology.as_ref()),
                Builtin::Source(s) => (s.name, &s.desc, s.ontology.as_ref()),
                Builtin::Log(l) => {
                    desc_storage = l.variant.desc();
                    (l.name, &desc_storage, l.ontology.as_ref())
                }
                _ => continue,
            };
            let Some(ont) = ontology else { continue };

            let col_names: BTreeSet<&str> = desc.iter_names().map(|c| c.as_str()).collect();

            for link in ont.links {
                let source_col = match &link.properties {
                    LinkProperties::ForeignKey { source_column, .. } => Some(*source_column),
                    LinkProperties::Measures { source_column, .. } => Some(*source_column),
                    LinkProperties::DependsOn { source_column, .. } => Some(*source_column),
                    LinkProperties::MapsTo { source_column, .. } => Some(*source_column),
                    LinkProperties::Union { .. } => None,
                };
                let Some(col) = source_col else { continue };
                if !col_names.contains(col) {
                    bad_source_cols.push(format!(
                        "entity {:?} (builtin {}) link {:?} references source_column {:?} which does not exist in the relation",
                        ont.entity_name, name, link.name, col
                    ));
                }
                let extra_key_columns = match &link.properties {
                    LinkProperties::ForeignKey {
                        extra_key_columns: Some(extras),
                        ..
                    } => Some(*extras),
                    LinkProperties::Measures {
                        extra_key_columns: Some(extras),
                        ..
                    } => Some(*extras),
                    _ => None,
                };
                if let Some(extras) = extra_key_columns {
                    for (src_col, _) in extras {
                        if !col_names.contains(*src_col) {
                            bad_source_cols.push(format!(
                                "entity {:?} (builtin {}) link {:?} extra_key_columns references {:?} which does not exist in the relation",
                                ont.entity_name, name, link.name, src_col
                            ));
                        }
                    }
                }
            }
        }
        assert!(
            bad_source_cols.is_empty(),
            "ontology links reference non-existent source_columns:\n{}",
            bad_source_cols.join("\n"),
        );

        // Sanity check: we have a reasonable number of annotated entities.
        assert!(
            entity_names.len() > 120,
            "expected > 120 ontology entities, found {}",
            entity_names.len()
        );
    }

    /// Verify that `LinkProperties` serializes to the same JSON that the old
    /// hand-written `properties_json` strings contained. One representative
    /// case per constructor/variant is enough — the important thing is that
    /// field names, enum tag values, and skip-if-None/false behaviour are all
    /// correct.
    #[mz_ore::test]
    fn test_link_properties_serialization() {
        let check = |props: LinkProperties, expected: &str| {
            let got = serde_json::to_string(&props).expect("serialize");
            let got_val: serde_json::Value = serde_json::from_str(&got).expect("parse got");
            let exp_val: serde_json::Value =
                serde_json::from_str(expected).expect("parse expected");
            assert_eq!(got_val, exp_val, "mismatch for {expected}");
        };

        // fk — basic, no optional fields
        check(
            LinkProperties::fk("owner_id", "id", Cardinality::ManyToOne),
            r#"{"kind":"foreign_key","source_column":"owner_id","target_column":"id","cardinality":"many_to_one"}"#,
        );
        // fk_composite — extra_key_columns present
        check(
            LinkProperties::fk_composite(
                "operator_id",
                "id",
                Cardinality::ManyToOne,
                &[("worker_id", "worker_id")],
            ),
            r#"{"kind":"foreign_key","source_column":"operator_id","target_column":"id","cardinality":"many_to_one","extra_key_columns":[["worker_id","worker_id"]]}"#,
        );
        // fk — one_to_one cardinality
        check(
            LinkProperties::fk("id", "id", Cardinality::OneToOne),
            r#"{"kind":"foreign_key","source_column":"id","target_column":"id","cardinality":"one_to_one"}"#,
        );
        // fk_nullable — nullable field present and true
        check(
            LinkProperties::fk_nullable("database_id", "id", Cardinality::ManyToOne),
            r#"{"kind":"foreign_key","source_column":"database_id","target_column":"id","cardinality":"many_to_one","nullable":true}"#,
        );
        // fk_typed — source_id_type present, requires_mapping absent
        check(
            LinkProperties::fk_typed(
                "replica_id",
                "id",
                Cardinality::ManyToOne,
                mz_repr::SemanticType::CatalogItemId,
            ),
            r#"{"kind":"foreign_key","source_column":"replica_id","target_column":"id","cardinality":"many_to_one","source_id_type":"CatalogItemId"}"#,
        );
        // fk_mapped — source_id_type + requires_mapping both present
        check(
            LinkProperties::fk_mapped(
                "object_id",
                "id",
                Cardinality::ManyToOne,
                mz_repr::SemanticType::GlobalId,
                "mz_internal.mz_object_global_ids",
            ),
            r#"{"kind":"foreign_key","source_column":"object_id","target_column":"id","cardinality":"many_to_one","source_id_type":"GlobalId","requires_mapping":"mz_internal.mz_object_global_ids"}"#,
        );
        // union_disc — discriminator fields present, note absent
        check(
            LinkProperties::union_disc("type", "table"),
            r#"{"kind":"union","discriminator_column":"type","discriminator_value":"table"}"#,
        );
        // Union — note only, discriminator absent
        check(
            LinkProperties::Union {
                discriminator_column: None,
                discriminator_value: None,
                note: Some("example note"),
            },
            r#"{"kind":"union","note":"example note"}"#,
        );
        // measures — basic
        check(
            LinkProperties::measures("id", "id", "cpu_time_ns"),
            r#"{"kind":"measures","source_column":"id","target_column":"id","metric":"cpu_time_ns"}"#,
        );
        // measures_composite — extra_key_columns present
        check(
            LinkProperties::measures_composite(
                "export_id",
                "export_id",
                "time_ns",
                &[("worker_id", "worker_id")],
            ),
            r#"{"kind":"measures","source_column":"export_id","target_column":"export_id","metric":"time_ns","extra_key_columns":[["worker_id","worker_id"]]}"#,
        );
        // measures_mapped — source_id_type + requires_mapping present
        check(
            LinkProperties::measures_mapped(
                "object_id",
                "id",
                "wallclock_lag",
                mz_repr::SemanticType::GlobalId,
                "mz_internal.mz_object_global_ids",
            ),
            r#"{"kind":"measures","source_column":"object_id","target_column":"id","metric":"wallclock_lag","source_id_type":"GlobalId","requires_mapping":"mz_internal.mz_object_global_ids"}"#,
        );
        // DependsOn — with mapping
        check(
            LinkProperties::DependsOn {
                source_column: "object_id",
                target_column: "id",
                source_id_type: Some(mz_repr::SemanticType::GlobalId),
                requires_mapping: Some("mz_internal.mz_object_global_ids"),
            },
            r#"{"kind":"depends_on","source_column":"object_id","target_column":"id","source_id_type":"GlobalId","requires_mapping":"mz_internal.mz_object_global_ids"}"#,
        );
        // DependsOn — direct (CatalogItemId, no mapping)
        check(
            LinkProperties::DependsOn {
                source_column: "object_id",
                target_column: "id",
                source_id_type: Some(mz_repr::SemanticType::CatalogItemId),
                requires_mapping: None,
            },
            r#"{"kind":"depends_on","source_column":"object_id","target_column":"id","source_id_type":"CatalogItemId"}"#,
        );
        // MapsTo — via + from_type + to_type
        check(
            LinkProperties::MapsTo {
                source_column: "id",
                target_column: "global_id",
                via: Some("mz_internal.mz_object_global_ids"),
                from_type: Some(mz_repr::SemanticType::CatalogItemId),
                to_type: Some(mz_repr::SemanticType::GlobalId),
                note: None,
            },
            r#"{"kind":"maps_to","source_column":"id","target_column":"global_id","via":"mz_internal.mz_object_global_ids","from_type":"CatalogItemId","to_type":"GlobalId"}"#,
        );
    }

    /// Verifies that the `mz_sources` materialized view fingerprint changes
    /// whenever a new builtin source or log is added.
    ///
    /// This is the correctness property that `make_mz_sources` provides: by
    /// inlining the full set of builtin sources/logs as VALUES in its SQL, any
    /// change to those sets is reflected in `fingerprint()`. A stale fingerprint
    /// would prevent the catalog migration from replacing `mz_sources`, leaving
    /// it with out-of-date data, silently serving stale builtin source rows.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn test_mz_sources_fingerprint_changes_with_new_builtin_source() {
        let sources: Vec<&'static BuiltinSource> = BUILTINS_STATIC
            .iter()
            .filter_map(|b| match b {
                Builtin::Source(x) => Some(*x),
                _ => None,
            })
            .collect();
        let logs: Vec<&'static BuiltinLog> = BUILTINS_STATIC
            .iter()
            .filter_map(|b| match b {
                Builtin::Log(x) => Some(*x),
                _ => None,
            })
            .collect();

        // The fingerprint from make_mz_sources must match the live BUILTINS_STATIC entry.
        let mv_base = builtin::make_mz_sources(sources.iter().copied(), logs.iter().copied());
        let fp_base = Fingerprint::fingerprint(&&mv_base);

        let mz_sources_static = BUILTINS_STATIC
            .iter()
            .find_map(|b| match b {
                Builtin::MaterializedView(mv) if mv.name == "mz_sources" => Some(*mv),
                _ => None,
            })
            .expect("mz_sources must be present in BUILTINS_STATIC");
        assert_eq!(
            fp_base,
            Fingerprint::fingerprint(&mz_sources_static),
            "make_mz_sources fingerprint must match the BUILTINS_STATIC mz_sources fingerprint"
        );

        // Adding an extra source must change the fingerprint, proving that
        // make_mz_sources inlines the source list into its SQL.
        let extra_source = sources[0];
        let mv_extra = builtin::make_mz_sources(
            sources.iter().copied().chain(std::iter::once(extra_source)),
            logs.iter().copied(),
        );
        assert_ne!(
            fp_base,
            Fingerprint::fingerprint(&&mv_extra),
            "mz_sources fingerprint must change when a builtin source is added"
        );
    }

    /// Verifies that the `mz_indexes` materialized view fingerprint changes
    /// whenever a new builtin index or log is added.
    ///
    /// This is the correctness property that `make_mz_indexes` provides: by
    /// inlining the full set of builtin indexes/logs as VALUES in its SQL,
    /// any change to those sets is reflected in `fingerprint()`. A stale
    /// fingerprint would prevent the catalog migration from replacing
    /// `mz_indexes`, leaving it with out-of-date data, silently serving
    /// stale builtin index rows.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn test_mz_indexes_fingerprint_changes_with_new_builtin_index() {
        let indexes: Vec<&'static BuiltinIndex> = BUILTINS_STATIC
            .iter()
            .filter_map(|b| match b {
                Builtin::Index(x) => Some(*x),
                _ => None,
            })
            .collect();
        let logs: Vec<&'static BuiltinLog> = BUILTINS_STATIC
            .iter()
            .filter_map(|b| match b {
                Builtin::Log(x) => Some(*x),
                _ => None,
            })
            .collect();

        // The fingerprint from make_mz_indexes must match the live BUILTINS_STATIC entry.
        let mv_base = mz_catalog::make_mz_indexes(indexes.iter().copied(), logs.iter().copied());
        let fp_base = Fingerprint::fingerprint(&&mv_base);

        let mz_indexes_static = BUILTINS_STATIC
            .iter()
            .find_map(|b| match b {
                Builtin::MaterializedView(mv) if mv.name == "mz_indexes" => Some(*mv),
                _ => None,
            })
            .expect("mz_indexes must be present in BUILTINS_STATIC");
        assert_eq!(
            fp_base,
            Fingerprint::fingerprint(&mz_indexes_static),
            "make_mz_indexes fingerprint must match the BUILTINS_STATIC mz_indexes fingerprint"
        );

        // Adding an extra index must change the fingerprint, proving that
        // make_mz_indexes inlines the index list into its SQL.
        let extra_index = indexes[0];
        let mv_extra_index = mz_catalog::make_mz_indexes(
            indexes.iter().copied().chain(std::iter::once(extra_index)),
            logs.iter().copied(),
        );
        assert_ne!(
            fp_base,
            Fingerprint::fingerprint(&&mv_extra_index),
            "mz_indexes fingerprint must change when a builtin index is added"
        );

        // Adding an extra log must also change the fingerprint, because the
        // log set feeds the introspection-source-indexes CTE.
        let extra_log = logs[0];
        let mv_extra_log = mz_catalog::make_mz_indexes(
            indexes.iter().copied(),
            logs.iter().copied().chain(std::iter::once(extra_log)),
        );
        assert_ne!(
            fp_base,
            Fingerprint::fingerprint(&&mv_extra_log),
            "mz_indexes fingerprint must change when a builtin log is added"
        );
    }
}
