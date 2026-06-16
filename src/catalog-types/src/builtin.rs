// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Builtin catalog object types needed by the durable catalog.
//!
//! `BuiltinLog` and the builtin role definitions live here so that
//! `mz-catalog-durable` can reference them without depending on `mz-catalog`.
//! The full `Builtin` family and the builtin object tables remain in
//! `mz-catalog`.

use mz_compute_client::logging::LogVariant;
use mz_pgrepr::oid;
use mz_repr::SemanticType;
use mz_repr::adt::mz_acl_item::MzAclItem;
use mz_repr::role_id::RoleId;
use mz_sql_types::catalog::RoleAttributesRaw;
use mz_sql_types::session::user::{
    ANALYTICS_USER_NAME, JWT_SYNC_ROLE_NAME, MZ_ANALYTICS_ROLE_ID, MZ_JWT_SYNC_ROLE_ID,
    MZ_MONITOR_REDACTED_ROLE_ID, MZ_MONITOR_ROLE_ID, MZ_SUPPORT_ROLE_ID, MZ_SYSTEM_ROLE_ID,
    SUPPORT_USER_NAME, SYSTEM_USER_NAME,
};
use serde::Serialize;

// --- BuiltinLog and ontology metadata ---
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

// --- Builtin roles ---
#[derive(Clone, Debug)]
pub struct BuiltinRole {
    pub id: RoleId,
    /// Name of the builtin role.
    ///
    /// IMPORTANT: Must start with a prefix from `BUILTIN_PREFIXES`.
    pub name: &'static str,
    pub oid: u32,
    pub attributes: RoleAttributesRaw,
}

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

pub const BUILTIN_ROLES: &[&BuiltinRole] = &[
    &MZ_SYSTEM_ROLE,
    &MZ_SUPPORT_ROLE,
    &MZ_ANALYTICS_ROLE,
    &MZ_MONITOR_ROLE,
    &MZ_MONITOR_REDACTED,
    &MZ_JWT_SYNC_ROLE,
];
