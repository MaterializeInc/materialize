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

pub mod notice;

use std::collections::BTreeMap;
use std::hash::Hash;
use std::string::ToString;
use std::sync::LazyLock;

use mz_compute_client::logging::{ComputeLog, DifferentialLog, LogVariant, TimelyLog};
use mz_ore::collections::HashMap;
use mz_pgrepr::oid;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::namespaces::{
    INFORMATION_SCHEMA, MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA, MZ_INTROSPECTION_SCHEMA,
    MZ_UNSAFE_SCHEMA, PG_CATALOG_SCHEMA,
};
use mz_repr::role_id::RoleId;
use mz_repr::{RelationDesc, SqlRelationType, SqlScalarType};
use mz_sql::catalog::RoleAttributesRaw;
use mz_sql::catalog::{
    CatalogItemType, CatalogType, CatalogTypeDetails, CatalogTypePgMetadata, NameReference,
    ObjectType, SystemObjectType, TypeReference,
};
use mz_sql::rbac;
use mz_sql::session::user::{
    ANALYTICS_USER_NAME, MZ_ANALYTICS_ROLE_ID, MZ_MONITOR_REDACTED_ROLE_ID, MZ_MONITOR_ROLE_ID,
    MZ_SUPPORT_ROLE_ID, MZ_SYSTEM_ROLE_ID, SUPPORT_USER_NAME, SYSTEM_USER_NAME,
};
use mz_storage_client::controller::IntrospectionType;
use mz_storage_client::healthcheck::WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RAW_DESC;
use mz_storage_client::healthcheck::{
    MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY_DESC, MZ_PREPARED_STATEMENT_HISTORY_DESC,
    MZ_SESSION_HISTORY_DESC, MZ_SINK_STATUS_HISTORY_DESC, MZ_SOURCE_STATUS_HISTORY_DESC,
    MZ_SQL_TEXT_DESC, MZ_STATEMENT_EXECUTION_HISTORY_DESC, REPLICA_METRICS_HISTORY_DESC,
    REPLICA_STATUS_HISTORY_DESC, WALLCLOCK_LAG_HISTORY_DESC,
};
use mz_storage_client::statistics::{MZ_SINK_STATISTICS_RAW_DESC, MZ_SOURCE_STATISTICS_RAW_DESC};
use serde::Serialize;

use crate::durable::objects::SystemObjectDescription;

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
    Type(&'static BuiltinType<T>),
    Func(BuiltinFunc),
    Source(&'static BuiltinSource),
    ContinualTask(&'static BuiltinContinualTask),
    Index(&'static BuiltinIndex),
    Connection(&'static BuiltinConnection),
}

impl<T: TypeReference> Builtin<T> {
    pub fn name(&self) -> &'static str {
        match self {
            Builtin::Log(log) => log.name,
            Builtin::Table(table) => table.name,
            Builtin::View(view) => view.name,
            Builtin::Type(typ) => typ.name,
            Builtin::Func(func) => func.name,
            Builtin::Source(coll) => coll.name,
            Builtin::ContinualTask(ct) => ct.name,
            Builtin::Index(index) => index.name,
            Builtin::Connection(connection) => connection.name,
        }
    }

    pub fn schema(&self) -> &'static str {
        match self {
            Builtin::Log(log) => log.schema,
            Builtin::Table(table) => table.schema,
            Builtin::View(view) => view.schema,
            Builtin::Type(typ) => typ.schema,
            Builtin::Func(func) => func.schema,
            Builtin::Source(coll) => coll.schema,
            Builtin::ContinualTask(ct) => ct.schema,
            Builtin::Index(index) => index.schema,
            Builtin::Connection(connection) => connection.schema,
        }
    }

    pub fn catalog_item_type(&self) -> CatalogItemType {
        match self {
            Builtin::Log(_) => CatalogItemType::Source,
            Builtin::Source(_) => CatalogItemType::Source,
            Builtin::ContinualTask(_) => CatalogItemType::ContinualTask,
            Builtin::Table(_) => CatalogItemType::Table,
            Builtin::View(_) => CatalogItemType::View,
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
}

#[derive(Clone, Debug, Hash, Serialize)]
pub struct BuiltinSource {
    pub name: &'static str,
    pub schema: &'static str,
    pub oid: u32,
    pub desc: RelationDesc,
    pub column_comments: BTreeMap<&'static str, &'static str>,
    pub data_source: IntrospectionType,
    /// Whether the source's retention policy is controlled by
    /// the system variable `METRICS_RETENTION`
    pub is_retained_metrics_object: bool,
    /// ACL items to apply to the object
    pub access: Vec<MzAclItem>,
}

#[derive(Hash, Debug, PartialEq, Eq)]
pub struct BuiltinContinualTask {
    pub name: &'static str,
    pub schema: &'static str,
    pub oid: u32,
    pub desc: RelationDesc,
    pub sql: &'static str,
    /// ACL items to apply to the object
    pub access: Vec<MzAclItem>,
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
}

impl BuiltinView {
    pub fn create_sql(&self) -> String {
        format!("CREATE VIEW {}.{} AS {}", self.schema, self.name, self.sql)
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

impl BuiltinContinualTask {
    pub fn create_sql(&self) -> String {
        format!(
            "CREATE CONTINUAL TASK {}.{}\n{}",
            self.schema, self.name, self.sql
        )
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
            Builtin::Type(typ) => typ.fingerprint(),
            Builtin::Func(func) => func.fingerprint(),
            Builtin::Source(coll) => coll.fingerprint(),
            Builtin::ContinualTask(ct) => ct.fingerprint(),
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

impl Fingerprint for &BuiltinContinualTask {
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

// Builtin definitions below. Ensure you add new builtins to the `BUILTINS` map.
//
// You SHOULD NOT delete a builtin. If you do, you will break any downstream
// user objects that depended on the builtin.
//
// Builtins are loaded in dependency order, so a builtin must appear in `BUILTINS`
// before any items it depends upon.
//
// WARNING: if you change the definition of an existing builtin item, you must
// be careful to maintain backwards compatibility! Adding new columns is safe.
// Removing a column, changing the name of a column, or changing the type of a
// column is not safe, as persisted user views may depend upon that column.

// The following types are the list of builtin data types available
// in Materialize. This list is derived from the `pg_type` table in PostgreSQL.
//
// Builtin types cannot be created, updated, or deleted. Their OIDs
// are static, unlike other objects, to match the type OIDs defined by Postgres.

pub const TYPE_BOOL: BuiltinType<NameReference> = BuiltinType {
    name: "bool",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_BOOL_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Bool,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 1242,
            typreceive_oid: 2436,
        }),
    },
};

pub const TYPE_BYTEA: BuiltinType<NameReference> = BuiltinType {
    name: "bytea",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_BYTEA_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Bytes,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 1244,
            typreceive_oid: 2412,
        }),
    },
};

pub const TYPE_INT8: BuiltinType<NameReference> = BuiltinType {
    name: "int8",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_INT8_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Int64,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 460,
            typreceive_oid: 2408,
        }),
    },
};

pub const TYPE_INT4: BuiltinType<NameReference> = BuiltinType {
    name: "int4",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_INT4_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Int32,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 42,
            typreceive_oid: 2406,
        }),
    },
};

pub const TYPE_TEXT: BuiltinType<NameReference> = BuiltinType {
    name: "text",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_TEXT_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::String,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 46,
            typreceive_oid: 2414,
        }),
    },
};

pub const TYPE_OID: BuiltinType<NameReference> = BuiltinType {
    name: "oid",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_OID_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Oid,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 1798,
            typreceive_oid: 2418,
        }),
    },
};

pub const TYPE_FLOAT4: BuiltinType<NameReference> = BuiltinType {
    name: "float4",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_FLOAT4_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Float32,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 200,
            typreceive_oid: 2424,
        }),
    },
};

pub const TYPE_FLOAT8: BuiltinType<NameReference> = BuiltinType {
    name: "float8",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_FLOAT8_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Float64,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 214,
            typreceive_oid: 2426,
        }),
    },
};

pub const TYPE_BOOL_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_bool",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_BOOL_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_BOOL.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_BYTEA_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_bytea",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_BYTEA_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_BYTEA.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_INT4_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_int4",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_INT4_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_INT4.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_TEXT_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_text",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_TEXT_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_TEXT.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_INT8_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_int8",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_INT8_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_INT8.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_FLOAT4_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_float4",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_FLOAT4_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_FLOAT4.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_FLOAT8_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_float8",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_FLOAT8_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_FLOAT8.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_OID_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_oid",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_OID_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_OID.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_DATE: BuiltinType<NameReference> = BuiltinType {
    name: "date",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_DATE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Date,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 1084,
            typreceive_oid: 2468,
        }),
    },
};

pub const TYPE_TIME: BuiltinType<NameReference> = BuiltinType {
    name: "time",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_TIME_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Time,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 1143,
            typreceive_oid: 2470,
        }),
    },
};

pub const TYPE_TIMESTAMP: BuiltinType<NameReference> = BuiltinType {
    name: "timestamp",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_TIMESTAMP_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Timestamp,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 1312,
            typreceive_oid: 2474,
        }),
    },
};

pub const TYPE_TIMESTAMP_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_timestamp",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_TIMESTAMP_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_TIMESTAMP.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_DATE_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_date",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_DATE_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_DATE.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_TIME_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_time",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_TIME_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_TIME.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_TIMESTAMPTZ: BuiltinType<NameReference> = BuiltinType {
    name: "timestamptz",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_TIMESTAMPTZ_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::TimestampTz,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 1150,
            typreceive_oid: 2476,
        }),
    },
};

pub const TYPE_TIMESTAMPTZ_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_timestamptz",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_TIMESTAMPTZ_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_TIMESTAMPTZ.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_INTERVAL: BuiltinType<NameReference> = BuiltinType {
    name: "interval",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_INTERVAL_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Interval,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 1160,
            typreceive_oid: 2478,
        }),
    },
};

pub const TYPE_INTERVAL_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_interval",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_INTERVAL_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_INTERVAL.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_NAME: BuiltinType<NameReference> = BuiltinType {
    name: "name",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_NAME_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::PgLegacyName,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 34,
            typreceive_oid: 2422,
        }),
    },
};

pub const TYPE_NAME_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_name",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_NAME_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_NAME.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_NUMERIC: BuiltinType<NameReference> = BuiltinType {
    name: "numeric",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_NUMERIC_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Numeric,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 1701,
            typreceive_oid: 2460,
        }),
    },
};

pub const TYPE_NUMERIC_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_numeric",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_NUMERIC_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_NUMERIC.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_RECORD: BuiltinType<NameReference> = BuiltinType {
    name: "record",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_RECORD_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 2290,
            typreceive_oid: 2402,
        }),
    },
};

pub const TYPE_RECORD_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_record",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_RECORD_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_RECORD.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_UUID: BuiltinType<NameReference> = BuiltinType {
    name: "uuid",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_UUID_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Uuid,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 2952,
            typreceive_oid: 2961,
        }),
    },
};

pub const TYPE_UUID_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_uuid",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_UUID_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_UUID.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_JSONB: BuiltinType<NameReference> = BuiltinType {
    name: "jsonb",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_JSONB_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Jsonb,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 3806,
            typreceive_oid: 3805,
        }),
    },
};

pub const TYPE_JSONB_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_jsonb",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_JSONB_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_JSONB.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_ANY: BuiltinType<NameReference> = BuiltinType {
    name: "any",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 2294,
            typreceive_oid: 0,
        }),
    },
};

pub const TYPE_ANYARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "anyarray",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 2296,
            typreceive_oid: 2502,
        }),
    },
};

pub const TYPE_ANYELEMENT: BuiltinType<NameReference> = BuiltinType {
    name: "anyelement",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYELEMENT_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 2312,
            typreceive_oid: 0,
        }),
    },
};

pub const TYPE_ANYNONARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "anynonarray",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYNONARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 2777,
            typreceive_oid: 0,
        }),
    },
};

pub const TYPE_ANYRANGE: BuiltinType<NameReference> = BuiltinType {
    name: "anyrange",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYRANGE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 3832,
            typreceive_oid: 0,
        }),
    },
};

pub const TYPE_CHAR: BuiltinType<NameReference> = BuiltinType {
    name: "char",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_CHAR_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::PgLegacyChar,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 1245,
            typreceive_oid: 2434,
        }),
    },
};

pub const TYPE_VARCHAR: BuiltinType<NameReference> = BuiltinType {
    name: "varchar",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_VARCHAR_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::VarChar,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 1046,
            typreceive_oid: 2432,
        }),
    },
};

pub const TYPE_INT2: BuiltinType<NameReference> = BuiltinType {
    name: "int2",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_INT2_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Int16,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 38,
            typreceive_oid: 2404,
        }),
    },
};

pub const TYPE_INT2_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_int2",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_INT2_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_INT2.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_BPCHAR: BuiltinType<NameReference> = BuiltinType {
    name: "bpchar",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_BPCHAR_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Char,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 1044,
            typreceive_oid: 2430,
        }),
    },
};

pub const TYPE_CHAR_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_char",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_CHAR_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_CHAR.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_VARCHAR_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_varchar",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_VARCHAR_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_VARCHAR.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_BPCHAR_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_bpchar",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_BPCHAR_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_BPCHAR.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_REGPROC: BuiltinType<NameReference> = BuiltinType {
    name: "regproc",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_REGPROC_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::RegProc,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 44,
            typreceive_oid: 2444,
        }),
    },
};

pub const TYPE_REGPROC_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_regproc",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_REGPROC_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_REGPROC.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_REGTYPE: BuiltinType<NameReference> = BuiltinType {
    name: "regtype",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_REGTYPE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::RegType,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 2220,
            typreceive_oid: 2454,
        }),
    },
};

pub const TYPE_REGTYPE_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_regtype",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_REGTYPE_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_REGTYPE.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_REGCLASS: BuiltinType<NameReference> = BuiltinType {
    name: "regclass",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_REGCLASS_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::RegClass,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 2218,
            typreceive_oid: 2452,
        }),
    },
};

pub const TYPE_REGCLASS_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_regclass",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_REGCLASS_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_REGCLASS.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_INT2_VECTOR: BuiltinType<NameReference> = BuiltinType {
    name: "int2vector",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_INT2_VECTOR_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Int2Vector,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 40,
            typreceive_oid: 2410,
        }),
    },
};

pub const TYPE_INT2_VECTOR_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_int2vector",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_INT2_VECTOR_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_INT2_VECTOR.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_ANYCOMPATIBLE: BuiltinType<NameReference> = BuiltinType {
    name: "anycompatible",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYCOMPATIBLE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 5086,
            typreceive_oid: 0,
        }),
    },
};

pub const TYPE_ANYCOMPATIBLEARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "anycompatiblearray",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYCOMPATIBLEARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 5088,
            typreceive_oid: 5090,
        }),
    },
};

pub const TYPE_ANYCOMPATIBLENONARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "anycompatiblenonarray",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYCOMPATIBLENONARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 5092,
            typreceive_oid: 0,
        }),
    },
};

pub const TYPE_ANYCOMPATIBLERANGE: BuiltinType<NameReference> = BuiltinType {
    name: "anycompatiblerange",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYCOMPATIBLERANGE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 5094,
            typreceive_oid: 0,
        }),
    },
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

pub const TYPE_INT4_RANGE: BuiltinType<NameReference> = BuiltinType {
    name: "int4range",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_INT4RANGE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Range {
            element_reference: TYPE_INT4.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 3834,
            typreceive_oid: 3836,
        }),
    },
};

pub const TYPE_INT4_RANGE_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_int4range",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_INT4RANGE_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_INT4_RANGE.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_INT8_RANGE: BuiltinType<NameReference> = BuiltinType {
    name: "int8range",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_INT8RANGE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Range {
            element_reference: TYPE_INT8.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 3834,
            typreceive_oid: 3836,
        }),
    },
};

pub const TYPE_INT8_RANGE_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_int8range",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_INT8RANGE_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_INT8_RANGE.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_DATE_RANGE: BuiltinType<NameReference> = BuiltinType {
    name: "daterange",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_DATERANGE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Range {
            element_reference: TYPE_DATE.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 3834,
            typreceive_oid: 3836,
        }),
    },
};

pub const TYPE_DATE_RANGE_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_daterange",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_DATERANGE_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_DATE_RANGE.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_NUM_RANGE: BuiltinType<NameReference> = BuiltinType {
    name: "numrange",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_NUMRANGE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Range {
            element_reference: TYPE_NUMERIC.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 3834,
            typreceive_oid: 3836,
        }),
    },
};

pub const TYPE_NUM_RANGE_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_numrange",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_NUMRANGE_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_NUM_RANGE.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_TS_RANGE: BuiltinType<NameReference> = BuiltinType {
    name: "tsrange",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_TSRANGE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Range {
            element_reference: TYPE_TIMESTAMP.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 3834,
            typreceive_oid: 3836,
        }),
    },
};

pub const TYPE_TS_RANGE_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_tsrange",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_TSRANGE_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_TS_RANGE.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_TSTZ_RANGE: BuiltinType<NameReference> = BuiltinType {
    name: "tstzrange",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_TSTZRANGE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Range {
            element_reference: TYPE_TIMESTAMPTZ.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 3834,
            typreceive_oid: 3836,
        }),
    },
};

pub const TYPE_TSTZ_RANGE_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_tstzrange",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_TSTZRANGE_ARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_TSTZ_RANGE.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
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

pub const TYPE_ACL_ITEM: BuiltinType<NameReference> = BuiltinType {
    name: "aclitem",
    schema: PG_CATALOG_SCHEMA,
    oid: 1033,
    details: CatalogTypeDetails {
        typ: CatalogType::AclItem,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 1031,
            typreceive_oid: 0,
        }),
    },
};

pub const TYPE_ACL_ITEM_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_aclitem",
    schema: PG_CATALOG_SCHEMA,
    oid: 1034,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_reference: TYPE_ACL_ITEM.name,
        },
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 750,
            typreceive_oid: 2400,
        }),
    },
};

pub const TYPE_INTERNAL: BuiltinType<NameReference> = BuiltinType {
    name: "internal",
    schema: PG_CATALOG_SCHEMA,
    oid: 2281,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
        pg_metadata: Some(CatalogTypePgMetadata {
            typinput_oid: 2304,
            typreceive_oid: 0,
        }),
    },
};

const PUBLIC_SELECT: MzAclItem = MzAclItem {
    grantee: RoleId::Public,
    grantor: MZ_SYSTEM_ROLE_ID,
    acl_mode: AclMode::SELECT,
};

const SUPPORT_SELECT: MzAclItem = MzAclItem {
    grantee: MZ_SUPPORT_ROLE_ID,
    grantor: MZ_SYSTEM_ROLE_ID,
    acl_mode: AclMode::SELECT,
};

const ANALYTICS_SELECT: MzAclItem = MzAclItem {
    grantee: MZ_ANALYTICS_ROLE_ID,
    grantor: MZ_SYSTEM_ROLE_ID,
    acl_mode: AclMode::SELECT,
};

const MONITOR_SELECT: MzAclItem = MzAclItem {
    grantee: MZ_MONITOR_ROLE_ID,
    grantor: MZ_SYSTEM_ROLE_ID,
    acl_mode: AclMode::SELECT,
};

const MONITOR_REDACTED_SELECT: MzAclItem = MzAclItem {
    grantee: MZ_MONITOR_REDACTED_ROLE_ID,
    grantor: MZ_SYSTEM_ROLE_ID,
    acl_mode: AclMode::SELECT,
};

pub static MZ_DATAFLOW_OPERATORS_PER_WORKER: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_dataflow_operators_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_DATAFLOW_OPERATORS_PER_WORKER_OID,
    variant: LogVariant::Timely(TimelyLog::Operates),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_DATAFLOW_ADDRESSES_PER_WORKER: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_dataflow_addresses_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_DATAFLOW_ADDRESSES_PER_WORKER_OID,
    variant: LogVariant::Timely(TimelyLog::Addresses),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_DATAFLOW_CHANNELS_PER_WORKER: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_dataflow_channels_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_DATAFLOW_CHANNELS_PER_WORKER_OID,
    variant: LogVariant::Timely(TimelyLog::Channels),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_SCHEDULING_ELAPSED_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_scheduling_elapsed_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_SCHEDULING_ELAPSED_RAW_OID,
    variant: LogVariant::Timely(TimelyLog::Elapsed),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM_RAW: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_compute_operator_durations_histogram_raw",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM_RAW_OID,
        variant: LogVariant::Timely(TimelyLog::Histogram),
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_SCHEDULING_PARKS_HISTOGRAM_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_scheduling_parks_histogram_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_SCHEDULING_PARKS_HISTOGRAM_RAW_OID,
    variant: LogVariant::Timely(TimelyLog::Parks),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_ARRANGEMENT_RECORDS_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_arrangement_records_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_ARRANGEMENT_RECORDS_RAW_OID,
    variant: LogVariant::Differential(DifferentialLog::ArrangementRecords),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_ARRANGEMENT_BATCHES_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_arrangement_batches_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_ARRANGEMENT_BATCHES_RAW_OID,
    variant: LogVariant::Differential(DifferentialLog::ArrangementBatches),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_ARRANGEMENT_SHARING_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_arrangement_sharing_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_ARRANGEMENT_SHARING_RAW_OID,
    variant: LogVariant::Differential(DifferentialLog::Sharing),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_ARRANGEMENT_BATCHER_RECORDS_RAW: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_arrangement_batcher_records_raw",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_ARRANGEMENT_BATCHER_RECORDS_RAW_OID,
        variant: LogVariant::Differential(DifferentialLog::BatcherRecords),
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_ARRANGEMENT_BATCHER_SIZE_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_arrangement_batcher_size_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_ARRANGEMENT_BATCHER_SIZE_RAW_OID,
    variant: LogVariant::Differential(DifferentialLog::BatcherSize),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_ARRANGEMENT_BATCHER_CAPACITY_RAW: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_arrangement_batcher_capacity_raw",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_ARRANGEMENT_BATCHER_CAPACITY_RAW_OID,
        variant: LogVariant::Differential(DifferentialLog::BatcherCapacity),
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_ARRANGEMENT_BATCHER_ALLOCATIONS_RAW: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_arrangement_batcher_allocations_raw",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_ARRANGEMENT_BATCHER_ALLOCATIONS_RAW_OID,
        variant: LogVariant::Differential(DifferentialLog::BatcherAllocations),
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_COMPUTE_EXPORTS_PER_WORKER: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_compute_exports_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_COMPUTE_EXPORTS_PER_WORKER_OID,
    variant: LogVariant::Compute(ComputeLog::DataflowCurrent),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_COMPUTE_DATAFLOW_GLOBAL_IDS_PER_WORKER: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_compute_dataflow_global_ids_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_COMPUTE_DATAFLOW_GLOBAL_IDS_PER_WORKER_OID,
        variant: LogVariant::Compute(ComputeLog::DataflowGlobal),
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_COMPUTE_FRONTIERS_PER_WORKER: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_compute_frontiers_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_COMPUTE_FRONTIERS_PER_WORKER_OID,
    variant: LogVariant::Compute(ComputeLog::FrontierCurrent),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_COMPUTE_IMPORT_FRONTIERS_PER_WORKER: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_compute_import_frontiers_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_COMPUTE_IMPORT_FRONTIERS_PER_WORKER_OID,
        variant: LogVariant::Compute(ComputeLog::ImportFrontierCurrent),
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_COMPUTE_ERROR_COUNTS_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_compute_error_counts_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_COMPUTE_ERROR_COUNTS_RAW_OID,
    variant: LogVariant::Compute(ComputeLog::ErrorCount),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_COMPUTE_HYDRATION_TIMES_PER_WORKER: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_compute_hydration_times_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_COMPUTE_HYDRATION_TIMES_PER_WORKER_OID,
        variant: LogVariant::Compute(ComputeLog::HydrationTime),
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_COMPUTE_OPERATOR_HYDRATION_STATUSES_PER_WORKER: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_compute_operator_hydration_statuses_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_COMPUTE_OPERATOR_HYDRATION_STATUSES_PER_WORKER_OID,
        variant: LogVariant::Compute(ComputeLog::OperatorHydrationStatus),
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_ACTIVE_PEEKS_PER_WORKER: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_active_peeks_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_ACTIVE_PEEKS_PER_WORKER_OID,
    variant: LogVariant::Compute(ComputeLog::PeekCurrent),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_COMPUTE_LIR_MAPPING_PER_WORKER: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_compute_lir_mapping_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_COMPUTE_LIR_MAPPING_PER_WORKER_OID,
    variant: LogVariant::Compute(ComputeLog::LirMapping),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_PEEK_DURATIONS_HISTOGRAM_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_peek_durations_histogram_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_PEEK_DURATIONS_HISTOGRAM_RAW_OID,
    variant: LogVariant::Compute(ComputeLog::PeekDuration),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_DATAFLOW_SHUTDOWN_DURATIONS_HISTOGRAM_RAW: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_dataflow_shutdown_durations_histogram_raw",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_DATAFLOW_SHUTDOWN_DURATIONS_HISTOGRAM_RAW_OID,
        variant: LogVariant::Compute(ComputeLog::ShutdownDuration),
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_ARRANGEMENT_HEAP_SIZE_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_arrangement_heap_size_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_ARRANGEMENT_HEAP_SIZE_RAW_OID,
    variant: LogVariant::Compute(ComputeLog::ArrangementHeapSize),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_ARRANGEMENT_HEAP_CAPACITY_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_arrangement_heap_capacity_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_ARRANGEMENT_HEAP_CAPACITY_RAW_OID,
    variant: LogVariant::Compute(ComputeLog::ArrangementHeapCapacity),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_ARRANGEMENT_HEAP_ALLOCATIONS_RAW: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_arrangement_heap_allocations_raw",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_ARRANGEMENT_HEAP_ALLOCATIONS_RAW_OID,
        variant: LogVariant::Compute(ComputeLog::ArrangementHeapAllocations),
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_MESSAGE_BATCH_COUNTS_RECEIVED_RAW: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_message_batch_counts_received_raw",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_MESSAGE_BATCH_COUNTS_RECEIVED_RAW_OID,
        variant: LogVariant::Timely(TimelyLog::BatchesReceived),
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_MESSAGE_BATCH_COUNTS_SENT_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_message_batch_counts_sent_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_MESSAGE_BATCH_COUNTS_SENT_RAW_OID,
    variant: LogVariant::Timely(TimelyLog::BatchesSent),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_MESSAGE_COUNTS_RECEIVED_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_message_counts_received_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_MESSAGE_COUNTS_RECEIVED_RAW_OID,
    variant: LogVariant::Timely(TimelyLog::MessagesReceived),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_MESSAGE_COUNTS_SENT_RAW: LazyLock<BuiltinLog> = LazyLock::new(|| BuiltinLog {
    name: "mz_message_counts_sent_raw",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::LOG_MZ_MESSAGE_COUNTS_SENT_RAW_OID,
    variant: LogVariant::Timely(TimelyLog::MessagesSent),
    access: vec![PUBLIC_SELECT],
});

pub static MZ_DATAFLOW_OPERATOR_REACHABILITY_RAW: LazyLock<BuiltinLog> =
    LazyLock::new(|| BuiltinLog {
        name: "mz_dataflow_operator_reachability_raw",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::LOG_MZ_DATAFLOW_OPERATOR_REACHABILITY_RAW_OID,
        variant: LogVariant::Timely(TimelyLog::Reachability),
        access: vec![PUBLIC_SELECT],
    });

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
        ("namespace", "The namespace of the sink."),
        ("table", "The table the sink is writing to."),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
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
            "The schema (or, database) of the upstream table being ingested.",
        ),
        (
            "table_name",
            "The name of the upstream table being ingested.",
        ),
    ]),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
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
            "The format of the Kafka message key: `avro`, `protobuf`, `csv`, `regex`, `bytes`, `json`, `text`, or `NULL`.",
        ),
        (
            "value_format",
            "The format of the Kafka message value: `avro`, `protobuf`, `csv`, `regex`, `bytes`, `json`, `text`. `NULL` for other source types.",
        ),
    ]),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
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
});
pub static MZ_COMPUTE_DEPENDENCIES: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_compute_dependencies",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_COMPUTE_DEPENDENCIES_OID,
    data_source: IntrospectionType::ComputeDependencies,
    desc: RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("dependency_id", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "object_id",
            "The ID of a compute object. Corresponds to `mz_catalog.mz_indexes.id`, `mz_catalog.mz_materialized_views.id`, or `mz_internal.mz_subscriptions`.",
        ),
        (
            "dependency_id",
            "The ID of a compute dependency. Corresponds to `mz_catalog.mz_indexes.id`, `mz_catalog.mz_materialized_views.id`, `mz_catalog.mz_sources.id`, or `mz_catalog.mz_tables.id`.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
});

pub static MZ_DATABASES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_databases",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_DATABASES_OID,
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
        (
            "oid",
            "A [PostgreSQL-compatible OID][`oid`] for the database.",
        ),
        ("name", "The name of the database."),
        (
            "owner_id",
            "The role ID of the owner of the database. Corresponds to `mz_roles.id`.",
        ),
        ("privileges", "The privileges belonging to the database."),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
});
pub static MZ_SCHEMAS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_schemas",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_SCHEMAS_OID,
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
        (
            "oid",
            "A [PostgreSQL-compatible oid][`oid`] for the schema.",
        ),
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
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
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
        ("oid", "A [PostgreSQL-compatible OID][`oid`] for the index."),
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
        ("oid", "A [PostgreSQL-compatible OID][`oid`] for the table."),
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
});
pub static MZ_CONNECTIONS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_connections",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_CONNECTIONS_OID,
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
        (
            "oid",
            "A [PostgreSQL-compatible OID][`oid`] for the connection.",
        ),
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
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
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
        (
            "oid",
            "A [PostgreSQL-compatible OID][`oid`] for the source.",
        ),
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
        ("size", "Deprecated The size of the source."),
        (
            "envelope_type",
            "For Kafka sources, the envelope type: `none`, `upsert`, or `debezium`. `NULL` for other source types.",
        ),
        (
            "key_format",
            "For Kafka sources, the format of the Kafka message key: `avro`, `protobuf`, `csv`, `regex`, `bytes`, `json`, `text`, or `NULL`.",
        ),
        (
            "value_format",
            "For Kafka sources, the format of the Kafka message value: `avro`, `protobuf`, `csv`, `regex`, `bytes`, `json`, `text`. `NULL` for other source types.",
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
            ("oid", "A [PostgreSQL-compatible OID][`oid`] for the sink."),
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
                "Deprecated The format of the Kafka messages produced by the sink: `avro`, `json`, `text`, or `bytes`.",
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
        ("oid", "A [PostgreSQL-compatible OID][`oid`] for the view."),
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
});
pub static MZ_MATERIALIZED_VIEWS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_materialized_views",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_MATERIALIZED_VIEWS_OID,
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
            "A [PostgreSQL-compatible OID][`oid`] for the materialized view.",
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
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
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
        ("oid", "A [PostgreSQL-compatible OID][`oid`] for the type."),
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
});
pub static MZ_CONTINUAL_TASKS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_continual_tasks",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_CONTINUAL_TASKS_OID,
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
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
});
pub static MZ_NETWORK_POLICIES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_network_policies",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_NETWORK_POLICIES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("owner_id", SqlScalarType::String.nullable(false))
        .with_column(
            "privileges",
            SqlScalarType::Array(Box::new(SqlScalarType::MzAclItem)).nullable(false),
        )
        .with_column("oid", SqlScalarType::Oid.nullable(false))
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
        (
            "oid",
            "A [PostgreSQL-compatible OID][`oid`] for the network policy.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
});
pub static MZ_NETWORK_POLICY_RULES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_network_policy_rules",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_NETWORK_POLICY_RULES_OID,
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
            "The ID the network policy the rule is part of. Corresponds to `mz_network_policy_rules.id`.",
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
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
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
        ("oid", "A [PostgreSQL-compatible OID][`oid`] for the role."),
        ("name", "The name of the role."),
        (
            "inherit",
            "Indicates whether the role has inheritance of privileges.",
        ),
        (
            "rolcanlogin",
            "Indicates whether the role can log in (i.e., is a user).",
        ),
        ("rolsuper", "Indicates whether the role is a superuser."),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
});
pub static MZ_ROLE_MEMBERS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_role_members",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_ROLE_MEMBERS_OID,
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
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
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
        (
            "role_oid",
            "The OID of the role whose configuration parameter default is set. Corresponds to `mz_roles.oid`.",
        ),
        ("password_hash", "The hashed password for the role"),
        (
            "updated_at",
            "The timestamp when the password was last updated.",
        ),
    ]),
    is_retained_metrics_object: false,
    access: vec![rbac::owner_privilege(ObjectType::Table, MZ_SYSTEM_ROLE_ID)],
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
            (
                "oid",
                "A [PostgreSQL-compatible OID][`oid`] for the function.",
            ),
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
            "Unstable If the cluster is managed, `true` if the replicas have the `DISK` option . `NULL` for unmanaged clusters.",
        ),
        (
            "availability_zones",
            "Unstable If the cluster is managed, the list of availability zones specified in `AVAILABILITY ZONES`. `NULL` for unmanaged clusters.",
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
});

pub static MZ_CLUSTER_WORKLOAD_CLASSES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_cluster_workload_classes",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_CLUSTER_WORKLOAD_CLASSES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("workload_class", SqlScalarType::String.nullable(true))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
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
});

pub static MZ_SECRETS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_secrets",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::TABLE_MZ_SECRETS_OID,
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
        (
            "oid",
            "A [PostgreSQL-compatible oid][`oid`] for the secret.",
        ),
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
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
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
});

pub static MZ_INTERNAL_CLUSTER_REPLICAS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_internal_cluster_replicas",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_INTERNAL_CLUSTER_REPLICAS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([(
        "id",
        "The ID of a cluster replica. Corresponds to `mz_cluster_replicas.id`.",
    )]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
});

pub static MZ_PENDING_CLUSTER_REPLICAS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "mz_pending_cluster_replicas",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::TABLE_MZ_PENDING_CLUSTER_REPLICAS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([(
        "id",
        "The ID of a cluster replica. Corresponds to `mz_cluster_replicas.id`.",
    )]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
});

pub static MZ_CLUSTER_REPLICA_STATUS_HISTORY: LazyLock<BuiltinSource> = LazyLock::new(|| {
    BuiltinSource {
        name: "mz_cluster_replica_status_history",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_CLUSTER_REPLICA_STATUS_HISTORY_OID,
        data_source: IntrospectionType::ReplicaStatusHistory,
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
    }
});

pub static MZ_CLUSTER_REPLICA_STATUS_HISTORY_CT: LazyLock<BuiltinContinualTask> = LazyLock::new(
    || {
        BuiltinContinualTask {
            name: "mz_cluster_replica_status_history_ct",
            schema: MZ_INTERNAL_SCHEMA,
            oid: oid::CT_MZ_CLUSTER_REPLICA_STATUS_HISTORY_OID,
            desc: REPLICA_STATUS_HISTORY_DESC.clone(),
            sql: "
IN CLUSTER mz_catalog_server
ON INPUT mz_internal.mz_cluster_replica_status_history AS (
    DELETE FROM mz_internal.mz_cluster_replica_status_history_ct WHERE occurred_at + '30d' < mz_now();
    INSERT INTO mz_internal.mz_cluster_replica_status_history_ct SELECT * FROM mz_internal.mz_cluster_replica_status_history;
)",
            access: vec![PUBLIC_SELECT],
        }
    },
);

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
});

pub static MZ_SOURCE_STATUS_HISTORY: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_source_status_history",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_SOURCE_STATUS_HISTORY_OID,
    data_source: IntrospectionType::SourceStatusHistory,
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
});

pub static MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY: LazyLock<BuiltinSource> = LazyLock::new(
    || BuiltinSource {
        name: "mz_aws_privatelink_connection_status_history",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_AWS_PRIVATELINK_CONNECTION_STATUS_HISTORY_OID,
        data_source: IntrospectionType::PrivatelinkConnectionStatusHistory,
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
    },
);

pub static MZ_AWS_PRIVATELINK_CONNECTION_STATUSES: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
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
            ("status", ""),
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
    });

pub static MZ_STATEMENT_EXECUTION_HISTORY: LazyLock<BuiltinSource> =
    LazyLock::new(|| BuiltinSource {
        name: "mz_statement_execution_history",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_STATEMENT_EXECUTION_HISTORY_OID,
        data_source: IntrospectionType::StatementExecutionHistory,
        desc: MZ_STATEMENT_EXECUTION_HISTORY_DESC.clone(),
        column_comments: BTreeMap::new(),
        is_retained_metrics_object: false,
        access: vec![MONITOR_SELECT],
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
}
});

pub static MZ_PREPARED_STATEMENT_HISTORY: LazyLock<BuiltinSource> =
    LazyLock::new(|| BuiltinSource {
        name: "mz_prepared_statement_history",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_PREPARED_STATEMENT_HISTORY_OID,
        data_source: IntrospectionType::PreparedStatementHistory,
        desc: MZ_PREPARED_STATEMENT_HISTORY_DESC.clone(),
        column_comments: BTreeMap::new(),
        is_retained_metrics_object: false,
        access: vec![
            SUPPORT_SELECT,
            ANALYTICS_SELECT,
            MONITOR_REDACTED_SELECT,
            MONITOR_SELECT,
        ],
    });

pub static MZ_SQL_TEXT: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_sql_text",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_SQL_TEXT_OID,
    desc: MZ_SQL_TEXT_DESC.clone(),
    data_source: IntrospectionType::SqlText,
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: false,
    access: vec![MONITOR_SELECT],
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
    data_source: IntrospectionType::SessionHistory,
    desc: MZ_SESSION_HISTORY_DESC.clone(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
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
            "The type of the statement, e.g. `select` for a `SELECT` query, or `NULL` if the statement was empty.",
        ),
        (
            "throttled_count",
            "The number of statements that were dropped due to throttling before the current one was seen. If you have a very high volume of queries and need to log them without throttling, contact our team.",
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
        .with_column("params", SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false))
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
    mralt.transient_index_id, mralt.params, mralt.mz_version, mralt.began_at, mralt.finished_at,
    mralt.finished_status, mralt.result_size, mralt.rows_returned, mralt.execution_strategy, mralt.transaction_id,
    mralt.prepared_statement_id, mralt.sql_hash, mralt.prepared_statement_name, mralt.session_id,
    mralt.prepared_at, mralt.statement_type, mralt.throttled_count,
    mralt.initial_application_name, mralt.authenticated_user,
    mrst.redacted_sql
FROM mz_internal.mz_recent_activity_log_thinned mralt,
     mz_internal.mz_recent_sql_text mrst
WHERE mralt.sql_hash = mrst.sql_hash",
    access: vec![MONITOR_SELECT, MONITOR_REDACTED_SELECT, SUPPORT_SELECT, ANALYTICS_SELECT],
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
        data_source: IntrospectionType::StatementLifecycleHistory,
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
});

pub static MZ_SINK_STATUS_HISTORY: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_sink_status_history",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_SINK_STATUS_HISTORY_OID,
    data_source: IntrospectionType::SinkStatusHistory,
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
});

pub static MZ_STORAGE_USAGE_BY_SHARD_DESCRIPTION: LazyLock<SystemObjectDescription> =
    LazyLock::new(|| SystemObjectDescription {
        schema_name: MZ_STORAGE_USAGE_BY_SHARD.schema.to_string(),
        object_type: CatalogItemType::Table,
        object_name: MZ_STORAGE_USAGE_BY_SHARD.name.to_string(),
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
});

pub static MZ_CLUSTER_REPLICA_METRICS_HISTORY: LazyLock<BuiltinSource> =
    LazyLock::new(|| BuiltinSource {
        name: "mz_cluster_replica_metrics_history",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_CLUSTER_REPLICA_METRICS_HISTORY_OID,
        data_source: IntrospectionType::ReplicaMetricsHistory,
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
    });

pub static MZ_CLUSTER_REPLICA_METRICS_HISTORY_CT: LazyLock<BuiltinContinualTask> = LazyLock::new(
    || {
        BuiltinContinualTask {
            name: "mz_cluster_replica_metrics_history_ct",
            schema: MZ_INTERNAL_SCHEMA,
            oid: oid::CT_MZ_CLUSTER_REPLICA_METRICS_HISTORY_OID,
            desc: REPLICA_METRICS_HISTORY_DESC.clone(),
            sql: "
IN CLUSTER mz_catalog_server
ON INPUT mz_internal.mz_cluster_replica_metrics_history AS (
    DELETE FROM mz_internal.mz_cluster_replica_metrics_history_ct WHERE occurred_at + '30d' < mz_now();
    INSERT INTO mz_internal.mz_cluster_replica_metrics_history_ct SELECT * FROM mz_internal.mz_cluster_replica_metrics_history;
)",
            access: vec![PUBLIC_SELECT],
        }
    },
);

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
});

pub static MZ_CLUSTER_REPLICA_FRONTIERS: LazyLock<BuiltinSource> =
    LazyLock::new(|| BuiltinSource {
        name: "mz_cluster_replica_frontiers",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::SOURCE_MZ_CLUSTER_REPLICA_FRONTIERS_OID,
        data_source: IntrospectionType::ReplicaFrontiers,
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
    });

pub static MZ_CLUSTER_REPLICA_FRONTIERS_IND: LazyLock<BuiltinIndex> =
    LazyLock::new(|| BuiltinIndex {
        name: "mz_cluster_replica_frontiers_ind",
        schema: MZ_CATALOG_SCHEMA,
        oid: oid::INDEX_MZ_CLUSTER_REPLICA_FRONTIERS_IND_OID,
        sql: "IN CLUSTER mz_catalog_server ON mz_catalog.mz_cluster_replica_frontiers (object_id)",
        is_retained_metrics_object: false,
    });

pub static MZ_FRONTIERS: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_frontiers",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_FRONTIERS_OID,
    data_source: IntrospectionType::Frontiers,
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
});

pub static MZ_WALLCLOCK_LAG_HISTORY: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_wallclock_lag_history",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_WALLCLOCK_LAG_HISTORY_OID,
    desc: WALLCLOCK_LAG_HISTORY_DESC.clone(),
    data_source: IntrospectionType::WallclockLagHistory,
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
});

pub static MZ_WALLCLOCK_LAG_HISTORY_CT: LazyLock<BuiltinContinualTask> = LazyLock::new(|| {
    BuiltinContinualTask {
    name: "mz_wallclock_lag_history_ct",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::CT_MZ_WALLCLOCK_LAG_HISTORY_OID,
    desc: WALLCLOCK_LAG_HISTORY_DESC.clone(),
    sql: "
IN CLUSTER mz_catalog_server
ON INPUT mz_internal.mz_wallclock_lag_history AS (
    DELETE FROM mz_internal.mz_wallclock_lag_history_ct WHERE occurred_at + '30d' < mz_now();
    INSERT INTO mz_internal.mz_wallclock_lag_history_ct SELECT * FROM mz_internal.mz_wallclock_lag_history;
)",
            access: vec![PUBLIC_SELECT],
        }
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
    column_comments: BTreeMap::new(),
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
});

pub static MZ_WALLCLOCK_GLOBAL_LAG_RECENT_HISTORY: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
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
        column_comments: BTreeMap::new(),
        sql: "
SELECT object_id, lag, occurred_at
FROM mz_internal.mz_wallclock_global_lag_history
WHERE occurred_at + '1 day' > mz_now()",
        access: vec![PUBLIC_SELECT],
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
});

pub static MZ_WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RAW: LazyLock<BuiltinSource> =
    LazyLock::new(|| BuiltinSource {
        name: "mz_wallclock_global_lag_histogram_raw",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RAW_OID,
        desc: WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RAW_DESC.clone(),
        column_comments: BTreeMap::new(),
        data_source: IntrospectionType::WallclockLagHistogram,
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
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
    });

pub static MZ_MATERIALIZED_VIEW_REFRESHES: LazyLock<BuiltinSource> = LazyLock::new(|| {
    BuiltinSource {
        name: "mz_materialized_view_refreshes",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::SOURCE_MZ_MATERIALIZED_VIEW_REFRESHES_OID,
        data_source: IntrospectionType::ComputeMaterializedViewRefreshes,
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
});

// These will be replaced with per-replica tables once source/sink multiplexing on
// a single cluster is supported.
pub static MZ_SOURCE_STATISTICS_RAW: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_source_statistics_raw",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_SOURCE_STATISTICS_RAW_OID,
    data_source: IntrospectionType::StorageSourceStatistics,
    desc: MZ_SOURCE_STATISTICS_RAW_DESC.clone(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
});
pub static MZ_SINK_STATISTICS_RAW: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_sink_statistics_raw",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_SINK_STATISTICS_RAW_OID,
    data_source: IntrospectionType::StorageSinkStatistics,
    desc: MZ_SINK_STATISTICS_RAW_DESC.clone(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
});

pub static MZ_STORAGE_SHARDS: LazyLock<BuiltinSource> = LazyLock::new(|| BuiltinSource {
    name: "mz_storage_shards",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::SOURCE_MZ_STORAGE_SHARDS_OID,
    data_source: IntrospectionType::ShardMapping,
    desc: RelationDesc::builder()
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("shard_id", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
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
            ("oid", "A [PostgreSQL-compatible OID][`oid`] for the relation."),
            ("schema_id", "The ID of the schema to which the relation belongs. Corresponds to `mz_schemas.id`."),
            ("name", "The name of the relation."),
            ("type", "The type of the relation: either `table`, `source`, `view`, or `materialized view`."),
            ("owner_id", "The role ID of the owner of the relation. Corresponds to `mz_roles.id`."),
            ("cluster_id", "The ID of the cluster maintaining the source, materialized view, index, or sink. Corresponds to `mz_clusters.id`. `NULL` for other object types."),
            ("privileges", "The privileges belonging to the relation."),
        ]),
        sql: "
      SELECT id, oid, schema_id, name, 'table' AS type, owner_id, NULL::text AS cluster_id, privileges FROM mz_catalog.mz_tables
UNION ALL SELECT id, oid, schema_id, name, 'source', owner_id, cluster_id, privileges FROM mz_catalog.mz_sources
UNION ALL SELECT id, oid, schema_id, name, 'view', owner_id, NULL::text, privileges FROM mz_catalog.mz_views
UNION ALL SELECT id, oid, schema_id, name, 'materialized-view', owner_id, cluster_id, privileges FROM mz_catalog.mz_materialized_views
UNION ALL SELECT id, oid, schema_id, name, 'continual-task', owner_id, cluster_id, privileges FROM mz_internal.mz_continual_tasks",
        access: vec![PUBLIC_SELECT],
    }
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
            ("oid", "A [PostgreSQL-compatible OID][`oid`] for the object."),
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
    }
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
            "The type of the object: one of `table`, `source`, `view`, `materialized view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`.",
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
        ("id", "Materialize's unique catalog item ID for the object."),
        ("global_id", "A global ID for the object."),
    ]),
    is_retained_metrics_object: false,
    access: vec![PUBLIC_SELECT],
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
            "The type of the object: one of `table`, `source`, `view`, `materialized view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`.",
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
            "The type of the object: one of `table`, `source`, `view`, `materialized view`, `sink`, `index`, `connection`, `secret`, `type`, or `function`.",
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
});

pub static MZ_DATAFLOWS_PER_WORKER: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflows_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOWS_PER_WORKER_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(true))
        .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    addrs.address[1] AS id,
    ops.worker_id,
    ops.name
FROM
    mz_introspection.mz_dataflow_addresses_per_worker addrs,
    mz_introspection.mz_dataflow_operators_per_worker ops
WHERE
    addrs.id = ops.id AND
    addrs.worker_id = ops.worker_id AND
    mz_catalog.list_length(addrs.address) = 1",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_DATAFLOWS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflows",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOWS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(true))
        .with_column("name", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the dataflow."),
        ("name", "The internal name of the dataflow."),
    ]),
    sql: "
SELECT id, name
FROM mz_introspection.mz_dataflows_per_worker
WHERE worker_id = 0",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_DATAFLOW_ADDRESSES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_addresses",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_ADDRESSES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column(
            "address",
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::UInt64),
                custom_id: None,
            }
            .nullable(false),
        )
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the channel or operator. Corresponds to `mz_dataflow_channels.id` or `mz_dataflow_operators.id`.",
        ),
        (
            "address",
            "A list of scope-local indexes indicating the path from the root to this channel or operator.",
        ),
    ]),
    sql: "
SELECT id, address
FROM mz_introspection.mz_dataflow_addresses_per_worker
WHERE worker_id = 0",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_DATAFLOW_CHANNELS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_channels",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_CHANNELS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("from_index", SqlScalarType::UInt64.nullable(false))
        .with_column("from_port", SqlScalarType::UInt64.nullable(false))
        .with_column("to_index", SqlScalarType::UInt64.nullable(false))
        .with_column("to_port", SqlScalarType::UInt64.nullable(false))
        .with_column("type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the channel."),
        (
            "from_index",
            "The scope-local index of the source operator. Corresponds to `mz_dataflow_addresses.address`.",
        ),
        ("from_port", "The source operator's output port."),
        (
            "to_index",
            "The scope-local index of the target operator. Corresponds to `mz_dataflow_addresses.address`.",
        ),
        ("to_port", "The target operator's input port."),
        ("type", "The container type of the channel."),
    ]),
    sql: "
SELECT id, from_index, from_port, to_index, to_port, type
FROM mz_introspection.mz_dataflow_channels_per_worker
WHERE worker_id = 0",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_DATAFLOW_OPERATORS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_operators",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_OPERATORS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the operator."),
        ("name", "The internal name of the operator."),
    ]),
    sql: "
SELECT id, name
FROM mz_introspection.mz_dataflow_operators_per_worker
WHERE worker_id = 0",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_DATAFLOW_GLOBAL_IDS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_global_ids",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_GLOBAL_IDS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("global_id", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The dataflow ID."),
        ("global_id", "A global ID associated with that dataflow."),
    ]),
    sql: "
SELECT id, global_id
FROM mz_introspection.mz_compute_dataflow_global_ids_per_worker
WHERE worker_id = 0",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_MAPPABLE_OBJECTS: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
    name: "mz_mappable_objects",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_MAPPABLE_OBJECTS_OID,
    desc: RelationDesc::builder()
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("global_id", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("name", "The name of the object."),
        ("global_id", "The global ID of the object."),
    ]),
    sql: "
SELECT COALESCE(quote_ident(md.name) || '.', '') || quote_ident(ms.name) || '.' || quote_ident(mo.name) AS name, mgi.global_id AS global_id
FROM      mz_catalog.mz_objects mo
          JOIN mz_introspection.mz_compute_exports mce ON (mo.id = mce.export_id)
          JOIN mz_catalog.mz_schemas ms ON (mo.schema_id = ms.id)
          JOIN mz_introspection.mz_dataflow_global_ids mgi ON (mce.dataflow_id = mgi.id)
     LEFT JOIN mz_catalog.mz_databases md ON (ms.database_id = md.id);",
    access: vec![PUBLIC_SELECT],
}
});

pub static MZ_LIR_MAPPING: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_lir_mapping",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_LIR_MAPPING_OID,
    desc: RelationDesc::builder()
        .with_column("global_id", SqlScalarType::String.nullable(false))
        .with_column("lir_id", SqlScalarType::UInt64.nullable(false))
        .with_column("operator", SqlScalarType::String.nullable(false))
        .with_column("parent_lir_id", SqlScalarType::UInt64.nullable(true))
        .with_column("nesting", SqlScalarType::UInt16.nullable(false))
        .with_column("operator_id_start", SqlScalarType::UInt64.nullable(false))
        .with_column("operator_id_end", SqlScalarType::UInt64.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("global_id", "The global ID."),
        ("lir_id", "The LIR node ID."),
        (
            "operator",
            "The LIR operator, in the format `OperatorName INPUTS [OPTIONS]`.",
        ),
        (
            "parent_lir_id",
            "The parent of this LIR node. May be `NULL`.",
        ),
        ("nesting", "The nesting level of this LIR node."),
        (
            "operator_id_start",
            "The first dataflow operator ID implementing this LIR operator (inclusive).",
        ),
        (
            "operator_id_end",
            "The first dataflow operator ID after this LIR operator (exclusive).",
        ),
    ]),
    sql: "
SELECT global_id, lir_id, operator, parent_lir_id, nesting, operator_id_start, operator_id_end
FROM mz_introspection.mz_compute_lir_mapping_per_worker
WHERE worker_id = 0",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_DATAFLOW_OPERATOR_DATAFLOWS_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_dataflow_operator_dataflows_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_DATAFLOW_OPERATOR_DATAFLOWS_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("dataflow_id", SqlScalarType::UInt64.nullable(false))
            .with_column("dataflow_name", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    ops.id,
    ops.name,
    ops.worker_id,
    dfs.id as dataflow_id,
    dfs.name as dataflow_name
FROM
    mz_introspection.mz_dataflow_operators_per_worker ops,
    mz_introspection.mz_dataflow_addresses_per_worker addrs,
    mz_introspection.mz_dataflows_per_worker dfs
WHERE
    ops.id = addrs.id AND
    ops.worker_id = addrs.worker_id AND
    dfs.id = addrs.address[1] AND
    dfs.worker_id = addrs.worker_id",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_DATAFLOW_OPERATOR_DATAFLOWS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_operator_dataflows",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_OPERATOR_DATAFLOWS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("dataflow_id", SqlScalarType::UInt64.nullable(false))
        .with_column("dataflow_name", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the operator. Corresponds to `mz_dataflow_operators.id`.",
        ),
        ("name", "The internal name of the operator."),
        (
            "dataflow_id",
            "The ID of the dataflow hosting the operator. Corresponds to `mz_dataflows.id`.",
        ),
        (
            "dataflow_name",
            "The internal name of the dataflow hosting the operator.",
        ),
    ]),
    sql: "
SELECT id, name, dataflow_id, dataflow_name
FROM mz_introspection.mz_dataflow_operator_dataflows_per_worker
WHERE worker_id = 0",
    access: vec![PUBLIC_SELECT],
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
    }
});

pub static MZ_COMPUTE_EXPORTS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_compute_exports",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_COMPUTE_EXPORTS_OID,
    desc: RelationDesc::builder()
        .with_column("export_id", SqlScalarType::String.nullable(false))
        .with_column("dataflow_id", SqlScalarType::UInt64.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "export_id",
            "The ID of the index, materialized view, or subscription exported by the dataflow. Corresponds to `mz_catalog.mz_indexes.id`, `mz_catalog.mz_materialized_views.id`, or `mz_internal.mz_subscriptions`.",
        ),
        (
            "dataflow_id",
            "The ID of the dataflow. Corresponds to `mz_dataflows.id`.",
        ),
    ]),
    sql: "
SELECT export_id, dataflow_id
FROM mz_introspection.mz_compute_exports_per_worker
WHERE worker_id = 0",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_COMPUTE_FRONTIERS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_compute_frontiers",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_COMPUTE_FRONTIERS_OID,
    desc: RelationDesc::builder()
        .with_column("export_id", SqlScalarType::String.nullable(false))
        .with_column("time", SqlScalarType::MzTimestamp.nullable(false))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "export_id",
            "The ID of the dataflow export. Corresponds to `mz_compute_exports.export_id`.",
        ),
        (
            "time",
            "The next timestamp at which the dataflow output may change.",
        ),
    ]),
    sql: "SELECT
    export_id, pg_catalog.min(time) AS time
FROM mz_introspection.mz_compute_frontiers_per_worker
GROUP BY export_id",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_DATAFLOW_CHANNEL_OPERATORS_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_dataflow_channel_operators_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_DATAFLOW_CHANNEL_OPERATORS_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("from_operator_id", SqlScalarType::UInt64.nullable(true))
            .with_column(
                "from_operator_address",
                SqlScalarType::List {
                    element_type: Box::new(SqlScalarType::UInt64),
                    custom_id: None,
                }
                .nullable(true),
            )
            .with_column("to_operator_id", SqlScalarType::UInt64.nullable(true))
            .with_column(
                "to_operator_address",
                SqlScalarType::List {
                    element_type: Box::new(SqlScalarType::UInt64),
                    custom_id: None,
                }
                .nullable(true),
            )
            .with_column("type", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
WITH
channel_addresses(id, worker_id, address, from_index, to_index, type) AS (
     SELECT id, worker_id, address, from_index, to_index, type
     FROM mz_introspection.mz_dataflow_channels_per_worker mdc
     INNER JOIN mz_introspection.mz_dataflow_addresses_per_worker mda
     USING (id, worker_id)
),
channel_operator_addresses(id, worker_id, from_address, to_address, type) AS (
     SELECT id, worker_id,
            address || from_index AS from_address,
            address || to_index AS to_address,
            type
     FROM channel_addresses
),
operator_addresses(id, worker_id, address) AS (
     SELECT id, worker_id, address
     FROM mz_introspection.mz_dataflow_addresses_per_worker mda
     INNER JOIN mz_introspection.mz_dataflow_operators_per_worker mdo
     USING (id, worker_id)
)
SELECT coa.id,
       coa.worker_id,
       from_ops.id AS from_operator_id,
       coa.from_address AS from_operator_address,
       to_ops.id AS to_operator_id,
       coa.to_address AS to_operator_address,
       coa.type
FROM channel_operator_addresses coa
     LEFT OUTER JOIN operator_addresses from_ops
          ON coa.from_address = from_ops.address AND
             coa.worker_id = from_ops.worker_id
     LEFT OUTER JOIN operator_addresses to_ops
          ON coa.to_address = to_ops.address AND
             coa.worker_id = to_ops.worker_id
",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_DATAFLOW_CHANNEL_OPERATORS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_channel_operators",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_CHANNEL_OPERATORS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("from_operator_id", SqlScalarType::UInt64.nullable(true))
        .with_column(
            "from_operator_address",
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::UInt64),
                custom_id: None,
            }
            .nullable(true),
        )
        .with_column("to_operator_id", SqlScalarType::UInt64.nullable(true))
        .with_column(
            "to_operator_address",
            SqlScalarType::List {
                element_type: Box::new(SqlScalarType::UInt64),
                custom_id: None,
            }
            .nullable(true),
        )
        .with_column("type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the channel. Corresponds to `mz_dataflow_channels.id`.",
        ),
        (
            "from_operator_id",
            "The ID of the source of the channel. Corresponds to `mz_dataflow_operators.id`.",
        ),
        (
            "from_operator_address",
            "The address of the source of the channel. Corresponds to `mz_dataflow_addresses.address`.",
        ),
        (
            "to_operator_id",
            "The ID of the target of the channel. Corresponds to `mz_dataflow_operators.id`.",
        ),
        (
            "to_operator_address",
            "The address of the target of the channel. Corresponds to `mz_dataflow_addresses.address`.",
        ),
        ("type", "The container type of the channel."),
    ]),
    sql: "
SELECT id, from_operator_id, from_operator_address, to_operator_id, to_operator_address, type
FROM mz_introspection.mz_dataflow_channel_operators_per_worker
WHERE worker_id = 0",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_COMPUTE_IMPORT_FRONTIERS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_compute_import_frontiers",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_COMPUTE_IMPORT_FRONTIERS_OID,
    desc: RelationDesc::builder()
        .with_column("export_id", SqlScalarType::String.nullable(false))
        .with_column("import_id", SqlScalarType::String.nullable(false))
        .with_column("time", SqlScalarType::MzTimestamp.nullable(false))
        .with_key(vec![0, 1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "export_id",
            "The ID of the dataflow export. Corresponds to `mz_compute_exports.export_id`.",
        ),
        (
            "import_id",
            "The ID of the dataflow import. Corresponds to `mz_catalog.mz_sources.id` or `mz_catalog.mz_tables.id` or `mz_compute_exports.export_id`.",
        ),
        (
            "time",
            "The next timestamp at which the dataflow input may change.",
        ),
    ]),
    sql: "SELECT
    export_id, import_id, pg_catalog.min(time) AS time
FROM mz_introspection.mz_compute_import_frontiers_per_worker
GROUP BY export_id, import_id",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_RECORDS_PER_DATAFLOW_OPERATOR_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_records_per_dataflow_operator_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_RECORDS_PER_DATAFLOW_OPERATOR_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("dataflow_id", SqlScalarType::UInt64.nullable(false))
            .with_column("records", SqlScalarType::Int64.nullable(true))
            .with_column("batches", SqlScalarType::Int64.nullable(true))
            .with_column("size", SqlScalarType::Int64.nullable(true))
            .with_column("capacity", SqlScalarType::Int64.nullable(true))
            .with_column("allocations", SqlScalarType::Int64.nullable(true))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    dod.id,
    dod.name,
    dod.worker_id,
    dod.dataflow_id,
    ar_size.records AS records,
    ar_size.batches AS batches,
    ar_size.size AS size,
    ar_size.capacity AS capacity,
    ar_size.allocations AS allocations
FROM
    mz_introspection.mz_dataflow_operator_dataflows_per_worker dod
    LEFT OUTER JOIN mz_introspection.mz_arrangement_sizes_per_worker ar_size ON
        dod.id = ar_size.operator_id AND
        dod.worker_id = ar_size.worker_id",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_RECORDS_PER_DATAFLOW_OPERATOR: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_records_per_dataflow_operator",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_RECORDS_PER_DATAFLOW_OPERATOR_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("dataflow_id", SqlScalarType::UInt64.nullable(false))
            .with_column("records", SqlScalarType::Int64.nullable(true))
            .with_column("batches", SqlScalarType::Int64.nullable(true))
            .with_column("size", SqlScalarType::Int64.nullable(true))
            .with_column("capacity", SqlScalarType::Int64.nullable(true))
            .with_column("allocations", SqlScalarType::Int64.nullable(true))
            .with_key(vec![0, 1, 2])
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "id",
                "The ID of the operator. Corresponds to `mz_dataflow_operators.id`.",
            ),
            ("name", "The internal name of the operator."),
            (
                "dataflow_id",
                "The ID of the dataflow. Corresponds to `mz_dataflows.id`.",
            ),
            ("records", "The number of records in the operator."),
            ("batches", "The number of batches in the dataflow."),
            ("size", "The utilized size in bytes of the arrangement."),
            (
                "capacity",
                "The capacity in bytes of the arrangement. Can be larger than the size.",
            ),
            (
                "allocations",
                "The number of separate memory allocations backing the arrangement.",
            ),
        ]),
        sql: "
SELECT
    id,
    name,
    dataflow_id,
    SUM(records)::int8 AS records,
    SUM(batches)::int8 AS batches,
    SUM(size)::int8 AS size,
    SUM(capacity)::int8 AS capacity,
    SUM(allocations)::int8 AS allocations
FROM mz_introspection.mz_records_per_dataflow_operator_per_worker
GROUP BY id, name, dataflow_id",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_RECORDS_PER_DATAFLOW_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_records_per_dataflow_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_RECORDS_PER_DATAFLOW_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("name", SqlScalarType::String.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("records", SqlScalarType::Int64.nullable(true))
            .with_column("batches", SqlScalarType::Int64.nullable(true))
            .with_column("size", SqlScalarType::Int64.nullable(true))
            .with_column("capacity", SqlScalarType::Int64.nullable(true))
            .with_column("allocations", SqlScalarType::Int64.nullable(true))
            .with_key(vec![0, 1, 2])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    rdo.dataflow_id as id,
    dfs.name,
    rdo.worker_id,
    SUM(rdo.records)::int8 as records,
    SUM(rdo.batches)::int8 as batches,
    SUM(rdo.size)::int8 as size,
    SUM(rdo.capacity)::int8 as capacity,
    SUM(rdo.allocations)::int8 as allocations
FROM
    mz_introspection.mz_records_per_dataflow_operator_per_worker rdo,
    mz_introspection.mz_dataflows_per_worker dfs
WHERE
    rdo.dataflow_id = dfs.id AND
    rdo.worker_id = dfs.worker_id
GROUP BY
    rdo.dataflow_id,
    dfs.name,
    rdo.worker_id",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_RECORDS_PER_DATAFLOW: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_records_per_dataflow",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_RECORDS_PER_DATAFLOW_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("records", SqlScalarType::Int64.nullable(true))
        .with_column("batches", SqlScalarType::Int64.nullable(true))
        .with_column("size", SqlScalarType::Int64.nullable(true))
        .with_column("capacity", SqlScalarType::Int64.nullable(true))
        .with_column("allocations", SqlScalarType::Int64.nullable(true))
        .with_key(vec![0, 1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the dataflow. Corresponds to `mz_dataflows.id`.",
        ),
        ("name", "The internal name of the dataflow."),
        ("records", "The number of records in the dataflow."),
        ("batches", "The number of batches in the dataflow."),
        ("size", "The utilized size in bytes of the arrangements."),
        (
            "capacity",
            "The capacity in bytes of the arrangements. Can be larger than the size.",
        ),
        (
            "allocations",
            "The number of separate memory allocations backing the arrangements.",
        ),
    ]),
    sql: "
SELECT
    id,
    name,
    SUM(records)::int8 as records,
    SUM(batches)::int8 as batches,
    SUM(size)::int8 as size,
    SUM(capacity)::int8 as capacity,
    SUM(allocations)::int8 as allocations
FROM
    mz_introspection.mz_records_per_dataflow_per_worker
GROUP BY
    id,
    name",
    access: vec![PUBLIC_SELECT],
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
});

pub const PG_NAMESPACE_ALL_DATABASES_IND: BuiltinIndex = BuiltinIndex {
    name: "pg_namespace_all_databases_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_PG_NAMESPACE_ALL_DATABASES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.pg_namespace_all_databases (nspname)",
    is_retained_metrics_object: false,
};

pub static PG_NAMESPACE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_namespace",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_NAMESPACE_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("nspname", SqlScalarType::String.nullable(false))
        .with_column("nspowner", SqlScalarType::Oid.nullable(false))
        .with_column(
            "nspacl",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(true),
        )
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT
    oid, nspname, nspowner, nspacl
FROM mz_internal.pg_namespace_all_databases
WHERE database_name IS NULL OR database_name = pg_catalog.current_database();",
    access: vec![PUBLIC_SELECT],
});

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
    COALESCE(
        (
            SELECT count(*)::pg_catalog.int2
            FROM mz_catalog.mz_columns
            WHERE mz_columns.id = class_objects.id
        ),
        0::pg_catalog.int2
    ) AS relnatts,
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

pub static PG_CLASS: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
    name: "pg_class",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_CLASS_OID,
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
        .with_column(
            "reloptions",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(true),
        )
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT
    oid, relname, relnamespace, reloftype, relowner, relam, reltablespace, reltuples, reltoastrelid,
    relhasindex, relpersistence, relkind, relnatts, relchecks, relhasrules, relhastriggers, relhassubclass,
    relrowsecurity, relforcerowsecurity, relreplident, relispartition, relhasoids, reloptions
FROM mz_internal.pg_class_all_databases
WHERE database_name IS NULL OR database_name = pg_catalog.current_database();
",
    access: vec![PUBLIC_SELECT],
}
});

pub static PG_DEPEND: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_depend",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_DEPEND_OID,
    desc: RelationDesc::builder()
        .with_column("classid", SqlScalarType::Oid.nullable(true))
        .with_column("objid", SqlScalarType::Oid.nullable(false))
        .with_column("objsubid", SqlScalarType::Int32.nullable(false))
        .with_column("refclassid", SqlScalarType::Oid.nullable(true))
        .with_column("refobjid", SqlScalarType::Oid.nullable(false))
        .with_column("refobjsubid", SqlScalarType::Int32.nullable(false))
        .with_column("deptype", SqlScalarType::PgLegacyChar.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
WITH class_objects AS (
    SELECT
        CASE
            WHEN type = 'table' THEN 'pg_tables'::pg_catalog.regclass::pg_catalog.oid
            WHEN type = 'source' THEN 'pg_tables'::pg_catalog.regclass::pg_catalog.oid
            WHEN type = 'view' THEN 'pg_views'::pg_catalog.regclass::pg_catalog.oid
            WHEN type = 'materialized-view' THEN 'pg_matviews'::pg_catalog.regclass::pg_catalog.oid
        END classid,
        id,
        oid,
        schema_id
    FROM mz_catalog.mz_relations
    UNION ALL
    SELECT
        'pg_index'::pg_catalog.regclass::pg_catalog.oid AS classid,
        i.id,
        i.oid,
        r.schema_id
    FROM mz_catalog.mz_indexes i
    JOIN mz_catalog.mz_relations r ON i.on_id = r.id
),

current_objects AS (
    SELECT class_objects.*
    FROM class_objects
    JOIN mz_catalog.mz_schemas ON mz_schemas.id = class_objects.schema_id
    LEFT JOIN mz_catalog.mz_databases d ON d.id = mz_schemas.database_id
    -- This filter is tricky, as it filters out not just objects outside the
    -- database, but *dependencies* on objects outside this database. It's not
    -- clear that this is the right choice, but because PostgreSQL doesn't
    -- support cross-database references, it's not clear that the other choice
    -- is better.
    WHERE mz_schemas.database_id IS NULL OR d.name = pg_catalog.current_database()
)

SELECT
    objects.classid::pg_catalog.oid,
    objects.oid::pg_catalog.oid AS objid,
    0::pg_catalog.int4 AS objsubid,
    dependents.classid::pg_catalog.oid AS refclassid,
    dependents.oid::pg_catalog.oid AS refobjid,
    0::pg_catalog.int4 AS refobjsubid,
    'n'::pg_catalog.char AS deptype
FROM mz_internal.mz_object_dependencies
JOIN current_objects objects ON object_id = objects.id
JOIN current_objects dependents ON referenced_object_id = dependents.id",
    access: vec![PUBLIC_SELECT],
});

pub static PG_DATABASE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_database",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_DATABASE_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("datname", SqlScalarType::String.nullable(false))
        .with_column("datdba", SqlScalarType::Oid.nullable(false))
        .with_column("encoding", SqlScalarType::Int32.nullable(false))
        .with_column("datistemplate", SqlScalarType::Bool.nullable(false))
        .with_column("datallowconn", SqlScalarType::Bool.nullable(false))
        .with_column("datcollate", SqlScalarType::String.nullable(false))
        .with_column("datctype", SqlScalarType::String.nullable(false))
        .with_column(
            "datacl",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(true),
        )
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    d.oid as oid,
    d.name as datname,
    role_owner.oid as datdba,
    6 as encoding,
    -- Materialize doesn't support database cloning.
    FALSE AS datistemplate,
    TRUE AS datallowconn,
    'C' as datcollate,
    'C' as datctype,
    NULL::pg_catalog.text[] as datacl
FROM mz_catalog.mz_databases d
JOIN mz_catalog.mz_roles role_owner ON role_owner.id = d.owner_id",
    access: vec![PUBLIC_SELECT],
});

pub static PG_INDEX: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "pg_index",
        schema: PG_CATALOG_SCHEMA,
        oid: oid::VIEW_PG_INDEX_OID,
        desc: RelationDesc::builder()
            .with_column("indexrelid", SqlScalarType::Oid.nullable(false))
            .with_column("indrelid", SqlScalarType::Oid.nullable(false))
            .with_column("indnatts", SqlScalarType::Int16.nullable(false))
            .with_column("indisunique", SqlScalarType::Bool.nullable(false))
            .with_column("indisprimary", SqlScalarType::Bool.nullable(false))
            .with_column("indimmediate", SqlScalarType::Bool.nullable(false))
            .with_column("indisclustered", SqlScalarType::Bool.nullable(false))
            .with_column("indisvalid", SqlScalarType::Bool.nullable(false))
            .with_column("indisreplident", SqlScalarType::Bool.nullable(false))
            .with_column("indkey", SqlScalarType::Int2Vector.nullable(false))
            .with_column("indoption", SqlScalarType::Int2Vector.nullable(false))
            .with_column("indexprs", SqlScalarType::String.nullable(true))
            .with_column("indpred", SqlScalarType::String.nullable(true))
            .with_key(vec![0, 1])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    mz_indexes.oid AS indexrelid,
    mz_relations.oid AS indrelid,
    COALESCE(
        (
            SELECT count(*)::pg_catalog.int2
            FROM mz_catalog.mz_columns
            JOIN mz_catalog.mz_relations mri ON mz_columns.id = mri.id
            WHERE mri.oid = mz_catalog.mz_relations.oid
        ),
        0::pg_catalog.int2
    ) AS indnatts,
    -- MZ doesn't support creating unique indexes so indisunique is filled with false
    false::pg_catalog.bool AS indisunique,
    false::pg_catalog.bool AS indisprimary,
    -- MZ doesn't support unique indexes so indimmediate is filled with false
    false::pg_catalog.bool AS indimmediate,
    -- MZ doesn't support CLUSTER so indisclustered is filled with false
    false::pg_catalog.bool AS indisclustered,
    -- MZ never creates invalid indexes so indisvalid is filled with true
    true::pg_catalog.bool AS indisvalid,
    -- MZ doesn't support replication so indisreplident is filled with false
    false::pg_catalog.bool AS indisreplident,
    -- Return zero if the index attribute is not a simple column reference, column position otherwise
    pg_catalog.string_agg(coalesce(mz_index_columns.on_position::int8, 0)::pg_catalog.text, ' ' ORDER BY mz_index_columns.index_position::int8)::pg_catalog.int2vector AS indkey,
    -- MZ doesn't have per-column flags, so returning a 0 for each column in the index
    pg_catalog.string_agg('0', ' ')::pg_catalog.int2vector AS indoption,
    -- Index expressions are returned in MZ format
    CASE pg_catalog.string_agg(mz_index_columns.on_expression, ' ' ORDER BY mz_index_columns.index_position::int8)
    WHEN NULL THEN NULL
    ELSE '{' || pg_catalog.string_agg(mz_index_columns.on_expression, '}, {' ORDER BY mz_index_columns.index_position::int8) || '}'
    END AS indexprs,
    -- MZ doesn't support indexes with predicates
    NULL::pg_catalog.text AS indpred
FROM mz_catalog.mz_indexes
JOIN mz_catalog.mz_relations ON mz_indexes.on_id = mz_relations.id
JOIN mz_catalog.mz_index_columns ON mz_index_columns.index_id = mz_indexes.id
JOIN mz_catalog.mz_schemas ON mz_schemas.id = mz_relations.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = mz_schemas.database_id
WHERE mz_schemas.database_id IS NULL OR d.name = pg_catalog.current_database()
GROUP BY mz_indexes.oid, mz_relations.oid",
        access: vec![PUBLIC_SELECT],
    }
});

pub static PG_INDEXES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_indexes",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_INDEXES_OID,
    desc: RelationDesc::builder()
        .with_column("table_catalog", SqlScalarType::String.nullable(false))
        .with_column("schemaname", SqlScalarType::String.nullable(false))
        .with_column("tablename", SqlScalarType::String.nullable(false))
        .with_column("indexname", SqlScalarType::String.nullable(false))
        .with_column("tablespace", SqlScalarType::String.nullable(true))
        .with_column("indexdef", SqlScalarType::String.nullable(true))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    current_database() as table_catalog,
    s.name AS schemaname,
    r.name AS tablename,
    i.name AS indexname,
    NULL::text AS tablespace,
    -- TODO(jkosh44) Fill in with actual index definition.
    NULL::text AS indexdef
FROM mz_catalog.mz_indexes i
JOIN mz_catalog.mz_relations r ON i.on_id = r.id
JOIN mz_catalog.mz_schemas s ON s.id = r.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE s.database_id IS NULL OR d.name = current_database()",
    access: vec![PUBLIC_SELECT],
});

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

/// Note: Databases, Roles, Clusters, Cluster Replicas, Secrets, and Connections are excluded from
/// this view for Postgres compatibility. Specifically, there is no classoid for these objects,
/// which is required for this view.
pub static PG_DESCRIPTION: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_description",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_DESCRIPTION_OID,
    desc: RelationDesc::builder()
        .with_column("objoid", SqlScalarType::Oid.nullable(false))
        .with_column("classoid", SqlScalarType::Oid.nullable(true))
        .with_column("objsubid", SqlScalarType::Int32.nullable(false))
        .with_column("description", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT
    objoid,
    classoid,
    objsubid,
    description
FROM
    mz_internal.pg_description_all_databases
WHERE
    (oid_database_name IS NULL OR oid_database_name = pg_catalog.current_database()) AND
    (class_database_name IS NULL OR class_database_name = pg_catalog.current_database());",
    access: vec![PUBLIC_SELECT],
});

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

pub static PG_TYPE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_type",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_TYPE_OID,
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
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    oid, typname, typnamespace, typowner, typlen, typtype, typcategory, typdelim, typrelid, typelem,
    typarray, typinput, typreceive, typnotnull, typbasetype, typtypmod, typcollation, typdefault
FROM mz_internal.pg_type_all_databases
WHERE database_name IS NULL OR database_name = pg_catalog.current_database();",
    access: vec![PUBLIC_SELECT],
});

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

pub static PG_ATTRIBUTE: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "pg_attribute",
        schema: PG_CATALOG_SCHEMA,
        oid: oid::VIEW_PG_ATTRIBUTE_OID,
        desc: RelationDesc::builder()
            .with_column("attrelid", SqlScalarType::Oid.nullable(false))
            .with_column("attname", SqlScalarType::String.nullable(false))
            .with_column("atttypid", SqlScalarType::Oid.nullable(false))
            .with_column("attlen", SqlScalarType::Int16.nullable(true))
            .with_column("attnum", SqlScalarType::Int16.nullable(false))
            .with_column("atttypmod", SqlScalarType::Int32.nullable(false))
            .with_column("attnotnull", SqlScalarType::Bool.nullable(false))
            .with_column("atthasdef", SqlScalarType::Bool.nullable(false))
            .with_column("attidentity", SqlScalarType::PgLegacyChar.nullable(false))
            .with_column("attgenerated", SqlScalarType::PgLegacyChar.nullable(false))
            .with_column("attisdropped", SqlScalarType::Bool.nullable(false))
            .with_column("attcollation", SqlScalarType::Oid.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    attrelid, attname, atttypid, attlen, attnum, atttypmod, attnotnull, atthasdef, attidentity,
    attgenerated, attisdropped, attcollation
FROM mz_internal.pg_attribute_all_databases
WHERE
  (database_name IS NULL OR database_name = pg_catalog.current_database()) AND
  (pg_type_database_name IS NULL OR pg_type_database_name = pg_catalog.current_database());",
        // Since this depends on pg_type, its id must be higher due to initialization
        // ordering.
        access: vec![PUBLIC_SELECT],
    }
});

pub static PG_PROC: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_proc",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_PROC_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("proname", SqlScalarType::String.nullable(false))
        .with_column("pronamespace", SqlScalarType::Oid.nullable(false))
        .with_column("proowner", SqlScalarType::Oid.nullable(false))
        .with_column("proargdefaults", SqlScalarType::String.nullable(true))
        .with_column("prorettype", SqlScalarType::Oid.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    mz_functions.oid,
    mz_functions.name AS proname,
    mz_schemas.oid AS pronamespace,
    role_owner.oid AS proowner,
    NULL::pg_catalog.text AS proargdefaults,
    ret_type.oid AS prorettype
FROM mz_catalog.mz_functions
JOIN mz_catalog.mz_schemas ON mz_functions.schema_id = mz_schemas.id
LEFT JOIN mz_catalog.mz_databases d ON d.id = mz_schemas.database_id
JOIN mz_catalog.mz_types AS ret_type ON mz_functions.return_type_id = ret_type.id
JOIN mz_catalog.mz_roles role_owner ON role_owner.id = mz_functions.owner_id
WHERE mz_schemas.database_id IS NULL OR d.name = pg_catalog.current_database()",
    access: vec![PUBLIC_SELECT],
});

pub static PG_OPERATOR: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_operator",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_OPERATOR_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("oprname", SqlScalarType::String.nullable(false))
        .with_column("oprresult", SqlScalarType::Oid.nullable(false))
        .with_column("oprleft", SqlScalarType::Oid.nullable(false))
        .with_column("oprright", SqlScalarType::Oid.nullable(false))
        .with_key(vec![0, 1, 2, 3, 4])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    mz_operators.oid,
    mz_operators.name AS oprname,
    ret_type.oid AS oprresult,
    left_type.oid as oprleft,
    right_type.oid as oprright
FROM mz_catalog.mz_operators
JOIN mz_catalog.mz_types AS ret_type ON mz_operators.return_type_id = ret_type.id
JOIN mz_catalog.mz_types AS left_type ON mz_operators.argument_type_ids[1] = left_type.id
JOIN mz_catalog.mz_types AS right_type ON mz_operators.argument_type_ids[2] = right_type.id
WHERE array_length(mz_operators.argument_type_ids, 1) = 2
UNION SELECT
    mz_operators.oid,
    mz_operators.name AS oprname,
    ret_type.oid AS oprresult,
    0 as oprleft,
    right_type.oid as oprright
FROM mz_catalog.mz_operators
JOIN mz_catalog.mz_types AS ret_type ON mz_operators.return_type_id = ret_type.id
JOIN mz_catalog.mz_types AS right_type ON mz_operators.argument_type_ids[1] = right_type.id
WHERE array_length(mz_operators.argument_type_ids, 1) = 1",
    access: vec![PUBLIC_SELECT],
});

pub static PG_RANGE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_range",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_RANGE_OID,
    desc: RelationDesc::builder()
        .with_column("rngtypid", SqlScalarType::Oid.nullable(false))
        .with_column("rngsubtype", SqlScalarType::Oid.nullable(false))
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    NULL::pg_catalog.oid AS rngtypid,
    NULL::pg_catalog.oid AS rngsubtype
WHERE false",
    access: vec![PUBLIC_SELECT],
});

pub static PG_ENUM: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_enum",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_ENUM_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("enumtypid", SqlScalarType::Oid.nullable(false))
        .with_column("enumsortorder", SqlScalarType::Float32.nullable(false))
        .with_column("enumlabel", SqlScalarType::String.nullable(false))
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    NULL::pg_catalog.oid AS oid,
    NULL::pg_catalog.oid AS enumtypid,
    NULL::pg_catalog.float4 AS enumsortorder,
    NULL::pg_catalog.text AS enumlabel
WHERE false",
    access: vec![PUBLIC_SELECT],
});

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
});

pub const PG_ATTRDEF_ALL_DATABASES_IND: BuiltinIndex = BuiltinIndex {
    name: "pg_attrdef_all_databases_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_PG_ATTRDEF_ALL_DATABASES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.pg_attrdef_all_databases (oid, adrelid, adnum, adbin, adsrc)",
    is_retained_metrics_object: false,
};

pub static PG_ATTRDEF: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_attrdef",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_ATTRDEF_OID,
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
    pg_attrdef_all_databases.oid as oid,
    adrelid,
    adnum,
    adbin,
    adsrc
FROM mz_internal.pg_attrdef_all_databases
    JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database());",
    access: vec![PUBLIC_SELECT],
});

pub static PG_SETTINGS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_settings",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_SETTINGS_OID,
    desc: RelationDesc::builder()
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("setting", SqlScalarType::String.nullable(false))
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    name, setting
FROM (VALUES
    ('max_index_keys'::pg_catalog.text, '1000'::pg_catalog.text)
) AS _ (name, setting)",
    access: vec![PUBLIC_SELECT],
});

pub static PG_AUTH_MEMBERS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_auth_members",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_AUTH_MEMBERS_OID,
    desc: RelationDesc::builder()
        .with_column("roleid", SqlScalarType::Oid.nullable(false))
        .with_column("member", SqlScalarType::Oid.nullable(false))
        .with_column("grantor", SqlScalarType::Oid.nullable(false))
        .with_column("admin_option", SqlScalarType::Bool.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    role.oid AS roleid,
    member.oid AS member,
    grantor.oid AS grantor,
    -- Materialize hasn't implemented admin_option.
    false as admin_option
FROM mz_catalog.mz_role_members membership
JOIN mz_catalog.mz_roles role ON membership.role_id = role.id
JOIN mz_catalog.mz_roles member ON membership.member = member.id
JOIN mz_catalog.mz_roles grantor ON membership.grantor = grantor.id",
    access: vec![PUBLIC_SELECT],
});

pub static PG_EVENT_TRIGGER: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_event_trigger",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_EVENT_TRIGGER_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("evtname", SqlScalarType::String.nullable(false))
        .with_column("evtevent", SqlScalarType::String.nullable(false))
        .with_column("evtowner", SqlScalarType::Oid.nullable(false))
        .with_column("evtfoid", SqlScalarType::Oid.nullable(false))
        .with_column("evtenabled", SqlScalarType::PgLegacyChar.nullable(false))
        .with_column(
            "evttags",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false),
        )
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
        NULL::pg_catalog.oid AS oid,
        NULL::pg_catalog.text AS evtname,
        NULL::pg_catalog.text AS evtevent,
        NULL::pg_catalog.oid AS evtowner,
        NULL::pg_catalog.oid AS evtfoid,
        NULL::pg_catalog.char AS evtenabled,
        NULL::pg_catalog.text[] AS evttags
    WHERE false",
    access: vec![PUBLIC_SELECT],
});

pub static PG_LANGUAGE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_language",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_LANGUAGE_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("lanname", SqlScalarType::String.nullable(false))
        .with_column("lanowner", SqlScalarType::Oid.nullable(false))
        .with_column("lanispl", SqlScalarType::Bool.nullable(false))
        .with_column("lanpltrusted", SqlScalarType::Bool.nullable(false))
        .with_column("lanplcallfoid", SqlScalarType::Oid.nullable(false))
        .with_column("laninline", SqlScalarType::Oid.nullable(false))
        .with_column("lanvalidator", SqlScalarType::Oid.nullable(false))
        .with_column(
            "lanacl",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false),
        )
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
        NULL::pg_catalog.oid  AS oid,
        NULL::pg_catalog.text AS lanname,
        NULL::pg_catalog.oid  AS lanowner,
        NULL::pg_catalog.bool AS lanispl,
        NULL::pg_catalog.bool AS lanpltrusted,
        NULL::pg_catalog.oid  AS lanplcallfoid,
        NULL::pg_catalog.oid  AS laninline,
        NULL::pg_catalog.oid  AS lanvalidator,
        NULL::pg_catalog.text[] AS lanacl
    WHERE false",
    access: vec![PUBLIC_SELECT],
});

pub static PG_SHDESCRIPTION: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_shdescription",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_SHDESCRIPTION_OID,
    desc: RelationDesc::builder()
        .with_column("objoid", SqlScalarType::Oid.nullable(false))
        .with_column("classoid", SqlScalarType::Oid.nullable(false))
        .with_column("description", SqlScalarType::String.nullable(false))
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
        NULL::pg_catalog.oid AS objoid,
        NULL::pg_catalog.oid AS classoid,
        NULL::pg_catalog.text AS description
    WHERE false",
    access: vec![PUBLIC_SELECT],
});

pub static PG_TIMEZONE_ABBREVS: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "pg_timezone_abbrevs",
        schema: PG_CATALOG_SCHEMA,
        oid: oid::VIEW_PG_TIMEZONE_ABBREVS_OID,
        desc: RelationDesc::builder()
            .with_column("abbrev", SqlScalarType::String.nullable(false))
            .with_column("utc_offset", SqlScalarType::Interval.nullable(true))
            .with_column("is_dst", SqlScalarType::Bool.nullable(true))
            .with_key(vec![0])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    abbreviation AS abbrev,
    COALESCE(utc_offset, timezone_offset(timezone_name, now()).base_utc_offset + timezone_offset(timezone_name, now()).dst_offset)
        AS utc_offset,
    COALESCE(dst, timezone_offset(timezone_name, now()).dst_offset <> INTERVAL '0')
        AS is_dst
FROM mz_catalog.mz_timezone_abbreviations",
        access: vec![PUBLIC_SELECT],
    }
});

pub static PG_TIMEZONE_NAMES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_timezone_names",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_TIMEZONE_NAMES_OID,
    desc: RelationDesc::builder()
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("abbrev", SqlScalarType::String.nullable(true))
        .with_column("utc_offset", SqlScalarType::Interval.nullable(true))
        .with_column("is_dst", SqlScalarType::Bool.nullable(true))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    name,
    timezone_offset(name, now()).abbrev AS abbrev,
    timezone_offset(name, now()).base_utc_offset + timezone_offset(name, now()).dst_offset
        AS utc_offset,
    timezone_offset(name, now()).dst_offset <> INTERVAL '0'
        AS is_dst
FROM mz_catalog.mz_timezone_names",
    access: vec![PUBLIC_SELECT],
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
});

pub static MZ_PEEK_DURATIONS_HISTOGRAM_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_peek_durations_histogram_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_PEEK_DURATIONS_HISTOGRAM_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("type", SqlScalarType::String.nullable(false))
            .with_column("duration_ns", SqlScalarType::UInt64.nullable(false))
            .with_column("count", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1, 2])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    worker_id, type, duration_ns, pg_catalog.count(*) AS count
FROM
    mz_introspection.mz_peek_durations_histogram_raw
GROUP BY
    worker_id, type, duration_ns",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_PEEK_DURATIONS_HISTOGRAM: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_peek_durations_histogram",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_PEEK_DURATIONS_HISTOGRAM_OID,
    desc: RelationDesc::builder()
        .with_column("type", SqlScalarType::String.nullable(false))
        .with_column("duration_ns", SqlScalarType::UInt64.nullable(false))
        .with_column(
            "count",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_key(vec![0, 1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("type", "The peek variant: `index` or `persist`."),
        (
            "duration_ns",
            "The upper bound of the bucket in nanoseconds.",
        ),
        (
            "count",
            "The (noncumulative) count of peeks in this bucket.",
        ),
    ]),
    sql: "
SELECT
    type, duration_ns,
    pg_catalog.sum(count) AS count
FROM mz_introspection.mz_peek_durations_histogram_per_worker
GROUP BY type, duration_ns",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_DATAFLOW_SHUTDOWN_DURATIONS_HISTOGRAM_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_dataflow_shutdown_durations_histogram_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_DATAFLOW_SHUTDOWN_DURATIONS_HISTOGRAM_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("duration_ns", SqlScalarType::UInt64.nullable(false))
            .with_column("count", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    worker_id, duration_ns, pg_catalog.count(*) AS count
FROM
    mz_introspection.mz_dataflow_shutdown_durations_histogram_raw
GROUP BY
    worker_id, duration_ns",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_DATAFLOW_SHUTDOWN_DURATIONS_HISTOGRAM: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_dataflow_shutdown_durations_histogram",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_DATAFLOW_SHUTDOWN_DURATIONS_HISTOGRAM_OID,
        desc: RelationDesc::builder()
            .with_column("duration_ns", SqlScalarType::UInt64.nullable(false))
            .with_column(
                "count",
                SqlScalarType::Numeric {
                    max_scale: Some(NumericMaxScale::ZERO),
                }
                .nullable(false),
            )
            .with_key(vec![0])
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "duration_ns",
                "The upper bound of the bucket in nanoseconds.",
            ),
            (
                "count",
                "The (noncumulative) count of dataflows in this bucket.",
            ),
        ]),
        sql: "
SELECT
    duration_ns,
    pg_catalog.sum(count) AS count
FROM mz_introspection.mz_dataflow_shutdown_durations_histogram_per_worker
GROUP BY duration_ns",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_SCHEDULING_ELAPSED_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_scheduling_elapsed_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_SCHEDULING_ELAPSED_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("elapsed_ns", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    id, worker_id, pg_catalog.count(*) AS elapsed_ns
FROM
    mz_introspection.mz_scheduling_elapsed_raw
GROUP BY
    id, worker_id",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_SCHEDULING_ELAPSED: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_scheduling_elapsed",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_SCHEDULING_ELAPSED_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column(
            "elapsed_ns",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the operator. Corresponds to `mz_dataflow_operators.id`.",
        ),
        (
            "elapsed_ns",
            "The total elapsed time spent in the operator in nanoseconds.",
        ),
    ]),
    sql: "
SELECT
    id,
    pg_catalog.sum(elapsed_ns) AS elapsed_ns
FROM mz_introspection.mz_scheduling_elapsed_per_worker
GROUP BY id",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_compute_operator_durations_histogram_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("duration_ns", SqlScalarType::UInt64.nullable(false))
            .with_column("count", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1, 2])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    id, worker_id, duration_ns, pg_catalog.count(*) AS count
FROM
    mz_introspection.mz_compute_operator_durations_histogram_raw
GROUP BY
    id, worker_id, duration_ns",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_compute_operator_durations_histogram",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("duration_ns", SqlScalarType::UInt64.nullable(false))
            .with_column(
                "count",
                SqlScalarType::Numeric {
                    max_scale: Some(NumericMaxScale::ZERO),
                }
                .nullable(false),
            )
            .with_key(vec![0, 1])
            .finish(),
        column_comments: BTreeMap::from_iter([
            (
                "id",
                "The ID of the operator. Corresponds to `mz_dataflow_operators.id`.",
            ),
            (
                "duration_ns",
                "The upper bound of the duration bucket in nanoseconds.",
            ),
            (
                "count",
                "The (noncumulative) count of invocations in the bucket.",
            ),
        ]),
        sql: "
SELECT
    id,
    duration_ns,
    pg_catalog.sum(count) AS count
FROM mz_introspection.mz_compute_operator_durations_histogram_per_worker
GROUP BY id, duration_ns",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_SCHEDULING_PARKS_HISTOGRAM_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_scheduling_parks_histogram_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_SCHEDULING_PARKS_HISTOGRAM_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("slept_for_ns", SqlScalarType::UInt64.nullable(false))
            .with_column("requested_ns", SqlScalarType::UInt64.nullable(false))
            .with_column("count", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1, 2])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    worker_id, slept_for_ns, requested_ns, pg_catalog.count(*) AS count
FROM
    mz_introspection.mz_scheduling_parks_histogram_raw
GROUP BY
    worker_id, slept_for_ns, requested_ns",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_SCHEDULING_PARKS_HISTOGRAM: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_scheduling_parks_histogram",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_SCHEDULING_PARKS_HISTOGRAM_OID,
    desc: RelationDesc::builder()
        .with_column("slept_for_ns", SqlScalarType::UInt64.nullable(false))
        .with_column("requested_ns", SqlScalarType::UInt64.nullable(false))
        .with_column(
            "count",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_key(vec![0, 1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "slept_for_ns",
            "The actual length of the park event in nanoseconds.",
        ),
        (
            "requested_ns",
            "The requested length of the park event in nanoseconds.",
        ),
        (
            "count",
            "The (noncumulative) count of park events in this bucket.",
        ),
    ]),
    sql: "
SELECT
    slept_for_ns,
    requested_ns,
    pg_catalog.sum(count) AS count
FROM mz_introspection.mz_scheduling_parks_histogram_per_worker
GROUP BY slept_for_ns, requested_ns",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_COMPUTE_ERROR_COUNTS_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_compute_error_counts_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_COMPUTE_ERROR_COUNTS_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("export_id", SqlScalarType::String.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("count", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1, 2])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
WITH MUTUALLY RECURSIVE
    -- Indexes that reuse existing indexes rather than maintaining separate dataflows.
    -- For these we don't log error counts separately, so we need to forward the error counts from
    -- their dependencies instead.
    index_reuses(reuse_id text, index_id text) AS (
        SELECT d.object_id, d.dependency_id
        FROM mz_internal.mz_compute_dependencies d
        JOIN mz_introspection.mz_compute_exports e ON (e.export_id = d.object_id)
        WHERE NOT EXISTS (
            SELECT 1 FROM mz_introspection.mz_dataflows
            WHERE id = e.dataflow_id
        )
    ),
    -- Error counts that were directly logged on compute exports.
    direct_errors(export_id text, worker_id uint8, count int8) AS (
        SELECT export_id, worker_id, count
        FROM mz_introspection.mz_compute_error_counts_raw
    ),
    -- Error counts propagated to index reused.
    all_errors(export_id text, worker_id uint8, count int8) AS (
        SELECT * FROM direct_errors
        UNION
        SELECT r.reuse_id, e.worker_id, e.count
        FROM all_errors e
        JOIN index_reuses r ON (r.index_id = e.export_id)
    )
SELECT * FROM all_errors",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_COMPUTE_ERROR_COUNTS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_compute_error_counts",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_COMPUTE_ERROR_COUNTS_OID,
    desc: RelationDesc::builder()
        .with_column("export_id", SqlScalarType::String.nullable(false))
        .with_column(
            "count",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "export_id",
            "The ID of the dataflow export. Corresponds to `mz_compute_exports.export_id`.",
        ),
        (
            "count",
            "The count of errors present in this dataflow export.",
        ),
    ]),
    sql: "
SELECT
    export_id,
    pg_catalog.sum(count) AS count
FROM mz_introspection.mz_compute_error_counts_per_worker
GROUP BY export_id
HAVING pg_catalog.sum(count) != 0",
    access: vec![PUBLIC_SELECT],
});

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
        data_source: IntrospectionType::ComputeErrorCounts,
        column_comments: BTreeMap::new(),
        is_retained_metrics_object: false,
        access: vec![PUBLIC_SELECT],
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
    data_source: IntrospectionType::ComputeHydrationTimes,
    column_comments: BTreeMap::new(),
    is_retained_metrics_object: true,
    access: vec![PUBLIC_SELECT],
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
    ),
    -- Ditto CTs
    complete_cts AS (
        SELECT
            ct.id,
            f.replica_id,
            true AS hydrated,
            NULL::interval AS hydration_time
        FROM mz_internal.mz_continual_tasks ct
        JOIN mz_catalog.mz_cluster_replica_frontiers f ON f.object_id = ct.id
        WHERE f.write_frontier IS NULL
    )
SELECT * FROM dataflows
UNION ALL
SELECT * FROM complete_mvs
UNION ALL
SELECT * FROM complete_cts",
    access: vec![PUBLIC_SELECT],
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
        data_source: IntrospectionType::ComputeOperatorHydrationStatus,
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
    }
});

pub static MZ_MESSAGE_COUNTS_PER_WORKER: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_message_counts_per_worker",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_MESSAGE_COUNTS_PER_WORKER_OID,
    desc: RelationDesc::builder()
        .with_column("channel_id", SqlScalarType::UInt64.nullable(false))
        .with_column("from_worker_id", SqlScalarType::UInt64.nullable(false))
        .with_column("to_worker_id", SqlScalarType::UInt64.nullable(false))
        .with_column("sent", SqlScalarType::Int64.nullable(false))
        .with_column("received", SqlScalarType::Int64.nullable(false))
        .with_column("batch_sent", SqlScalarType::Int64.nullable(false))
        .with_column("batch_received", SqlScalarType::Int64.nullable(false))
        .with_key(vec![0, 1, 2])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
WITH batch_sent_cte AS (
    SELECT
        channel_id,
        from_worker_id,
        to_worker_id,
        pg_catalog.count(*) AS sent
    FROM
        mz_introspection.mz_message_batch_counts_sent_raw
    GROUP BY
        channel_id, from_worker_id, to_worker_id
),
batch_received_cte AS (
    SELECT
        channel_id,
        from_worker_id,
        to_worker_id,
        pg_catalog.count(*) AS received
    FROM
        mz_introspection.mz_message_batch_counts_received_raw
    GROUP BY
        channel_id, from_worker_id, to_worker_id
),
sent_cte AS (
    SELECT
        channel_id,
        from_worker_id,
        to_worker_id,
        pg_catalog.count(*) AS sent
    FROM
        mz_introspection.mz_message_counts_sent_raw
    GROUP BY
        channel_id, from_worker_id, to_worker_id
),
received_cte AS (
    SELECT
        channel_id,
        from_worker_id,
        to_worker_id,
        pg_catalog.count(*) AS received
    FROM
        mz_introspection.mz_message_counts_received_raw
    GROUP BY
        channel_id, from_worker_id, to_worker_id
)
SELECT
    sent_cte.channel_id,
    sent_cte.from_worker_id,
    sent_cte.to_worker_id,
    sent_cte.sent,
    received_cte.received,
    batch_sent_cte.sent AS batch_sent,
    batch_received_cte.received AS batch_received
FROM sent_cte
JOIN received_cte USING (channel_id, from_worker_id, to_worker_id)
JOIN batch_sent_cte USING (channel_id, from_worker_id, to_worker_id)
JOIN batch_received_cte USING (channel_id, from_worker_id, to_worker_id)",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_MESSAGE_COUNTS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_message_counts",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_MESSAGE_COUNTS_OID,
    desc: RelationDesc::builder()
        .with_column("channel_id", SqlScalarType::UInt64.nullable(false))
        .with_column(
            "sent",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_column(
            "received",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_column(
            "batch_sent",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_column(
            "batch_received",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(false),
        )
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "channel_id",
            "The ID of the channel. Corresponds to `mz_dataflow_channels.id`.",
        ),
        ("sent", "The number of messages sent."),
        ("received", "The number of messages received."),
        ("batch_sent", "The number of batches sent."),
        ("batch_received", "The number of batches received."),
    ]),
    sql: "
SELECT
    channel_id,
    pg_catalog.sum(sent) AS sent,
    pg_catalog.sum(received) AS received,
    pg_catalog.sum(batch_sent) AS batch_sent,
    pg_catalog.sum(batch_received) AS batch_received
FROM mz_introspection.mz_message_counts_per_worker
GROUP BY channel_id",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_ACTIVE_PEEKS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_active_peeks",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_ACTIVE_PEEKS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::Uuid.nullable(false))
        .with_column("object_id", SqlScalarType::String.nullable(false))
        .with_column("type", SqlScalarType::String.nullable(false))
        .with_column("time", SqlScalarType::MzTimestamp.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "The ID of the peek request."),
        (
            "object_id",
            "The ID of the collection the peek is targeting. Corresponds to `mz_catalog.mz_indexes.id`, `mz_catalog.mz_materialized_views.id`, `mz_catalog.mz_sources.id`, or `mz_catalog.mz_tables.id`.",
        ),
        (
            "type",
            "The type of the corresponding peek: `index` if targeting an index or temporary dataflow; `persist` for a source, materialized view, or table.",
        ),
        ("time", "The timestamp the peek has requested."),
    ]),
    sql: "
SELECT id, object_id, type, time
FROM mz_introspection.mz_active_peeks_per_worker
WHERE worker_id = 0",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_DATAFLOW_OPERATOR_REACHABILITY_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_dataflow_operator_reachability_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_DATAFLOW_OPERATOR_REACHABILITY_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("port", SqlScalarType::UInt64.nullable(false))
            .with_column("update_type", SqlScalarType::String.nullable(false))
            .with_column("time", SqlScalarType::MzTimestamp.nullable(true))
            .with_column("count", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1, 2, 3, 4])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    addr2.id,
    reachability.worker_id,
    port,
    update_type,
    time,
    pg_catalog.count(*) as count
FROM
    mz_introspection.mz_dataflow_operator_reachability_raw reachability,
    mz_introspection.mz_dataflow_addresses_per_worker addr1,
    mz_introspection.mz_dataflow_addresses_per_worker addr2
WHERE
    addr2.address =
    CASE
        WHEN source = 0 THEN addr1.address
        ELSE addr1.address || reachability.source
    END
    AND addr1.id = reachability.id
    AND addr1.worker_id = reachability.worker_id
    AND addr2.worker_id = reachability.worker_id
GROUP BY addr2.id, reachability.worker_id, port, update_type, time",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_DATAFLOW_OPERATOR_REACHABILITY: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_dataflow_operator_reachability",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_DATAFLOW_OPERATOR_REACHABILITY_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("port", SqlScalarType::UInt64.nullable(false))
            .with_column("update_type", SqlScalarType::String.nullable(false))
            .with_column("time", SqlScalarType::MzTimestamp.nullable(true))
            .with_column(
                "count",
                SqlScalarType::Numeric {
                    max_scale: Some(NumericMaxScale::ZERO),
                }
                .nullable(false),
            )
            .with_key(vec![0, 1, 2, 3])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    id,
    port,
    update_type,
    time,
    pg_catalog.sum(count) as count
FROM mz_introspection.mz_dataflow_operator_reachability_per_worker
GROUP BY id, port, update_type, time",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_ARRANGEMENT_SIZES_PER_WORKER: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "mz_arrangement_sizes_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_ARRANGEMENT_SIZES_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("operator_id", SqlScalarType::UInt64.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("records", SqlScalarType::Int64.nullable(true))
            .with_column("batches", SqlScalarType::Int64.nullable(true))
            .with_column("size", SqlScalarType::Int64.nullable(true))
            .with_column("capacity", SqlScalarType::Int64.nullable(true))
            .with_column("allocations", SqlScalarType::Int64.nullable(true))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
WITH operators_per_worker_cte AS (
    SELECT
        id AS operator_id,
        worker_id
    FROM
        mz_introspection.mz_dataflow_operators_per_worker
),
batches_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS batches
    FROM
        mz_introspection.mz_arrangement_batches_raw
    GROUP BY
        operator_id, worker_id
),
records_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS records
    FROM
        mz_introspection.mz_arrangement_records_raw
    GROUP BY
        operator_id, worker_id
),
heap_size_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS size
    FROM
        mz_introspection.mz_arrangement_heap_size_raw
    GROUP BY
        operator_id, worker_id
),
heap_capacity_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS capacity
    FROM
        mz_introspection.mz_arrangement_heap_capacity_raw
    GROUP BY
        operator_id, worker_id
),
heap_allocations_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS allocations
    FROM
        mz_introspection.mz_arrangement_heap_allocations_raw
    GROUP BY
        operator_id, worker_id
),
batcher_records_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS records
    FROM
        mz_introspection.mz_arrangement_batcher_records_raw
    GROUP BY
        operator_id, worker_id
),
batcher_size_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS size
    FROM
        mz_introspection.mz_arrangement_batcher_size_raw
    GROUP BY
        operator_id, worker_id
),
batcher_capacity_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS capacity
    FROM
        mz_introspection.mz_arrangement_batcher_capacity_raw
    GROUP BY
        operator_id, worker_id
),
batcher_allocations_cte AS (
    SELECT
        operator_id,
        worker_id,
        COUNT(*) AS allocations
    FROM
        mz_introspection.mz_arrangement_batcher_allocations_raw
    GROUP BY
        operator_id, worker_id
),
combined AS (
    SELECT
        opw.operator_id,
        opw.worker_id,
        CASE
            WHEN records_cte.records IS NULL AND batcher_records_cte.records IS NULL THEN NULL
            ELSE COALESCE(records_cte.records, 0) + COALESCE(batcher_records_cte.records, 0)
        END AS records,
        batches_cte.batches AS batches,
        CASE
            WHEN heap_size_cte.size IS NULL AND batcher_size_cte.size IS NULL THEN NULL
            ELSE COALESCE(heap_size_cte.size, 0) + COALESCE(batcher_size_cte.size, 0)
        END AS size,
        CASE
            WHEN heap_capacity_cte.capacity IS NULL AND batcher_capacity_cte.capacity IS NULL THEN NULL
            ELSE COALESCE(heap_capacity_cte.capacity, 0) + COALESCE(batcher_capacity_cte.capacity, 0)
        END AS capacity,
        CASE
            WHEN heap_allocations_cte.allocations IS NULL AND batcher_allocations_cte.allocations IS NULL THEN NULL
            ELSE COALESCE(heap_allocations_cte.allocations, 0) + COALESCE(batcher_allocations_cte.allocations, 0)
        END AS allocations
    FROM
                    operators_per_worker_cte opw
    LEFT OUTER JOIN batches_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN records_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN heap_size_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN heap_capacity_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN heap_allocations_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN batcher_records_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN batcher_size_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN batcher_capacity_cte USING (operator_id, worker_id)
    LEFT OUTER JOIN batcher_allocations_cte USING (operator_id, worker_id)
)
SELECT
    operator_id, worker_id, records, batches, size, capacity, allocations
FROM combined
WHERE
       records     IS NOT NULL
    OR batches     IS NOT NULL
    OR size        IS NOT NULL
    OR capacity    IS NOT NULL
    OR allocations IS NOT NULL
",
        access: vec![PUBLIC_SELECT],
    }
});

pub static MZ_ARRANGEMENT_SIZES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_arrangement_sizes",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_ARRANGEMENT_SIZES_OID,
    desc: RelationDesc::builder()
        .with_column("operator_id", SqlScalarType::UInt64.nullable(false))
        .with_column("records", SqlScalarType::Int64.nullable(true))
        .with_column("batches", SqlScalarType::Int64.nullable(true))
        .with_column("size", SqlScalarType::Int64.nullable(true))
        .with_column("capacity", SqlScalarType::Int64.nullable(true))
        .with_column("allocations", SqlScalarType::Int64.nullable(true))
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "operator_id",
            "The ID of the operator that created the arrangement. Corresponds to `mz_dataflow_operators.id`.",
        ),
        ("records", "The number of records in the arrangement."),
        ("batches", "The number of batches in the arrangement."),
        ("size", "The utilized size in bytes of the arrangement."),
        (
            "capacity",
            "The capacity in bytes of the arrangement. Can be larger than the size.",
        ),
        (
            "allocations",
            "The number of separate memory allocations backing the arrangement.",
        ),
    ]),
    sql: "
SELECT
    operator_id,
    SUM(records)::int8 AS records,
    SUM(batches)::int8 AS batches,
    SUM(size)::int8 AS size,
    SUM(capacity)::int8 AS capacity,
    SUM(allocations)::int8 AS allocations
FROM mz_introspection.mz_arrangement_sizes_per_worker
GROUP BY operator_id",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_ARRANGEMENT_SHARING_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_arrangement_sharing_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_ARRANGEMENT_SHARING_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("operator_id", SqlScalarType::UInt64.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .with_column("count", SqlScalarType::Int64.nullable(false))
            .with_key(vec![0, 1])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    operator_id,
    worker_id,
    pg_catalog.count(*) AS count
FROM mz_introspection.mz_arrangement_sharing_raw
GROUP BY operator_id, worker_id",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_ARRANGEMENT_SHARING: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_arrangement_sharing",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_ARRANGEMENT_SHARING_OID,
    desc: RelationDesc::builder()
        .with_column("operator_id", SqlScalarType::UInt64.nullable(false))
        .with_column("count", SqlScalarType::Int64.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "operator_id",
            "The ID of the operator that created the arrangement. Corresponds to `mz_dataflow_operators.id`.",
        ),
        (
            "count",
            "The number of operators that share the arrangement.",
        ),
    ]),
    sql: "
SELECT operator_id, count
FROM mz_introspection.mz_arrangement_sharing_per_worker
WHERE worker_id = 0",
    access: vec![PUBLIC_SELECT],
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
    });

pub static MZ_DATAFLOW_OPERATOR_PARENTS_PER_WORKER: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "mz_dataflow_operator_parents_per_worker",
        schema: MZ_INTROSPECTION_SCHEMA,
        oid: oid::VIEW_MZ_DATAFLOW_OPERATOR_PARENTS_PER_WORKER_OID,
        desc: RelationDesc::builder()
            .with_column("id", SqlScalarType::UInt64.nullable(false))
            .with_column("parent_id", SqlScalarType::UInt64.nullable(false))
            .with_column("worker_id", SqlScalarType::UInt64.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
WITH operator_addrs AS(
    SELECT
        id, address, worker_id
    FROM mz_introspection.mz_dataflow_addresses_per_worker
        INNER JOIN mz_introspection.mz_dataflow_operators_per_worker
            USING (id, worker_id)
),
parent_addrs AS (
    SELECT
        id,
        address[1:list_length(address) - 1] AS parent_address,
        worker_id
    FROM operator_addrs
)
SELECT pa.id, oa.id AS parent_id, pa.worker_id
FROM parent_addrs AS pa
    INNER JOIN operator_addrs AS oa
        ON pa.parent_address = oa.address
        AND pa.worker_id = oa.worker_id",
        access: vec![PUBLIC_SELECT],
    });

pub static MZ_DATAFLOW_OPERATOR_PARENTS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_operator_parents",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_OPERATOR_PARENTS_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("parent_id", SqlScalarType::UInt64.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the operator. Corresponds to `mz_dataflow_operators.id`.",
        ),
        (
            "parent_id",
            "The ID of the operator's parent operator. Corresponds to `mz_dataflow_operators.id`.",
        ),
    ]),
    sql: "
SELECT id, parent_id
FROM mz_introspection.mz_dataflow_operator_parents_per_worker
WHERE worker_id = 0",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_DATAFLOW_ARRANGEMENT_SIZES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_dataflow_arrangement_sizes",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_DATAFLOW_ARRANGEMENT_SIZES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::UInt64.nullable(false))
        .with_column("name", SqlScalarType::String.nullable(false))
        .with_column("records", SqlScalarType::Int64.nullable(true))
        .with_column("batches", SqlScalarType::Int64.nullable(true))
        .with_column("size", SqlScalarType::Int64.nullable(true))
        .with_column("capacity", SqlScalarType::Int64.nullable(true))
        .with_column("allocations", SqlScalarType::Int64.nullable(true))
        .with_key(vec![0, 1])
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "id",
            "The ID of the [dataflow]. Corresponds to `mz_dataflows.id`.",
        ),
        ("name", "The name of the [dataflow]."),
        (
            "records",
            "The number of records in all arrangements in the dataflow.",
        ),
        (
            "batches",
            "The number of batches in all arrangements in the dataflow.",
        ),
        ("size", "The utilized size in bytes of the arrangements."),
        (
            "capacity",
            "The capacity in bytes of the arrangements. Can be larger than the size.",
        ),
        (
            "allocations",
            "The number of separate memory allocations backing the arrangements.",
        ),
    ]),
    sql: "
SELECT
    mdod.dataflow_id AS id,
    mdod.dataflow_name AS name,
    SUM(mas.records)::int8 AS records,
    SUM(mas.batches)::int8 AS batches,
    SUM(mas.size)::int8 AS size,
    SUM(mas.capacity)::int8 AS capacity,
    SUM(mas.allocations)::int8 AS allocations
FROM mz_introspection.mz_dataflow_operator_dataflows AS mdod
LEFT JOIN mz_introspection.mz_arrangement_sizes AS mas
    ON mdod.id = mas.operator_id
GROUP BY mdod.dataflow_id, mdod.dataflow_name",
    access: vec![PUBLIC_SELECT],
});

pub static MZ_EXPECTED_GROUP_SIZE_ADVICE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_expected_group_size_advice",
    schema: MZ_INTROSPECTION_SCHEMA,
    oid: oid::VIEW_MZ_EXPECTED_GROUP_SIZE_ADVICE_OID,
    desc: RelationDesc::builder()
        .with_column("dataflow_id", SqlScalarType::UInt64.nullable(false))
        .with_column("dataflow_name", SqlScalarType::String.nullable(false))
        .with_column("region_id", SqlScalarType::UInt64.nullable(false))
        .with_column("region_name", SqlScalarType::String.nullable(false))
        .with_column("levels", SqlScalarType::Int64.nullable(false))
        .with_column("to_cut", SqlScalarType::Int64.nullable(false))
        .with_column(
            "savings",
            SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            }
            .nullable(true),
        )
        .with_column("hint", SqlScalarType::Float64.nullable(false))
        .finish(),
    column_comments: BTreeMap::from_iter([
        (
            "dataflow_id",
            "The ID of the [dataflow]. Corresponds to `mz_dataflows.id`.",
        ),
        (
            "dataflow_name",
            "The internal name of the dataflow hosting the min/max aggregation or Top K.",
        ),
        (
            "region_id",
            "The ID of the root operator scope. Corresponds to `mz_dataflow_operators.id`.",
        ),
        (
            "region_name",
            "The internal name of the root operator scope for the min/max aggregation or Top K.",
        ),
        (
            "levels",
            "The number of levels in the hierarchical scheme implemented by the region.",
        ),
        (
            "to_cut",
            "The number of levels that can be eliminated (cut) from the region's hierarchy.",
        ),
        (
            "savings",
            "A conservative estimate of the amount of memory in bytes to be saved by applying the hint.",
        ),
        (
            "hint",
            "The hint value that will eliminate `to_cut` levels from the region's hierarchy.",
        ),
    ]),
    sql: "
        -- The mz_expected_group_size_advice view provides tuning suggestions for the GROUP SIZE
        -- query hints. This tuning hint is effective for min/max/top-k patterns, where a stack
        -- of arrangements must be built. For each dataflow and region corresponding to one
        -- such pattern, we look for how many levels can be eliminated without hitting a level
        -- that actually substantially filters the input. The advice is constructed so that
        -- setting the hint for the affected region will eliminate these redundant levels of
        -- the hierarchical rendering.
        --
        -- A number of helper CTEs are used for the view definition. The first one, operators,
        -- looks for operator names that comprise arrangements of inputs to each level of a
        -- min/max/top-k hierarchy.
        WITH operators AS (
            SELECT
                dod.dataflow_id,
                dor.id AS region_id,
                dod.id,
                ars.records,
                ars.size
            FROM
                mz_introspection.mz_dataflow_operator_dataflows dod
                JOIN mz_introspection.mz_dataflow_addresses doa
                    ON dod.id = doa.id
                JOIN mz_introspection.mz_dataflow_addresses dra
                    ON dra.address = doa.address[:list_length(doa.address) - 1]
                JOIN mz_introspection.mz_dataflow_operators dor
                    ON dor.id = dra.id
                JOIN mz_introspection.mz_arrangement_sizes ars
                    ON ars.operator_id = dod.id
            WHERE
                dod.name = 'Arranged TopK input'
                OR dod.name = 'Arranged MinsMaxesHierarchical input'
                OR dod.name = 'Arrange ReduceMinsMaxes'
            ),
        -- The second CTE, levels, simply computes the heights of the min/max/top-k hierarchies
        -- identified in operators above.
        levels AS (
            SELECT o.dataflow_id, o.region_id, COUNT(*) AS levels
            FROM operators o
            GROUP BY o.dataflow_id, o.region_id
        ),
        -- The third CTE, pivot, determines for each min/max/top-k hierarchy, the first input
        -- operator. This operator is crucially important, as it records the number of records
        -- that was given as input to the gadget as a whole.
        pivot AS (
            SELECT
                o1.dataflow_id,
                o1.region_id,
                o1.id,
                o1.records
            FROM operators o1
            WHERE
                o1.id = (
                    SELECT MIN(o2.id)
                    FROM operators o2
                    WHERE
                        o2.dataflow_id = o1.dataflow_id
                        AND o2.region_id = o1.region_id
                    OPTIONS (AGGREGATE INPUT GROUP SIZE = 8)
                )
        ),
        -- The fourth CTE, candidates, will look for operators where the number of records
        -- maintained is not significantly different from the number at the pivot (excluding
        -- the pivot itself). These are the candidates for being cut from the dataflow region
        -- by adjusting the hint. The query includes a constant, heuristically tuned on TPC-H
        -- load generator data, to give some room for small deviations in number of records.
        -- The intuition for allowing for this deviation is that we are looking for a strongly
        -- reducing point in the hierarchy. To see why one such operator ought to exist in an
        -- untuned hierarchy, consider that at each level, we use hashing to distribute rows
        -- among groups where the min/max/top-k computation is (partially) applied. If the
        -- hierarchy has too many levels, the first-level (pivot) groups will be such that many
        -- groups might be empty or contain only one row. Each subsequent level will have a number
        -- of groups that is reduced exponentially. So at some point, we will find the level where
        -- we actually start having a few rows per group. That's where we will see the row counts
        -- significantly drop off.
        candidates AS (
            SELECT
                o.dataflow_id,
                o.region_id,
                o.id,
                o.records,
                o.size
            FROM
                operators o
                JOIN pivot p
                    ON o.dataflow_id = p.dataflow_id
                        AND o.region_id = p.region_id
                        AND o.id <> p.id
            WHERE o.records >= p.records * (1 - 0.15)
        ),
        -- The fifth CTE, cuts, computes for each relevant dataflow region, the number of
        -- candidate levels that should be cut. We only return here dataflow regions where at
        -- least one level must be cut. Note that once we hit a point where the hierarchy starts
        -- to have a filtering effect, i.e., after the last candidate, it is dangerous to suggest
        -- cutting the height of the hierarchy further. This is because we will have way less
        -- groups in the next level, so there should be even further reduction happening or there
        -- is some substantial skew in the data. But if the latter is the case, then we should not
        -- tune the GROUP SIZE hints down anyway to avoid hurting latency upon updates directed
        -- at these unusually large groups. In addition to selecting the levels to cut, we also
        -- compute a conservative estimate of the memory savings in bytes that will result from
        -- cutting these levels from the hierarchy. The estimate is based on the sizes of the
        -- input arrangements for each level to be cut. These arrangements should dominate the
        -- size of each level that can be cut, since the reduction gadget internal to the level
        -- does not remove much data at these levels.
        cuts AS (
            SELECT c.dataflow_id, c.region_id, COUNT(*) AS to_cut, SUM(c.size) AS savings
            FROM candidates c
            GROUP BY c.dataflow_id, c.region_id
            HAVING COUNT(*) > 0
        )
        -- Finally, we compute the hint suggestion for each dataflow region based on the number of
        -- levels and the number of candidates to be cut. The hint is computed taking into account
        -- the fan-in used in rendering for the hash partitioning and reduction of the groups,
        -- currently equal to 16.
        SELECT
            dod.dataflow_id,
            dod.dataflow_name,
            dod.id AS region_id,
            dod.name AS region_name,
            l.levels,
            c.to_cut,
            c.savings,
            pow(16, l.levels - c.to_cut) - 1 AS hint
        FROM cuts c
            JOIN levels l
                ON c.dataflow_id = l.dataflow_id AND c.region_id = l.region_id
            JOIN mz_introspection.mz_dataflow_operator_dataflows dod
                ON dod.dataflow_id = c.dataflow_id AND dod.id = c.region_id",
    access: vec![PUBLIC_SELECT],
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
    }
});

// NOTE: If you add real data to this implementation, then please update
// the related `pg_` function implementations (like `pg_get_constraintdef`)
pub static PG_CONSTRAINT: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_constraint",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_CONSTRAINT_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("conname", SqlScalarType::String.nullable(false))
        .with_column("connamespace", SqlScalarType::Oid.nullable(false))
        .with_column("contype", SqlScalarType::PgLegacyChar.nullable(false))
        .with_column("condeferrable", SqlScalarType::Bool.nullable(false))
        .with_column("condeferred", SqlScalarType::Bool.nullable(false))
        .with_column("convalidated", SqlScalarType::Bool.nullable(false))
        .with_column("conrelid", SqlScalarType::Oid.nullable(false))
        .with_column("contypid", SqlScalarType::Oid.nullable(false))
        .with_column("conindid", SqlScalarType::Oid.nullable(false))
        .with_column("conparentid", SqlScalarType::Oid.nullable(false))
        .with_column("confrelid", SqlScalarType::Oid.nullable(false))
        .with_column("confupdtype", SqlScalarType::PgLegacyChar.nullable(false))
        .with_column("confdeltype", SqlScalarType::PgLegacyChar.nullable(false))
        .with_column("confmatchtype", SqlScalarType::PgLegacyChar.nullable(false))
        .with_column("conislocal", SqlScalarType::Bool.nullable(false))
        .with_column("coninhcount", SqlScalarType::Int32.nullable(false))
        .with_column("connoinherit", SqlScalarType::Bool.nullable(false))
        .with_column(
            "conkey",
            SqlScalarType::Array(Box::new(SqlScalarType::Int16)).nullable(false),
        )
        .with_column(
            "confkey",
            SqlScalarType::Array(Box::new(SqlScalarType::Int16)).nullable(false),
        )
        .with_column(
            "conpfeqop",
            SqlScalarType::Array(Box::new(SqlScalarType::Oid)).nullable(false),
        )
        .with_column(
            "conppeqop",
            SqlScalarType::Array(Box::new(SqlScalarType::Oid)).nullable(false),
        )
        .with_column(
            "conffeqop",
            SqlScalarType::Array(Box::new(SqlScalarType::Oid)).nullable(false),
        )
        .with_column(
            "conexclop",
            SqlScalarType::Array(Box::new(SqlScalarType::Oid)).nullable(false),
        )
        .with_column("conbin", SqlScalarType::String.nullable(false))
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    NULL::pg_catalog.oid as oid,
    NULL::pg_catalog.text as conname,
    NULL::pg_catalog.oid as connamespace,
    NULL::pg_catalog.\"char\" as contype,
    NULL::pg_catalog.bool as condeferrable,
    NULL::pg_catalog.bool as condeferred,
    NULL::pg_catalog.bool as convalidated,
    NULL::pg_catalog.oid as conrelid,
    NULL::pg_catalog.oid as contypid,
    NULL::pg_catalog.oid as conindid,
    NULL::pg_catalog.oid as conparentid,
    NULL::pg_catalog.oid as confrelid,
    NULL::pg_catalog.\"char\" as confupdtype,
    NULL::pg_catalog.\"char\" as confdeltype,
    NULL::pg_catalog.\"char\" as confmatchtype,
    NULL::pg_catalog.bool as conislocal,
    NULL::pg_catalog.int4 as coninhcount,
    NULL::pg_catalog.bool as connoinherit,
    NULL::pg_catalog.int2[] as conkey,
    NULL::pg_catalog.int2[] as confkey,
    NULL::pg_catalog.oid[] as conpfeqop,
    NULL::pg_catalog.oid[] as conppeqop,
    NULL::pg_catalog.oid[] as conffeqop,
    NULL::pg_catalog.oid[] as conexclop,
    NULL::pg_catalog.text as conbin
WHERE false",
    access: vec![PUBLIC_SELECT],
});

pub static PG_TABLES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_tables",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_TABLES_OID,
    desc: RelationDesc::builder()
        .with_column("schemaname", SqlScalarType::String.nullable(true))
        .with_column("tablename", SqlScalarType::String.nullable(false))
        .with_column("tableowner", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT n.nspname AS schemaname,
    c.relname AS tablename,
    pg_catalog.pg_get_userbyid(c.relowner) AS tableowner
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')",
    access: vec![PUBLIC_SELECT],
});

pub static PG_TABLESPACE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_tablespace",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_TABLESPACE_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("spcname", SqlScalarType::String.nullable(false))
        .with_column("spcowner", SqlScalarType::Oid.nullable(true))
        .with_column(
            "spcacl",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(true),
        )
        .with_column(
            "spcoptions",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(true),
        )
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
    SELECT oid, spcname, spcowner, spcacl, spcoptions
    FROM (
        VALUES (
            --These are the same defaults CockroachDB uses.
            0::pg_catalog.oid,
            'pg_default'::pg_catalog.text,
            NULL::pg_catalog.oid,
            NULL::pg_catalog.text[],
            NULL::pg_catalog.text[]
        )
    ) AS _ (oid, spcname, spcowner, spcacl, spcoptions)
",
    access: vec![PUBLIC_SELECT],
});

pub static PG_ACCESS_METHODS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_am",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_AM_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("amname", SqlScalarType::String.nullable(false))
        .with_column("amhandler", SqlScalarType::RegProc.nullable(false))
        .with_column("amtype", SqlScalarType::PgLegacyChar.nullable(false))
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT NULL::pg_catalog.oid AS oid,
    NULL::pg_catalog.text AS amname,
    NULL::pg_catalog.regproc AS amhandler,
    NULL::pg_catalog.\"char\" AS amtype
WHERE false",
    access: vec![PUBLIC_SELECT],
});

pub static PG_ROLES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_roles",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_ROLES_OID,
    desc: RelationDesc::builder()
        .with_column("rolname", SqlScalarType::String.nullable(false))
        .with_column("rolsuper", SqlScalarType::Bool.nullable(true))
        .with_column("rolinherit", SqlScalarType::Bool.nullable(false))
        .with_column("rolcreaterole", SqlScalarType::Bool.nullable(true))
        .with_column("rolcreatedb", SqlScalarType::Bool.nullable(true))
        .with_column("rolcanlogin", SqlScalarType::Bool.nullable(false))
        .with_column("rolreplication", SqlScalarType::Bool.nullable(false))
        .with_column("rolconnlimit", SqlScalarType::Int32.nullable(false))
        .with_column("rolpassword", SqlScalarType::String.nullable(false))
        .with_column(
            "rolvaliduntil",
            SqlScalarType::TimestampTz { precision: None }.nullable(true),
        )
        .with_column("rolbypassrls", SqlScalarType::Bool.nullable(false))
        .with_column(
            "rolconfig",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(true),
        )
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    rolname,
    rolsuper,
    rolinherit,
    rolcreaterole,
    rolcreatedb,
    COALESCE(rolcanlogin, false) AS rolcanlogin,
    rolreplication,
    rolconnlimit,
    '********' as rolpassword,
    rolvaliduntil,
    rolbypassrls,
    (
        SELECT array_agg(parameter_name || '=' || parameter_value)
        FROM mz_catalog.mz_role_parameters rp
        JOIN mz_catalog.mz_roles r ON r.id = rp.role_id
        WHERE ai.oid = r.oid
    ) AS rolconfig,
    oid
FROM pg_catalog.pg_authid ai",
    access: vec![PUBLIC_SELECT],
});

pub static PG_USER: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_user",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_USER_OID,
    desc: RelationDesc::builder()
        .with_column("usename", SqlScalarType::String.nullable(false))
        .with_column("usesysid", SqlScalarType::Oid.nullable(false))
        .with_column("usecreatedb", SqlScalarType::Bool.nullable(true))
        .with_column("usesuper", SqlScalarType::Bool.nullable(true))
        .with_column("userepl", SqlScalarType::Bool.nullable(false))
        .with_column("usebypassrls", SqlScalarType::Bool.nullable(false))
        .with_column("passwd", SqlScalarType::String.nullable(true))
        .with_column(
            "valuntil",
            SqlScalarType::TimestampTz { precision: None }.nullable(true),
        )
        .with_column(
            "useconfig",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(true),
        )
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT
    rolname as usename,
    ai.oid as usesysid,
    rolcreatedb AS usecreatedb,
    rolsuper AS usesuper,
    rolreplication AS userepl,
    rolbypassrls AS usebypassrls,
    rolpassword as passwd,
    rolvaliduntil as valuntil,
    (
        SELECT array_agg(parameter_name || '=' || parameter_value)
        FROM mz_catalog.mz_role_parameters rp
        JOIN mz_catalog.mz_roles r ON r.id = rp.role_id
        WHERE ai.oid = r.oid
    ) AS useconfig
FROM pg_catalog.pg_authid ai
WHERE rolcanlogin",
    access: vec![PUBLIC_SELECT],
});

pub static PG_VIEWS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_views",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_VIEWS_OID,
    desc: RelationDesc::builder()
        .with_column("schemaname", SqlScalarType::String.nullable(true))
        .with_column("viewname", SqlScalarType::String.nullable(false))
        .with_column("viewowner", SqlScalarType::Oid.nullable(false))
        .with_column("definition", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    s.name AS schemaname,
    v.name AS viewname,
    role_owner.oid AS viewowner,
    v.definition AS definition
FROM mz_catalog.mz_views v
LEFT JOIN mz_catalog.mz_schemas s ON s.id = v.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
JOIN mz_catalog.mz_roles role_owner ON role_owner.id = v.owner_id
WHERE s.database_id IS NULL OR d.name = current_database()",
    access: vec![PUBLIC_SELECT],
});

pub static PG_MATVIEWS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_matviews",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_MATVIEWS_OID,
    desc: RelationDesc::builder()
        .with_column("schemaname", SqlScalarType::String.nullable(true))
        .with_column("matviewname", SqlScalarType::String.nullable(false))
        .with_column("matviewowner", SqlScalarType::Oid.nullable(false))
        .with_column("definition", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    s.name AS schemaname,
    m.name AS matviewname,
    role_owner.oid AS matviewowner,
    m.definition AS definition
FROM mz_catalog.mz_materialized_views m
LEFT JOIN mz_catalog.mz_schemas s ON s.id = m.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
JOIN mz_catalog.mz_roles role_owner ON role_owner.id = m.owner_id
WHERE s.database_id IS NULL OR d.name = current_database()",
    access: vec![PUBLIC_SELECT],
});

pub static INFORMATION_SCHEMA_APPLICABLE_ROLES: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "applicable_roles",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_APPLICABLE_ROLES_OID,
        desc: RelationDesc::builder()
            .with_column("grantee", SqlScalarType::String.nullable(false))
            .with_column("role_name", SqlScalarType::String.nullable(false))
            .with_column("is_grantable", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    member.name AS grantee,
    role.name AS role_name,
    -- ADMIN OPTION isn't implemented.
    'NO' AS is_grantable
FROM mz_catalog.mz_role_members membership
JOIN mz_catalog.mz_roles role ON membership.role_id = role.id
JOIN mz_catalog.mz_roles member ON membership.member = member.id
WHERE mz_catalog.mz_is_superuser() OR pg_has_role(current_role, member.oid, 'USAGE')",
        access: vec![PUBLIC_SELECT],
    });

pub static INFORMATION_SCHEMA_COLUMNS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "columns",
    schema: INFORMATION_SCHEMA,
    oid: oid::VIEW_COLUMNS_OID,
    desc: RelationDesc::builder()
        .with_column("table_catalog", SqlScalarType::String.nullable(false))
        .with_column("table_schema", SqlScalarType::String.nullable(false))
        .with_column("table_name", SqlScalarType::String.nullable(false))
        .with_column("column_name", SqlScalarType::String.nullable(false))
        .with_column("ordinal_position", SqlScalarType::Int64.nullable(false))
        .with_column("column_default", SqlScalarType::String.nullable(true))
        .with_column("is_nullable", SqlScalarType::String.nullable(false))
        .with_column("data_type", SqlScalarType::String.nullable(false))
        .with_column(
            "character_maximum_length",
            SqlScalarType::Int32.nullable(true),
        )
        .with_column("numeric_precision", SqlScalarType::Int32.nullable(true))
        .with_column("numeric_scale", SqlScalarType::Int32.nullable(true))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT
    current_database() as table_catalog,
    s.name AS table_schema,
    o.name AS table_name,
    c.name AS column_name,
    c.position::int8 AS ordinal_position,
    c.default AS column_default,
    CASE WHEN c.nullable THEN 'YES' ELSE 'NO' END AS is_nullable,
    c.type AS data_type,
    NULL::pg_catalog.int4 AS character_maximum_length,
    NULL::pg_catalog.int4 AS numeric_precision,
    NULL::pg_catalog.int4 AS numeric_scale
FROM mz_catalog.mz_columns c
JOIN mz_catalog.mz_objects o ON o.id = c.id
JOIN mz_catalog.mz_schemas s ON s.id = o.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE s.database_id IS NULL OR d.name = current_database()",
    access: vec![PUBLIC_SELECT],
});

pub static INFORMATION_SCHEMA_ENABLED_ROLES: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "enabled_roles",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_ENABLED_ROLES_OID,
        desc: RelationDesc::builder()
            .with_column("role_name", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT name AS role_name
FROM mz_catalog.mz_roles
WHERE mz_catalog.mz_is_superuser() OR pg_has_role(current_role, oid, 'USAGE')",
        access: vec![PUBLIC_SELECT],
    });

pub static INFORMATION_SCHEMA_ROLE_TABLE_GRANTS: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "role_table_grants",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_ROLE_TABLE_GRANTS_OID,
        desc: RelationDesc::builder()
            .with_column("grantor", SqlScalarType::String.nullable(false))
            .with_column("grantee", SqlScalarType::String.nullable(true))
            .with_column("table_catalog", SqlScalarType::String.nullable(true))
            .with_column("table_schema", SqlScalarType::String.nullable(false))
            .with_column("table_name", SqlScalarType::String.nullable(false))
            .with_column("privilege_type", SqlScalarType::String.nullable(true))
            .with_column("is_grantable", SqlScalarType::String.nullable(false))
            .with_column("with_hierarchy", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT grantor, grantee, table_catalog, table_schema, table_name, privilege_type, is_grantable, with_hierarchy
FROM information_schema.table_privileges
WHERE
    grantor IN (SELECT role_name FROM information_schema.enabled_roles)
    OR grantee IN (SELECT role_name FROM information_schema.enabled_roles)",
        access: vec![PUBLIC_SELECT],
    }
});

pub static INFORMATION_SCHEMA_KEY_COLUMN_USAGE: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "key_column_usage",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_KEY_COLUMN_USAGE_OID,
        desc: RelationDesc::builder()
            .with_column("constraint_catalog", SqlScalarType::String.nullable(false))
            .with_column("constraint_schema", SqlScalarType::String.nullable(false))
            .with_column("constraint_name", SqlScalarType::String.nullable(false))
            .with_column("table_catalog", SqlScalarType::String.nullable(false))
            .with_column("table_schema", SqlScalarType::String.nullable(false))
            .with_column("table_name", SqlScalarType::String.nullable(false))
            .with_column("column_name", SqlScalarType::String.nullable(false))
            .with_column("ordinal_position", SqlScalarType::Int32.nullable(false))
            .with_column(
                "position_in_unique_constraint",
                SqlScalarType::Int32.nullable(false),
            )
            .with_key(vec![])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    NULL::text AS constraint_catalog,
    NULL::text AS constraint_schema,
    NULL::text AS constraint_name,
    NULL::text AS table_catalog,
    NULL::text AS table_schema,
    NULL::text AS table_name,
    NULL::text AS column_name,
    NULL::integer AS ordinal_position,
    NULL::integer AS position_in_unique_constraint
WHERE false",
        access: vec![PUBLIC_SELECT],
    });

pub static INFORMATION_SCHEMA_REFERENTIAL_CONSTRAINTS: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "referential_constraints",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_REFERENTIAL_CONSTRAINTS_OID,
        desc: RelationDesc::builder()
            .with_column("constraint_catalog", SqlScalarType::String.nullable(false))
            .with_column("constraint_schema", SqlScalarType::String.nullable(false))
            .with_column("constraint_name", SqlScalarType::String.nullable(false))
            .with_column(
                "unique_constraint_catalog",
                SqlScalarType::String.nullable(false),
            )
            .with_column(
                "unique_constraint_schema",
                SqlScalarType::String.nullable(false),
            )
            .with_column(
                "unique_constraint_name",
                SqlScalarType::String.nullable(false),
            )
            .with_column("match_option", SqlScalarType::String.nullable(false))
            .with_column("update_rule", SqlScalarType::String.nullable(false))
            .with_column("delete_rule", SqlScalarType::String.nullable(false))
            .with_key(vec![])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    NULL::text AS constraint_catalog,
    NULL::text AS constraint_schema,
    NULL::text AS constraint_name,
    NULL::text AS unique_constraint_catalog,
    NULL::text AS unique_constraint_schema,
    NULL::text AS unique_constraint_name,
    NULL::text AS match_option,
    NULL::text AS update_rule,
    NULL::text AS delete_rule
WHERE false",
        access: vec![PUBLIC_SELECT],
    });

pub static INFORMATION_SCHEMA_ROUTINES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "routines",
    schema: INFORMATION_SCHEMA,
    oid: oid::VIEW_ROUTINES_OID,
    desc: RelationDesc::builder()
        .with_column("routine_catalog", SqlScalarType::String.nullable(false))
        .with_column("routine_schema", SqlScalarType::String.nullable(false))
        .with_column("routine_name", SqlScalarType::String.nullable(false))
        .with_column("routine_type", SqlScalarType::String.nullable(false))
        .with_column("routine_definition", SqlScalarType::String.nullable(true))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    current_database() as routine_catalog,
    s.name AS routine_schema,
    f.name AS routine_name,
    'FUNCTION' AS routine_type,
    NULL::text AS routine_definition
FROM mz_catalog.mz_functions f
JOIN mz_catalog.mz_schemas s ON s.id = f.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE s.database_id IS NULL OR d.name = current_database()",
    access: vec![PUBLIC_SELECT],
});

pub static INFORMATION_SCHEMA_SCHEMATA: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "schemata",
    schema: INFORMATION_SCHEMA,
    oid: oid::VIEW_SCHEMATA_OID,
    desc: RelationDesc::builder()
        .with_column("catalog_name", SqlScalarType::String.nullable(false))
        .with_column("schema_name", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT
    current_database() as catalog_name,
    s.name AS schema_name
FROM mz_catalog.mz_schemas s
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE s.database_id IS NULL OR d.name = current_database()",
    access: vec![PUBLIC_SELECT],
});

pub static INFORMATION_SCHEMA_TABLES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "tables",
    schema: INFORMATION_SCHEMA,
    oid: oid::VIEW_TABLES_OID,
    desc: RelationDesc::builder()
        .with_column("table_catalog", SqlScalarType::String.nullable(false))
        .with_column("table_schema", SqlScalarType::String.nullable(false))
        .with_column("table_name", SqlScalarType::String.nullable(false))
        .with_column("table_type", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    current_database() as table_catalog,
    s.name AS table_schema,
    r.name AS table_name,
    CASE r.type
        WHEN 'materialized-view' THEN 'MATERIALIZED VIEW'
        WHEN 'table' THEN 'BASE TABLE'
        ELSE pg_catalog.upper(r.type)
    END AS table_type
FROM mz_catalog.mz_relations r
JOIN mz_catalog.mz_schemas s ON s.id = r.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE s.database_id IS NULL OR d.name = current_database()",
    access: vec![PUBLIC_SELECT],
});

pub static INFORMATION_SCHEMA_TABLE_CONSTRAINTS: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "table_constraints",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_TABLE_CONSTRAINTS_OID,
        desc: RelationDesc::builder()
            .with_column("constraint_catalog", SqlScalarType::String.nullable(false))
            .with_column("constraint_schema", SqlScalarType::String.nullable(false))
            .with_column("constraint_name", SqlScalarType::String.nullable(false))
            .with_column("table_catalog", SqlScalarType::String.nullable(false))
            .with_column("table_schema", SqlScalarType::String.nullable(false))
            .with_column("table_name", SqlScalarType::String.nullable(false))
            .with_column("constraint_type", SqlScalarType::String.nullable(false))
            .with_column("is_deferrable", SqlScalarType::String.nullable(false))
            .with_column("initially_deferred", SqlScalarType::String.nullable(false))
            .with_column("enforced", SqlScalarType::String.nullable(false))
            .with_column("nulls_distinct", SqlScalarType::String.nullable(false))
            .with_key(vec![])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    NULL::text AS constraint_catalog,
    NULL::text AS constraint_schema,
    NULL::text AS constraint_name,
    NULL::text AS table_catalog,
    NULL::text AS table_schema,
    NULL::text AS table_name,
    NULL::text AS constraint_type,
    NULL::text AS is_deferrable,
    NULL::text AS initially_deferred,
    NULL::text AS enforced,
    NULL::text AS nulls_distinct
WHERE false",
        access: vec![PUBLIC_SELECT],
    });

pub static INFORMATION_SCHEMA_TABLE_PRIVILEGES: LazyLock<BuiltinView> = LazyLock::new(|| {
    BuiltinView {
        name: "table_privileges",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_TABLE_PRIVILEGES_OID,
        desc: RelationDesc::builder()
            .with_column("grantor", SqlScalarType::String.nullable(false))
            .with_column("grantee", SqlScalarType::String.nullable(true))
            .with_column("table_catalog", SqlScalarType::String.nullable(true))
            .with_column("table_schema", SqlScalarType::String.nullable(false))
            .with_column("table_name", SqlScalarType::String.nullable(false))
            .with_column("privilege_type", SqlScalarType::String.nullable(true))
            .with_column("is_grantable", SqlScalarType::String.nullable(false))
            .with_column("with_hierarchy", SqlScalarType::String.nullable(false))
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "
SELECT
    grantor,
    grantee,
    table_catalog,
    table_schema,
    table_name,
    privilege_type,
    is_grantable,
    CASE privilege_type
        WHEN 'SELECT' THEN 'YES'
        ELSE 'NO'
    END AS with_hierarchy
FROM
    (SELECT
        grantor.name AS grantor,
        CASE mz_internal.mz_aclitem_grantee(privileges)
            WHEN 'p' THEN 'PUBLIC'
            ELSE grantee.name
        END AS grantee,
        table_catalog,
        table_schema,
        table_name,
        unnest(mz_internal.mz_format_privileges(mz_internal.mz_aclitem_privileges(privileges))) AS privilege_type,
        -- ADMIN OPTION isn't implemented.
        'NO' AS is_grantable
    FROM
        (SELECT
            unnest(relations.privileges) AS privileges,
            CASE
                WHEN schemas.database_id IS NULL THEN current_database()
                ELSE databases.name
            END AS table_catalog,
            schemas.name AS table_schema,
            relations.name AS table_name
        FROM mz_catalog.mz_relations AS relations
        JOIN mz_catalog.mz_schemas AS schemas ON relations.schema_id = schemas.id
        LEFT JOIN mz_catalog.mz_databases AS databases ON schemas.database_id = databases.id
        WHERE schemas.database_id IS NULL OR databases.name = current_database())
    JOIN mz_catalog.mz_roles AS grantor ON mz_internal.mz_aclitem_grantor(privileges) = grantor.id
    LEFT JOIN mz_catalog.mz_roles AS grantee ON mz_internal.mz_aclitem_grantee(privileges) = grantee.id)
WHERE
    -- WHERE clause is not guaranteed to short-circuit and 'PUBLIC' will cause an error when passed
    -- to pg_has_role. Therefore we need to use a CASE statement.
    CASE
        WHEN grantee = 'PUBLIC' THEN true
        ELSE mz_catalog.mz_is_superuser()
            OR pg_has_role(current_role, grantee, 'USAGE')
            OR pg_has_role(current_role, grantor, 'USAGE')
    END",
        access: vec![PUBLIC_SELECT],
    }
});

pub static INFORMATION_SCHEMA_TRIGGERS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "triggers",
    schema: INFORMATION_SCHEMA,
    oid: oid::VIEW_TRIGGERS_OID,
    desc: RelationDesc::builder()
        .with_column("trigger_catalog", SqlScalarType::String.nullable(false))
        .with_column("trigger_schema", SqlScalarType::String.nullable(false))
        .with_column("trigger_name", SqlScalarType::String.nullable(false))
        .with_column("event_manipulation", SqlScalarType::String.nullable(false))
        .with_column(
            "event_object_catalog",
            SqlScalarType::String.nullable(false),
        )
        .with_column("event_object_schema", SqlScalarType::String.nullable(false))
        .with_column("event_object_table", SqlScalarType::String.nullable(false))
        .with_column("action_order", SqlScalarType::Int32.nullable(false))
        .with_column("action_condition", SqlScalarType::String.nullable(false))
        .with_column("action_statement", SqlScalarType::String.nullable(false))
        .with_column("action_orientation", SqlScalarType::String.nullable(false))
        .with_column("action_timing", SqlScalarType::String.nullable(false))
        .with_column(
            "action_reference_old_table",
            SqlScalarType::String.nullable(false),
        )
        .with_column(
            "action_reference_new_table",
            SqlScalarType::String.nullable(false),
        )
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    NULL::text as trigger_catalog,
    NULL::text AS trigger_schema,
    NULL::text AS trigger_name,
    NULL::text AS event_manipulation,
    NULL::text AS event_object_catalog,
    NULL::text AS event_object_schema,
    NULL::text AS event_object_table,
    NULL::integer AS action_order,
    NULL::text AS action_condition,
    NULL::text AS action_statement,
    NULL::text AS action_orientation,
    NULL::text AS action_timing,
    NULL::text AS action_reference_old_table,
    NULL::text AS action_reference_new_table
WHERE FALSE",
    access: vec![PUBLIC_SELECT],
});

pub static INFORMATION_SCHEMA_VIEWS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "views",
    schema: INFORMATION_SCHEMA,
    oid: oid::VIEW_VIEWS_OID,
    desc: RelationDesc::builder()
        .with_column("table_catalog", SqlScalarType::String.nullable(false))
        .with_column("table_schema", SqlScalarType::String.nullable(false))
        .with_column("table_name", SqlScalarType::String.nullable(false))
        .with_column("view_definition", SqlScalarType::String.nullable(false))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    current_database() as table_catalog,
    s.name AS table_schema,
    v.name AS table_name,
    v.definition AS view_definition
FROM mz_catalog.mz_views v
JOIN mz_catalog.mz_schemas s ON s.id = v.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE s.database_id IS NULL OR d.name = current_database()",
    access: vec![PUBLIC_SELECT],
});

pub static INFORMATION_SCHEMA_CHARACTER_SETS: LazyLock<BuiltinView> =
    LazyLock::new(|| BuiltinView {
        name: "character_sets",
        schema: INFORMATION_SCHEMA,
        oid: oid::VIEW_CHARACTER_SETS_OID,
        desc: RelationDesc::builder()
            .with_column(
                "character_set_catalog",
                SqlScalarType::String.nullable(true),
            )
            .with_column("character_set_schema", SqlScalarType::String.nullable(true))
            .with_column("character_set_name", SqlScalarType::String.nullable(false))
            .with_column(
                "character_repertoire",
                SqlScalarType::String.nullable(false),
            )
            .with_column("form_of_use", SqlScalarType::String.nullable(false))
            .with_column(
                "default_collate_catalog",
                SqlScalarType::String.nullable(false),
            )
            .with_column(
                "default_collate_schema",
                SqlScalarType::String.nullable(false),
            )
            .with_column(
                "default_collate_name",
                SqlScalarType::String.nullable(false),
            )
            .with_key(vec![])
            .finish(),
        column_comments: BTreeMap::new(),
        sql: "SELECT
    NULL as character_set_catalog,
    NULL as character_set_schema,
    'UTF8' as character_set_name,
    'UCS' as character_repertoire,
    'UTF8' as form_of_use,
    current_database() as default_collate_catalog,
    'pg_catalog' as default_collate_schema,
    'en_US.utf8' as default_collate_name",
        access: vec![PUBLIC_SELECT],
    });

// MZ doesn't support COLLATE so the table is filled with NULLs and made empty. pg_database hard
// codes a collation of 'C' for every database, so we could copy that here.
pub static PG_COLLATION: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_collation",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_COLLATION_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("collname", SqlScalarType::String.nullable(false))
        .with_column("collnamespace", SqlScalarType::Oid.nullable(false))
        .with_column("collowner", SqlScalarType::Oid.nullable(false))
        .with_column("collprovider", SqlScalarType::PgLegacyChar.nullable(false))
        .with_column("collisdeterministic", SqlScalarType::Bool.nullable(false))
        .with_column("collencoding", SqlScalarType::Int32.nullable(false))
        .with_column("collcollate", SqlScalarType::String.nullable(false))
        .with_column("collctype", SqlScalarType::String.nullable(false))
        .with_column("collversion", SqlScalarType::String.nullable(false))
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT
    NULL::pg_catalog.oid AS oid,
    NULL::pg_catalog.text AS collname,
    NULL::pg_catalog.oid AS collnamespace,
    NULL::pg_catalog.oid AS collowner,
    NULL::pg_catalog.\"char\" AS collprovider,
    NULL::pg_catalog.bool AS collisdeterministic,
    NULL::pg_catalog.int4 AS collencoding,
    NULL::pg_catalog.text AS collcollate,
    NULL::pg_catalog.text AS collctype,
    NULL::pg_catalog.text AS collversion
WHERE false",
    access: vec![PUBLIC_SELECT],
});

// MZ doesn't support row level security policies so the table is filled in with NULLs and made empty.
pub static PG_POLICY: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_policy",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_POLICY_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("polname", SqlScalarType::String.nullable(false))
        .with_column("polrelid", SqlScalarType::Oid.nullable(false))
        .with_column("polcmd", SqlScalarType::PgLegacyChar.nullable(false))
        .with_column("polpermissive", SqlScalarType::Bool.nullable(false))
        .with_column(
            "polroles",
            SqlScalarType::Array(Box::new(SqlScalarType::Oid)).nullable(false),
        )
        .with_column("polqual", SqlScalarType::String.nullable(false))
        .with_column("polwithcheck", SqlScalarType::String.nullable(false))
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT
    NULL::pg_catalog.oid AS oid,
    NULL::pg_catalog.text AS polname,
    NULL::pg_catalog.oid AS polrelid,
    NULL::pg_catalog.\"char\" AS polcmd,
    NULL::pg_catalog.bool AS polpermissive,
    NULL::pg_catalog.oid[] AS polroles,
    NULL::pg_catalog.text AS polqual,
    NULL::pg_catalog.text AS polwithcheck
WHERE false",
    access: vec![PUBLIC_SELECT],
});

// MZ doesn't support table inheritance so the table is filled in with NULLs and made empty.
pub static PG_INHERITS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_inherits",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_INHERITS_OID,
    desc: RelationDesc::builder()
        .with_column("inhrelid", SqlScalarType::Oid.nullable(false))
        .with_column("inhparent", SqlScalarType::Oid.nullable(false))
        .with_column("inhseqno", SqlScalarType::Int32.nullable(false))
        .with_column("inhdetachpending", SqlScalarType::Bool.nullable(false))
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT
    NULL::pg_catalog.oid AS inhrelid,
    NULL::pg_catalog.oid AS inhparent,
    NULL::pg_catalog.int4 AS inhseqno,
    NULL::pg_catalog.bool AS inhdetachpending
WHERE false",
    access: vec![PUBLIC_SELECT],
});

pub static PG_LOCKS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_locks",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_LOCKS_OID,
    desc: RelationDesc::builder()
        .with_column("locktype", SqlScalarType::String.nullable(false))
        .with_column("database", SqlScalarType::Oid.nullable(false))
        .with_column("relation", SqlScalarType::Oid.nullable(false))
        .with_column("page", SqlScalarType::Int32.nullable(false))
        .with_column("tuple", SqlScalarType::Int16.nullable(false))
        .with_column("virtualxid", SqlScalarType::String.nullable(false))
        .with_column("transactionid", SqlScalarType::String.nullable(false))
        .with_column("classid", SqlScalarType::Oid.nullable(false))
        .with_column("objid", SqlScalarType::Oid.nullable(false))
        .with_column("objsubid", SqlScalarType::Int16.nullable(false))
        .with_column("virtualtransaction", SqlScalarType::String.nullable(false))
        .with_column("pid", SqlScalarType::Int32.nullable(false))
        .with_column("mode", SqlScalarType::String.nullable(false))
        .with_column("granted", SqlScalarType::Bool.nullable(false))
        .with_column("fastpath", SqlScalarType::Bool.nullable(false))
        .with_column(
            "waitstart",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
SELECT
-- While there exist locks in Materialize, we don't expose them, so all of these fields are NULL.
    NULL::pg_catalog.text AS locktype,
    NULL::pg_catalog.oid AS database,
    NULL::pg_catalog.oid AS relation,
    NULL::pg_catalog.int4 AS page,
    NULL::pg_catalog.int2 AS tuple,
    NULL::pg_catalog.text AS virtualxid,
    NULL::pg_catalog.text AS transactionid,
    NULL::pg_catalog.oid AS classid,
    NULL::pg_catalog.oid AS objid,
    NULL::pg_catalog.int2 AS objsubid,
    NULL::pg_catalog.text AS virtualtransaction,
    NULL::pg_catalog.int4 AS pid,
    NULL::pg_catalog.text AS mode,
    NULL::pg_catalog.bool AS granted,
    NULL::pg_catalog.bool AS fastpath,
    NULL::pg_catalog.timestamptz AS waitstart
WHERE false",
    access: vec![PUBLIC_SELECT],
});

pub static PG_AUTHID: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_authid",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_AUTHID_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("rolname", SqlScalarType::String.nullable(false))
        .with_column("rolsuper", SqlScalarType::Bool.nullable(true))
        .with_column("rolinherit", SqlScalarType::Bool.nullable(false))
        .with_column("rolcreaterole", SqlScalarType::Bool.nullable(true))
        .with_column("rolcreatedb", SqlScalarType::Bool.nullable(true))
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
    mz_catalog.has_system_privilege(r.oid, 'CREATEROLE') AS rolcreaterole,
    mz_catalog.has_system_privilege(r.oid, 'CREATEDB') AS rolcreatedb,
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
});

pub static PG_AGGREGATE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_aggregate",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_AGGREGATE_OID,
    desc: RelationDesc::builder()
        .with_column("aggfnoid", SqlScalarType::Oid.nullable(false))
        .with_column("aggkind", SqlScalarType::String.nullable(false))
        .with_column("aggnumdirectargs", SqlScalarType::Int16.nullable(false))
        .with_column("aggtransfn", SqlScalarType::RegProc.nullable(true))
        .with_column("aggfinalfn", SqlScalarType::RegProc.nullable(false))
        .with_column("aggcombinefn", SqlScalarType::RegProc.nullable(false))
        .with_column("aggserialfn", SqlScalarType::RegProc.nullable(false))
        .with_column("aggdeserialfn", SqlScalarType::RegProc.nullable(false))
        .with_column("aggmtransfn", SqlScalarType::RegProc.nullable(false))
        .with_column("aggminvtransfn", SqlScalarType::RegProc.nullable(false))
        .with_column("aggmfinalfn", SqlScalarType::RegProc.nullable(false))
        .with_column("aggfinalextra", SqlScalarType::Bool.nullable(false))
        .with_column("aggmfinalextra", SqlScalarType::Bool.nullable(false))
        .with_column("aggfinalmodify", SqlScalarType::PgLegacyChar.nullable(true))
        .with_column(
            "aggmfinalmodify",
            SqlScalarType::PgLegacyChar.nullable(true),
        )
        .with_column("aggsortop", SqlScalarType::Oid.nullable(false))
        .with_column("aggtranstype", SqlScalarType::Oid.nullable(true))
        .with_column("aggtransspace", SqlScalarType::Int32.nullable(true))
        .with_column("aggmtranstype", SqlScalarType::Oid.nullable(false))
        .with_column("aggmtransspace", SqlScalarType::Int32.nullable(true))
        .with_column("agginitval", SqlScalarType::String.nullable(true))
        .with_column("aggminitval", SqlScalarType::String.nullable(true))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    a.oid as aggfnoid,
    -- Currently Materialize only support 'normal' aggregate functions.
    a.agg_kind as aggkind,
    a.agg_num_direct_args as aggnumdirectargs,
    -- Materialize doesn't support these fields.
    NULL::pg_catalog.regproc as aggtransfn,
    '0'::pg_catalog.regproc as aggfinalfn,
    '0'::pg_catalog.regproc as aggcombinefn,
    '0'::pg_catalog.regproc as aggserialfn,
    '0'::pg_catalog.regproc as aggdeserialfn,
    '0'::pg_catalog.regproc as aggmtransfn,
    '0'::pg_catalog.regproc as aggminvtransfn,
    '0'::pg_catalog.regproc as aggmfinalfn,
    false as aggfinalextra,
    false as aggmfinalextra,
    NULL::pg_catalog.\"char\" AS aggfinalmodify,
    NULL::pg_catalog.\"char\" AS aggmfinalmodify,
    '0'::pg_catalog.oid as aggsortop,
    NULL::pg_catalog.oid as aggtranstype,
    NULL::pg_catalog.int4 as aggtransspace,
    '0'::pg_catalog.oid as aggmtranstype,
    NULL::pg_catalog.int4 as aggmtransspace,
    NULL::pg_catalog.text as agginitval,
    NULL::pg_catalog.text as aggminitval
FROM mz_internal.mz_aggregates a",
    access: vec![PUBLIC_SELECT],
});

pub static PG_TRIGGER: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_trigger",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_TRIGGER_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("tgrelid", SqlScalarType::Oid.nullable(false))
        .with_column("tgparentid", SqlScalarType::Oid.nullable(false))
        .with_column("tgname", SqlScalarType::String.nullable(false))
        .with_column("tgfoid", SqlScalarType::Oid.nullable(false))
        .with_column("tgtype", SqlScalarType::Int16.nullable(false))
        .with_column("tgenabled", SqlScalarType::PgLegacyChar.nullable(false))
        .with_column("tgisinternal", SqlScalarType::Bool.nullable(false))
        .with_column("tgconstrrelid", SqlScalarType::Oid.nullable(false))
        .with_column("tgconstrindid", SqlScalarType::Oid.nullable(false))
        .with_column("tgconstraint", SqlScalarType::Oid.nullable(false))
        .with_column("tgdeferrable", SqlScalarType::Bool.nullable(false))
        .with_column("tginitdeferred", SqlScalarType::Bool.nullable(false))
        .with_column("tgnargs", SqlScalarType::Int16.nullable(false))
        .with_column("tgattr", SqlScalarType::Int2Vector.nullable(false))
        .with_column("tgargs", SqlScalarType::Bytes.nullable(false))
        .with_column("tgqual", SqlScalarType::String.nullable(false))
        .with_column("tgoldtable", SqlScalarType::String.nullable(false))
        .with_column("tgnewtable", SqlScalarType::String.nullable(false))
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    -- MZ doesn't support triggers so all of these fields are NULL.
    NULL::pg_catalog.oid AS oid,
    NULL::pg_catalog.oid AS tgrelid,
    NULL::pg_catalog.oid AS tgparentid,
    NULL::pg_catalog.text AS tgname,
    NULL::pg_catalog.oid AS tgfoid,
    NULL::pg_catalog.int2 AS tgtype,
    NULL::pg_catalog.\"char\" AS tgenabled,
    NULL::pg_catalog.bool AS tgisinternal,
    NULL::pg_catalog.oid AS tgconstrrelid,
    NULL::pg_catalog.oid AS tgconstrindid,
    NULL::pg_catalog.oid AS tgconstraint,
    NULL::pg_catalog.bool AS tgdeferrable,
    NULL::pg_catalog.bool AS tginitdeferred,
    NULL::pg_catalog.int2 AS tgnargs,
    NULL::pg_catalog.int2vector AS tgattr,
    NULL::pg_catalog.bytea AS tgargs,
    -- NOTE: The tgqual column is actually type `pg_node_tree` which we don't support. CockroachDB
    -- uses text as a placeholder, so we'll follow their lead here.
    NULL::pg_catalog.text AS tgqual,
    NULL::pg_catalog.text AS tgoldtable,
    NULL::pg_catalog.text AS tgnewtable
WHERE false
    ",
    access: vec![PUBLIC_SELECT],
});

pub static PG_REWRITE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_rewrite",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_REWRITE_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("rulename", SqlScalarType::String.nullable(false))
        .with_column("ev_class", SqlScalarType::Oid.nullable(false))
        .with_column("ev_type", SqlScalarType::PgLegacyChar.nullable(false))
        .with_column("ev_enabled", SqlScalarType::PgLegacyChar.nullable(false))
        .with_column("is_instead", SqlScalarType::Bool.nullable(false))
        .with_column("ev_qual", SqlScalarType::String.nullable(false))
        .with_column("ev_action", SqlScalarType::String.nullable(false))
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    -- MZ doesn't support rewrite rules so all of these fields are NULL.
    NULL::pg_catalog.oid AS oid,
    NULL::pg_catalog.text AS rulename,
    NULL::pg_catalog.oid AS ev_class,
    NULL::pg_catalog.\"char\" AS ev_type,
    NULL::pg_catalog.\"char\" AS ev_enabled,
    NULL::pg_catalog.bool AS is_instead,
    -- NOTE: The ev_qual and ev_action columns are actually type `pg_node_tree` which we don't
    -- support. CockroachDB uses text as a placeholder, so we'll follow their lead here.
    NULL::pg_catalog.text AS ev_qual,
    NULL::pg_catalog.text AS ev_action
WHERE false
    ",
    access: vec![PUBLIC_SELECT],
});

pub static PG_EXTENSION: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_extension",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::VIEW_PG_EXTENSION_OID,
    desc: RelationDesc::builder()
        .with_column("oid", SqlScalarType::Oid.nullable(false))
        .with_column("extname", SqlScalarType::String.nullable(false))
        .with_column("extowner", SqlScalarType::Oid.nullable(false))
        .with_column("extnamespace", SqlScalarType::Oid.nullable(false))
        .with_column("extrelocatable", SqlScalarType::Bool.nullable(false))
        .with_column("extversion", SqlScalarType::String.nullable(false))
        .with_column(
            "extconfig",
            SqlScalarType::Array(Box::new(SqlScalarType::Oid)).nullable(false),
        )
        .with_column(
            "extcondition",
            SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false),
        )
        .with_key(vec![])
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "SELECT
    -- MZ doesn't support extensions so all of these fields are NULL.
    NULL::pg_catalog.oid AS oid,
    NULL::pg_catalog.text AS extname,
    NULL::pg_catalog.oid AS extowner,
    NULL::pg_catalog.oid AS extnamespace,
    NULL::pg_catalog.bool AS extrelocatable,
    NULL::pg_catalog.text AS extversion,
    NULL::pg_catalog.oid[] AS extconfig,
    NULL::pg_catalog.text[] AS extcondition
WHERE false
    ",
    access: vec![PUBLIC_SELECT],
});

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
        .with_column("replacing", SqlScalarType::String.nullable(true))
        .finish(),
    column_comments: BTreeMap::new(),
    sql: "
WITH
    comments AS (
        SELECT id, comment
        FROM mz_internal.mz_comments
        WHERE object_type = 'materialized-view' AND object_sub_id IS NULL
    ),
    replacements AS (
        SELECT r.id, mv.name AS target_name
        FROM mz_internal.mz_replacements r
        JOIN mz_materialized_views mv ON r.target_id = mv.id
    )
SELECT
    mviews.id as id,
    mviews.name,
    clusters.name AS cluster,
    schema_id,
    cluster_id,
    COALESCE(comments.comment, '') as comment,
    replacements.target_name as replacing
FROM
    mz_catalog.mz_materialized_views AS mviews
    JOIN mz_catalog.mz_clusters AS clusters ON clusters.id = mviews.cluster_id
    LEFT JOIN comments ON mviews.id = comments.id
    LEFT JOIN replacements ON mviews.id = replacements.id",
    access: vec![PUBLIC_SELECT],
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
});

pub static MZ_SHOW_CONTINUAL_TASKS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_show_continual_tasks",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_SHOW_CONTINUAL_TASKS_OID,
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
WITH comments AS (
    SELECT id, comment
    FROM mz_internal.mz_comments
    WHERE object_type = 'continual-task' AND object_sub_id IS NULL
)
SELECT
    cts.id as id,
    cts.name,
    clusters.name AS cluster,
    schema_id,
    cluster_id,
    COALESCE(comments.comment, '') as comment
FROM
    mz_internal.mz_continual_tasks AS cts
    JOIN mz_catalog.mz_clusters AS clusters ON clusters.id = cts.cluster_id
    LEFT JOIN comments ON cts.id = comments.id",
    access: vec![PUBLIC_SELECT],
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
        ELSE pg_has_role(grantee, 'USAGE')
    END"#,
    access: vec![PUBLIC_SELECT],
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
continual_tasks AS (
    SELECT
        i.id AS object_id,
        h.replica_id,
        COALESCE(h.hydrated, false) AS hydrated
    FROM mz_internal.mz_continual_tasks i
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
SELECT * FROM continual_tasks
UNION ALL
SELECT * FROM sources
UNION ALL
SELECT * FROM sinks"#,
    access: vec![PUBLIC_SELECT],
});

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
        SELECT id FROM mz_internal.mz_continual_tasks
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

pub const MZ_CONTINUAL_TASKS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_continual_tasks_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_CONTINUAL_TASKS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_continual_tasks (id)",
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

pub const MZ_SHOW_CONTINUAL_TASKS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_continual_tasks_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SHOW_CONTINUAL_TASKS_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_show_continual_tasks (id)",
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
            "The ID of the sink. Corresponds to `mz_catalog.mz_sources.id`.",
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
});

pub const MZ_SINK_STATISTICS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_sink_statistics_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_SINK_STATISTICS_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_internal.mz_sink_statistics (id, replica_id)",
    is_retained_metrics_object: true,
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

pub const MZ_KAFKA_SOURCES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_kafka_sources_ind",
    schema: MZ_CATALOG_SCHEMA,
    oid: oid::INDEX_MZ_KAFKA_SOURCES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server
ON mz_catalog.mz_kafka_sources (id)",
    is_retained_metrics_object: true,
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
    let mut builtins = vec![
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
            builtins.push(Builtin::Func(BuiltinFunc {
                name,
                schema,
                inner: func,
            }));
        }
    }
    builtins.append(&mut vec![
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
        Builtin::Log(&MZ_MESSAGE_COUNTS_RECEIVED_RAW),
        Builtin::Log(&MZ_MESSAGE_COUNTS_SENT_RAW),
        Builtin::Log(&MZ_MESSAGE_BATCH_COUNTS_RECEIVED_RAW),
        Builtin::Log(&MZ_MESSAGE_BATCH_COUNTS_SENT_RAW),
        Builtin::Log(&MZ_ACTIVE_PEEKS_PER_WORKER),
        Builtin::Log(&MZ_PEEK_DURATIONS_HISTOGRAM_RAW),
        Builtin::Log(&MZ_DATAFLOW_SHUTDOWN_DURATIONS_HISTOGRAM_RAW),
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
        Builtin::Table(&MZ_KAFKA_CONNECTIONS),
        Builtin::Table(&MZ_KAFKA_SOURCES),
        Builtin::Table(&MZ_OBJECT_DEPENDENCIES),
        Builtin::Table(&MZ_ICEBERG_SINKS),
        Builtin::Table(&MZ_DATABASES),
        Builtin::Table(&MZ_SCHEMAS),
        Builtin::Table(&MZ_COLUMNS),
        Builtin::Table(&MZ_INDEXES),
        Builtin::Table(&MZ_INDEX_COLUMNS),
        Builtin::Table(&MZ_TABLES),
        Builtin::Table(&MZ_SOURCES),
        Builtin::Table(&MZ_SOURCE_REFERENCES),
        Builtin::Table(&MZ_POSTGRES_SOURCES),
        Builtin::Table(&MZ_POSTGRES_SOURCE_TABLES),
        Builtin::Table(&MZ_MYSQL_SOURCE_TABLES),
        Builtin::Table(&MZ_SQL_SERVER_SOURCE_TABLES),
        Builtin::Table(&MZ_KAFKA_SOURCE_TABLES),
        Builtin::Table(&MZ_SINKS),
        Builtin::Table(&MZ_VIEWS),
        Builtin::Table(&MZ_MATERIALIZED_VIEWS),
        Builtin::Table(&MZ_MATERIALIZED_VIEW_REFRESH_STRATEGIES),
        Builtin::Table(&MZ_TYPES),
        Builtin::Table(&MZ_TYPE_PG_METADATA),
        Builtin::Table(&MZ_ARRAY_TYPES),
        Builtin::Table(&MZ_BASE_TYPES),
        Builtin::Table(&MZ_LIST_TYPES),
        Builtin::Table(&MZ_MAP_TYPES),
        Builtin::Table(&MZ_ROLES),
        Builtin::Table(&MZ_ROLE_AUTH),
        Builtin::Table(&MZ_ROLE_MEMBERS),
        Builtin::Table(&MZ_ROLE_PARAMETERS),
        Builtin::Table(&MZ_PSEUDO_TYPES),
        Builtin::Table(&MZ_FUNCTIONS),
        Builtin::Table(&MZ_OPERATORS),
        Builtin::Table(&MZ_AGGREGATES),
        Builtin::Table(&MZ_CLUSTERS),
        Builtin::Table(&MZ_CLUSTER_WORKLOAD_CLASSES),
        Builtin::Table(&MZ_CLUSTER_SCHEDULES),
        Builtin::Table(&MZ_SECRETS),
        Builtin::Table(&MZ_CONNECTIONS),
        Builtin::Table(&MZ_SSH_TUNNEL_CONNECTIONS),
        Builtin::Table(&MZ_CLUSTER_REPLICAS),
        Builtin::Source(&MZ_CLUSTER_REPLICA_METRICS_HISTORY),
        Builtin::View(&MZ_CLUSTER_REPLICA_METRICS),
        Builtin::Table(&MZ_CLUSTER_REPLICA_SIZES),
        Builtin::Source(&MZ_CLUSTER_REPLICA_STATUS_HISTORY),
        Builtin::View(&MZ_CLUSTER_REPLICA_STATUSES),
        Builtin::Table(&MZ_INTERNAL_CLUSTER_REPLICAS),
        Builtin::Table(&MZ_PENDING_CLUSTER_REPLICAS),
        Builtin::Table(&MZ_AUDIT_EVENTS),
        Builtin::Table(&MZ_STORAGE_USAGE_BY_SHARD),
        Builtin::Table(&MZ_EGRESS_IPS),
        Builtin::Table(&MZ_AWS_PRIVATELINK_CONNECTIONS),
        Builtin::Table(&MZ_AWS_CONNECTIONS),
        Builtin::Table(&MZ_SUBSCRIPTIONS),
        Builtin::Table(&MZ_SESSIONS),
        Builtin::Table(&MZ_DEFAULT_PRIVILEGES),
        Builtin::Table(&MZ_SYSTEM_PRIVILEGES),
        Builtin::Table(&MZ_COMMENTS),
        Builtin::Table(&MZ_WEBHOOKS_SOURCES),
        Builtin::Table(&MZ_HISTORY_RETENTION_STRATEGIES),
        Builtin::Table(&MZ_CONTINUAL_TASKS),
        Builtin::Table(&MZ_NETWORK_POLICIES),
        Builtin::Table(&MZ_NETWORK_POLICY_RULES),
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
        Builtin::View(&MZ_COMPUTE_EXPORTS),
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
        Builtin::View(&MZ_DATAFLOW_SHUTDOWN_DURATIONS_HISTOGRAM_PER_WORKER),
        Builtin::View(&MZ_DATAFLOW_SHUTDOWN_DURATIONS_HISTOGRAM),
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
        Builtin::View(&MZ_SHOW_CONTINUAL_TASKS),
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
        Builtin::View(&MZ_COMPUTE_ERROR_COUNTS_PER_WORKER),
        Builtin::View(&MZ_COMPUTE_ERROR_COUNTS),
        Builtin::Source(&MZ_COMPUTE_ERROR_COUNTS_RAW_UNIFIED),
        Builtin::Source(&MZ_COMPUTE_HYDRATION_TIMES),
        Builtin::Log(&MZ_COMPUTE_LIR_MAPPING_PER_WORKER),
        Builtin::View(&MZ_LIR_MAPPING),
        Builtin::Source(&MZ_COMPUTE_OPERATOR_HYDRATION_STATUSES),
        Builtin::Source(&MZ_CLUSTER_REPLICA_FRONTIERS),
        Builtin::View(&MZ_COMPUTE_HYDRATION_STATUSES),
        Builtin::View(&MZ_HYDRATION_STATUSES),
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
        Builtin::Index(&MZ_INDEXES_IND),
        Builtin::Index(&MZ_ROLES_IND),
        Builtin::Index(&MZ_SOURCES_IND),
        Builtin::Index(&MZ_SINKS_IND),
        Builtin::Index(&MZ_MATERIALIZED_VIEWS_IND),
        Builtin::Index(&MZ_CONTINUAL_TASKS_IND),
        Builtin::Index(&MZ_SOURCE_STATUSES_IND),
        Builtin::Index(&MZ_SOURCE_STATUS_HISTORY_IND),
        Builtin::Index(&MZ_SINK_STATUSES_IND),
        Builtin::Index(&MZ_SINK_STATUS_HISTORY_IND),
        Builtin::Index(&MZ_CLUSTER_REPLICAS_IND),
        Builtin::Index(&MZ_CLUSTER_REPLICA_SIZES_IND),
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
        Builtin::Index(&MZ_CLUSTER_DEPLOYMENT_LINEAGE_IND),
        Builtin::Index(&MZ_CLUSTER_REPLICA_FRONTIERS_IND),
        Builtin::Index(&MZ_COMPUTE_HYDRATION_TIMES_IND),
        Builtin::View(&MZ_RECENT_STORAGE_USAGE),
        Builtin::Index(&MZ_RECENT_STORAGE_USAGE_IND),
        Builtin::Connection(&MZ_ANALYTICS),
        Builtin::ContinualTask(&MZ_CLUSTER_REPLICA_METRICS_HISTORY_CT),
        Builtin::ContinualTask(&MZ_CLUSTER_REPLICA_STATUS_HISTORY_CT),
        Builtin::ContinualTask(&MZ_WALLCLOCK_LAG_HISTORY_CT),
        Builtin::View(&MZ_INDEX_ADVICE),
    ]);

    builtins.extend(notice::builtins());

    builtins
});
pub const BUILTIN_ROLES: &[&BuiltinRole] = &[
    &MZ_SYSTEM_ROLE,
    &MZ_SUPPORT_ROLE,
    &MZ_ANALYTICS_ROLE,
    &MZ_MONITOR_ROLE,
    &MZ_MONITOR_REDACTED,
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
    use mz_sql::catalog::BuiltinsConfig;

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

    pub fn funcs() -> impl Iterator<Item = &'static BuiltinFunc> {
        BUILTINS_STATIC.iter().filter_map(|b| match b {
            Builtin::Func(func) => Some(func),
            _ => None,
        })
    }

    pub fn iter(cfg: &BuiltinsConfig) -> impl Iterator<Item = &'static Builtin<NameReference>> {
        let include_continual_tasks = cfg.include_continual_tasks;
        BUILTINS_STATIC.iter().filter(move |x| match x {
            Builtin::ContinualTask(_) if !include_continual_tasks => false,
            _ => true,
        })
    }
}

pub static BUILTIN_LOG_LOOKUP: LazyLock<HashMap<&'static str, &'static BuiltinLog>> =
    LazyLock::new(|| BUILTINS::logs().map(|log| (log.name, log)).collect());
/// Keys are builtin object description, values are the builtin index when sorted by dependency and
/// the builtin itself.
pub static BUILTIN_LOOKUP: LazyLock<
    HashMap<SystemObjectDescription, (usize, &'static Builtin<NameReference>)>,
> = LazyLock::new(|| {
    // BUILTIN_LOOKUP is only ever used for lookups by key, it's not iterated,
    // so it's safe to include all of them, regardless of BuiltinConfig. We
    // enforce this statically by using the mz_ore HashMap which disallows
    // iteration.
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

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
fn test_builtin_type_schema() {
    use mz_pgrepr::oid::FIRST_MATERIALIZE_OID;

    for typ in BUILTINS::types() {
        if typ.oid < FIRST_MATERIALIZE_OID {
            assert_eq!(
                typ.schema, PG_CATALOG_SCHEMA,
                "{typ:?} should be in {PG_CATALOG_SCHEMA} schema"
            );
        } else {
            // `mz_pgrepr::Type` resolution relies on all non-PG types existing in the mz_catalog
            // schema.
            assert_eq!(
                typ.schema, MZ_CATALOG_SCHEMA,
                "{typ:?} should be in {MZ_CATALOG_SCHEMA} schema"
            );
        }
    }
}
