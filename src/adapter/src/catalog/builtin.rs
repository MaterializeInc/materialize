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

use std::hash::Hash;

use once_cell::sync::Lazy;
use serde::Serialize;

use mz_compute_client::logging::{ComputeLog, DifferentialLog, LogVariant, TimelyLog};
use mz_pgrepr::oid;
use mz_repr::{RelationDesc, RelationType, ScalarType};
use mz_sql::catalog::{
    CatalogItemType, CatalogType, CatalogTypeDetails, NameReference, RoleAttributes, TypeReference,
};
use mz_sql::session::user::{INTROSPECTION_USER, SYSTEM_USER};
use mz_storage_client::controller::IntrospectionType;
use mz_storage_client::healthcheck::{MZ_SINK_STATUS_HISTORY_DESC, MZ_SOURCE_STATUS_HISTORY_DESC};

use crate::catalog::DEFAULT_CLUSTER_REPLICA_NAME;

pub const MZ_TEMP_SCHEMA: &str = "mz_temp";
pub const MZ_CATALOG_SCHEMA: &str = "mz_catalog";
pub const PG_CATALOG_SCHEMA: &str = "pg_catalog";
pub const MZ_INTERNAL_SCHEMA: &str = "mz_internal";
pub const INFORMATION_SCHEMA: &str = "information_schema";

pub static BUILTIN_PREFIXES: Lazy<Vec<&str>> = Lazy::new(|| vec!["mz_", "pg_"]);

#[derive(Debug)]
pub enum Builtin<T: 'static + TypeReference> {
    Log(&'static BuiltinLog),
    Table(&'static BuiltinTable),
    View(&'static BuiltinView),
    Type(&'static BuiltinType<T>),
    Func(BuiltinFunc),
    Source(&'static BuiltinSource),
    Index(&'static BuiltinIndex),
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
            Builtin::Index(index) => index.name,
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
            Builtin::Index(index) => index.schema,
        }
    }

    pub fn catalog_item_type(&self) -> CatalogItemType {
        match self {
            Builtin::Log(_) => CatalogItemType::Source,
            Builtin::Source(_) => CatalogItemType::Source,
            Builtin::Table(_) => CatalogItemType::Table,
            Builtin::View(_) => CatalogItemType::View,
            Builtin::Type(_) => CatalogItemType::Type,
            Builtin::Func(_) => CatalogItemType::Func,
            Builtin::Index(_) => CatalogItemType::Index,
        }
    }
}

#[derive(Clone, Debug, Hash, Serialize)]
pub struct BuiltinLog {
    pub variant: LogVariant,
    pub name: &'static str,
    pub schema: &'static str,
}

#[derive(Hash, Debug)]
pub struct BuiltinTable {
    pub name: &'static str,
    pub schema: &'static str,
    pub desc: RelationDesc,
    /// Whether the table's retention policy is controlled by
    /// the system variable `METRICS_RETENTION`
    pub is_retained_metrics_relation: bool,
}

#[derive(Clone, Debug, Hash, Serialize)]
pub struct BuiltinSource {
    pub name: &'static str,
    pub schema: &'static str,
    pub desc: RelationDesc,
    pub data_source: Option<IntrospectionType>,
    /// Whether the source's retention policy is controlled by
    /// the system variable `METRICS_RETENTION`
    pub is_retained_metrics_relation: bool,
}

#[derive(Hash, Debug)]
pub struct BuiltinView {
    pub name: &'static str,
    pub schema: &'static str,
    pub sql: &'static str,
}

#[derive(Debug)]
pub struct BuiltinType<T: TypeReference> {
    pub name: &'static str,
    pub schema: &'static str,
    pub oid: u32,
    pub details: CatalogTypeDetails<T>,
}

#[derive(Debug)]
pub struct BuiltinFunc {
    pub schema: &'static str,
    pub name: &'static str,
    pub inner: &'static mz_sql::func::Func,
}

#[derive(Debug)]
pub struct BuiltinIndex {
    pub name: &'static str,
    pub schema: &'static str,
    pub sql: &'static str,
}

#[derive(Clone, Debug)]
pub struct BuiltinRole {
    /// Name of the builtin role.
    ///
    /// IMPORTANT: Must start with a prefix from [`BUILTIN_PREFIXES`].
    pub name: &'static str,
    pub attributes: RoleAttributes,
}

#[derive(Clone, Debug)]
pub struct BuiltinCluster {
    /// Name of the cluster.
    ///
    /// IMPORTANT: Must start with a prefix from [`BUILTIN_PREFIXES`].
    pub name: &'static str,
}

#[derive(Clone, Debug)]
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
            Builtin::Index(index) => index.fingerprint(),
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

impl Fingerprint for &BuiltinIndex {
    fn fingerprint(&self) -> String {
        self.sql.to_string()
    }
}

impl Fingerprint for RelationDesc {
    fn fingerprint(&self) -> String {
        self.typ().fingerprint()
    }
}

impl Fingerprint for RelationType {
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
    },
};

pub const TYPE_BYTEA: BuiltinType<NameReference> = BuiltinType {
    name: "bytea",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_BYTEA_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Bytes,
        array_id: None,
    },
};

pub const TYPE_INT8: BuiltinType<NameReference> = BuiltinType {
    name: "int8",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_INT8_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Int64,
        array_id: None,
    },
};

pub const TYPE_INT4: BuiltinType<NameReference> = BuiltinType {
    name: "int4",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_INT4_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Int32,
        array_id: None,
    },
};

pub const TYPE_TEXT: BuiltinType<NameReference> = BuiltinType {
    name: "text",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_TEXT_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::String,
        array_id: None,
    },
};

pub const TYPE_OID: BuiltinType<NameReference> = BuiltinType {
    name: "oid",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_OID_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Oid,
        array_id: None,
    },
};

pub const TYPE_FLOAT4: BuiltinType<NameReference> = BuiltinType {
    name: "float4",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_FLOAT4_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Float32,
        array_id: None,
    },
};

pub const TYPE_FLOAT8: BuiltinType<NameReference> = BuiltinType {
    name: "float8",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_FLOAT8_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Float64,
        array_id: None,
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
    },
};

pub const TYPE_DATE: BuiltinType<NameReference> = BuiltinType {
    name: "date",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_DATE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Date,
        array_id: None,
    },
};

pub const TYPE_TIME: BuiltinType<NameReference> = BuiltinType {
    name: "time",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_TIME_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Time,
        array_id: None,
    },
};

pub const TYPE_TIMESTAMP: BuiltinType<NameReference> = BuiltinType {
    name: "timestamp",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_TIMESTAMP_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Timestamp,
        array_id: None,
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
    },
};

pub const TYPE_TIMESTAMPTZ: BuiltinType<NameReference> = BuiltinType {
    name: "timestamptz",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_TIMESTAMPTZ_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::TimestampTz,
        array_id: None,
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
    },
};

pub const TYPE_INTERVAL: BuiltinType<NameReference> = BuiltinType {
    name: "interval",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_INTERVAL_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Interval,
        array_id: None,
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
    },
};

pub const TYPE_NUMERIC: BuiltinType<NameReference> = BuiltinType {
    name: "numeric",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_NUMERIC_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Numeric,
        array_id: None,
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
    },
};

pub const TYPE_RECORD: BuiltinType<NameReference> = BuiltinType {
    name: "record",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_RECORD_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
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
    },
};

pub const TYPE_UUID: BuiltinType<NameReference> = BuiltinType {
    name: "uuid",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_UUID_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Uuid,
        array_id: None,
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
    },
};

pub const TYPE_JSONB: BuiltinType<NameReference> = BuiltinType {
    name: "jsonb",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_JSONB_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Jsonb,
        array_id: None,
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
    },
};

pub const TYPE_ANY: BuiltinType<NameReference> = BuiltinType {
    name: "any",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "anyarray",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYELEMENT: BuiltinType<NameReference> = BuiltinType {
    name: "anyelement",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYELEMENT_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYNONARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "anynonarray",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYNONARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYRANGE: BuiltinType<NameReference> = BuiltinType {
    name: "anyrange",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYRANGE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_CHAR: BuiltinType<NameReference> = BuiltinType {
    name: "char",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_CHAR_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::PgLegacyChar,
        array_id: None,
    },
};

pub const TYPE_VARCHAR: BuiltinType<NameReference> = BuiltinType {
    name: "varchar",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_VARCHAR_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::VarChar,
        array_id: None,
    },
};

pub const TYPE_INT2: BuiltinType<NameReference> = BuiltinType {
    name: "int2",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_INT2_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Int16,
        array_id: None,
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
    },
};

pub const TYPE_BPCHAR: BuiltinType<NameReference> = BuiltinType {
    name: "bpchar",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_BPCHAR_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Char,
        array_id: None,
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
    },
};

pub const TYPE_REGPROC: BuiltinType<NameReference> = BuiltinType {
    name: "regproc",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_REGPROC_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::RegProc,
        array_id: None,
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
    },
};

pub const TYPE_REGTYPE: BuiltinType<NameReference> = BuiltinType {
    name: "regtype",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_REGTYPE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::RegType,
        array_id: None,
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
    },
};

pub const TYPE_REGCLASS: BuiltinType<NameReference> = BuiltinType {
    name: "regclass",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_REGCLASS_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::RegClass,
        array_id: None,
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
    },
};

pub const TYPE_INT2_VECTOR: BuiltinType<NameReference> = BuiltinType {
    name: "int2vector",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_INT2_VECTOR_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Int2Vector,
        array_id: None,
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
    },
};

pub const TYPE_ANYCOMPATIBLE: BuiltinType<NameReference> = BuiltinType {
    name: "anycompatible",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYCOMPATIBLE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYCOMPATIBLEARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "anycompatiblearray",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYCOMPATIBLEARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYCOMPATIBLENONARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "anycompatiblenonarray",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYCOMPATIBLENONARRAY_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYCOMPATIBLERANGE: BuiltinType<NameReference> = BuiltinType {
    name: "anycompatiblerange",
    schema: PG_CATALOG_SCHEMA,
    oid: oid::TYPE_ANYCOMPATIBLERANGE_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_LIST: BuiltinType<NameReference> = BuiltinType {
    name: "list",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_LIST_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_MAP: BuiltinType<NameReference> = BuiltinType {
    name: "map",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_MAP_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYCOMPATIBLELIST: BuiltinType<NameReference> = BuiltinType {
    name: "anycompatiblelist",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_ANYCOMPATIBLELIST_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYCOMPATIBLEMAP: BuiltinType<NameReference> = BuiltinType {
    name: "anycompatiblemap",
    schema: PG_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_ANYCOMPATIBLEMAP_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_UINT2: BuiltinType<NameReference> = BuiltinType {
    name: "uint2",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_UINT2_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::UInt16,
        array_id: None,
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
    },
};

pub const TYPE_UINT4: BuiltinType<NameReference> = BuiltinType {
    name: "uint4",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_UINT4_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::UInt32,
        array_id: None,
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
    },
};

pub const TYPE_UINT8: BuiltinType<NameReference> = BuiltinType {
    name: "uint8",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_UINT8_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::UInt64,
        array_id: None,
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
    },
};

pub const TYPE_MZ_TIMESTAMP: BuiltinType<NameReference> = BuiltinType {
    name: "mz_timestamp",
    schema: MZ_CATALOG_SCHEMA,
    oid: mz_pgrepr::oid::TYPE_MZ_TIMESTAMP_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::MzTimestamp,
        array_id: None,
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
    },
};

pub const MZ_DATAFLOW_OPERATORS: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_operators",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Operates),
};

pub const MZ_DATAFLOW_OPERATORS_ADDRESSES: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_addresses",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Addresses),
};

pub const MZ_DATAFLOW_CHANNELS: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_channels",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Channels),
};

pub const MZ_SCHEDULING_ELAPSED_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_scheduling_elapsed_internal",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Elapsed),
};

pub const MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_compute_operator_durations_histogram_internal",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Histogram),
};

pub const MZ_SCHEDULING_PARKS_HISTOGRAM_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_scheduling_parks_histogram_internal",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Parks),
};

pub const MZ_ARRANGEMENT_BATCHES_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_arrangement_batches_internal",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Differential(DifferentialLog::ArrangementBatches),
};

pub const MZ_ARRANGEMENT_SHARING_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_arrangement_sharing_internal",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Differential(DifferentialLog::Sharing),
};

pub const MZ_COMPUTE_EXPORTS: BuiltinLog = BuiltinLog {
    name: "mz_compute_exports",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Compute(ComputeLog::DataflowCurrent),
};

pub const MZ_WORKER_COMPUTE_DEPENDENCIES: BuiltinLog = BuiltinLog {
    name: "mz_worker_compute_dependencies",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Compute(ComputeLog::DataflowDependency),
};

pub const MZ_WORKER_COMPUTE_FRONTIERS: BuiltinLog = BuiltinLog {
    name: "mz_worker_compute_frontiers",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Compute(ComputeLog::FrontierCurrent),
};

pub const MZ_WORKER_COMPUTE_IMPORT_FRONTIERS: BuiltinLog = BuiltinLog {
    name: "mz_worker_compute_import_frontiers",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Compute(ComputeLog::ImportFrontierCurrent),
};

pub const MZ_WORKER_COMPUTE_DELAYS_HISTOGRAM_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_worker_compute_delays_histogram_internal",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Compute(ComputeLog::FrontierDelay),
};

pub const MZ_ACTIVE_PEEKS: BuiltinLog = BuiltinLog {
    name: "mz_active_peeks",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Compute(ComputeLog::PeekCurrent),
};

pub const MZ_PEEK_DURATIONS_HISTOGRAM_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_peek_durations_histogram_internal",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Compute(ComputeLog::PeekDuration),
};

pub const MZ_MESSAGE_COUNTS_RECEIVED_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_message_counts_received_internal",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::MessagesReceived),
};

pub const MZ_MESSAGE_COUNTS_SENT_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_message_counts_sent_internal",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::MessagesSent),
};

pub const MZ_DATAFLOW_OPERATOR_REACHABILITY_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_operator_reachability_internal",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Reachability),
};

pub const MZ_ARRANGEMENT_RECORDS_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_arrangement_records_internal",
    schema: MZ_INTERNAL_SCHEMA,
    variant: LogVariant::Differential(DifferentialLog::ArrangementRecords),
};

pub static MZ_VIEW_KEYS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_view_keys",
    schema: MZ_INTERNAL_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("object_id", ScalarType::String.nullable(false))
        .with_column("column", ScalarType::UInt64.nullable(false))
        .with_column("key_group", ScalarType::UInt64.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_VIEW_FOREIGN_KEYS: Lazy<BuiltinTable> = Lazy::new(|| {
    BuiltinTable {
        name: "mz_view_foreign_keys",
        schema: MZ_INTERNAL_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("child_id", ScalarType::String.nullable(false))
            .with_column("child_column", ScalarType::UInt64.nullable(false))
            .with_column("parent_id", ScalarType::String.nullable(false))
            .with_column("parent_column", ScalarType::UInt64.nullable(false))
            .with_column("key_group", ScalarType::UInt64.nullable(false))
            .with_key(vec![0, 1, 4]), // TODO: explain why this is a key.
        is_retained_metrics_relation: false,
    }
});
pub static MZ_KAFKA_SINKS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_kafka_sinks",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("topic", ScalarType::String.nullable(false))
        .with_key(vec![0]),
    is_retained_metrics_relation: false,
});
pub static MZ_KAFKA_CONNECTIONS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_kafka_connections",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column(
            "brokers",
            ScalarType::Array(Box::new(ScalarType::String)).nullable(false),
        )
        .with_column("sink_progress_topic", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_POSTGRES_SOURCES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_postgres_sources",
    schema: MZ_INTERNAL_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("replication_slot", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_OBJECT_DEPENDENCIES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_object_dependencies",
    schema: MZ_INTERNAL_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("object_id", ScalarType::String.nullable(false))
        .with_column("referenced_object_id", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_DATABASES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_databases",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::UInt64.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("name", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_SCHEMAS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_schemas",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::UInt64.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("database_id", ScalarType::UInt64.nullable(true))
        .with_column("name", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_COLUMNS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_columns",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("position", ScalarType::UInt64.nullable(false))
        .with_column("nullable", ScalarType::Bool.nullable(false))
        .with_column("type", ScalarType::String.nullable(false))
        .with_column("default", ScalarType::String.nullable(true))
        .with_column("type_oid", ScalarType::Oid.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_INDEXES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_indexes",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("on_id", ScalarType::String.nullable(false))
        .with_column("cluster_id", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_INDEX_COLUMNS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_index_columns",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("index_id", ScalarType::String.nullable(false))
        .with_column("index_position", ScalarType::UInt64.nullable(false))
        .with_column("on_position", ScalarType::UInt64.nullable(true))
        .with_column("on_expression", ScalarType::String.nullable(true))
        .with_column("nullable", ScalarType::Bool.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_TABLES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_tables",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::UInt64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_CONNECTIONS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_connections",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::UInt64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("type", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_SSH_TUNNEL_CONNECTIONS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_ssh_tunnel_connections",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("public_key_1", ScalarType::String.nullable(false))
        .with_column("public_key_2", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_SOURCES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_sources",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::UInt64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("type", ScalarType::String.nullable(false))
        .with_column("connection_id", ScalarType::String.nullable(true))
        .with_column("size", ScalarType::String.nullable(true))
        .with_column("envelope_type", ScalarType::String.nullable(true))
        .with_column("cluster_id", ScalarType::String.nullable(true)),
    is_retained_metrics_relation: true,
});
pub static MZ_SINKS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_sinks",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::UInt64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("type", ScalarType::String.nullable(false))
        .with_column("connection_id", ScalarType::String.nullable(true))
        .with_column("size", ScalarType::String.nullable(true))
        .with_column("envelope_type", ScalarType::String.nullable(true))
        .with_column("cluster_id", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: true,
});
pub static MZ_VIEWS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_views",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::UInt64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("definition", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_MATERIALIZED_VIEWS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_materialized_views",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::UInt64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("cluster_id", ScalarType::String.nullable(false))
        .with_column("definition", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_TYPES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_types",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::UInt64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("category", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_ARRAY_TYPES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_array_types",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("element_id", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_BASE_TYPES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_base_types",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty().with_column("id", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_LIST_TYPES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_list_types",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("element_id", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_MAP_TYPES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_map_types",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("key_id", ScalarType::String.nullable(false))
        .with_column("value_id", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_ROLES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_roles",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("inherit", ScalarType::Bool.nullable(false))
        .with_column("create_role", ScalarType::Bool.nullable(false))
        .with_column("create_db", ScalarType::Bool.nullable(false))
        .with_column("create_cluster", ScalarType::Bool.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_ROLE_MEMBERS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_role_members",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("role_id", ScalarType::String.nullable(false))
        .with_column("member", ScalarType::String.nullable(false))
        .with_column("grantor", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_PSEUDO_TYPES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_pseudo_types",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty().with_column("id", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_FUNCTIONS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_functions",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::UInt64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column(
            "argument_type_ids",
            ScalarType::Array(Box::new(ScalarType::String)).nullable(false),
        )
        .with_column(
            "variadic_argument_type_id",
            ScalarType::String.nullable(true),
        )
        .with_column("return_type_id", ScalarType::String.nullable(true))
        .with_column("returns_set", ScalarType::Bool.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_OPERATORS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_operators",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column(
            "argument_type_ids",
            ScalarType::Array(Box::new(ScalarType::String)).nullable(false),
        )
        .with_column("return_type_id", ScalarType::String.nullable(true)),
    is_retained_metrics_relation: false,
});

pub static MZ_CLUSTERS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_clusters",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("name", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});

pub static MZ_CLUSTER_LINKS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_cluster_links",
    schema: MZ_INTERNAL_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("cluster_id", ScalarType::String.nullable(false))
        .with_column("object_id", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});

pub static MZ_SECRETS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_secrets",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("schema_id", ScalarType::UInt64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});
pub static MZ_CLUSTER_REPLICAS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_cluster_replicas",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::UInt64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("cluster_id", ScalarType::String.nullable(false))
        .with_column("size", ScalarType::String.nullable(true))
        .with_column("availability_zone", ScalarType::String.nullable(true)),
    is_retained_metrics_relation: true,
});

pub static MZ_CLUSTER_REPLICA_STATUSES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_cluster_replica_statuses",
    schema: MZ_INTERNAL_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("replica_id", ScalarType::UInt64.nullable(false))
        .with_column("process_id", ScalarType::UInt64.nullable(false))
        .with_column("status", ScalarType::String.nullable(false))
        .with_column("updated_at", ScalarType::TimestampTz.nullable(false)),
    is_retained_metrics_relation: false,
});

pub static MZ_CLUSTER_REPLICA_SIZES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_cluster_replica_sizes",
    schema: MZ_INTERNAL_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("size", ScalarType::String.nullable(false))
        .with_column("processes", ScalarType::UInt64.nullable(false))
        .with_column("workers", ScalarType::UInt64.nullable(false))
        .with_column("cpu_nano_cores", ScalarType::UInt64.nullable(false))
        .with_column("memory_bytes", ScalarType::UInt64.nullable(false)),
    is_retained_metrics_relation: true,
});

pub static MZ_CLUSTER_REPLICA_HEARTBEATS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_cluster_replica_heartbeats",
    schema: MZ_INTERNAL_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("replica_id", ScalarType::UInt64.nullable(false))
        .with_column("last_heartbeat", ScalarType::TimestampTz.nullable(false)),
    is_retained_metrics_relation: false,
});

pub static MZ_AUDIT_EVENTS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_audit_events",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::UInt64.nullable(false))
        .with_column("event_type", ScalarType::String.nullable(false))
        .with_column("object_type", ScalarType::String.nullable(false))
        .with_column("details", ScalarType::Jsonb.nullable(false))
        .with_column("user", ScalarType::String.nullable(true))
        .with_column("occurred_at", ScalarType::TimestampTz.nullable(false)),
    is_retained_metrics_relation: false,
});

pub static MZ_SOURCE_STATUS_HISTORY: Lazy<BuiltinSource> = Lazy::new(|| BuiltinSource {
    name: "mz_source_status_history",
    schema: MZ_INTERNAL_SCHEMA,
    data_source: Some(IntrospectionType::SourceStatusHistory),
    desc: MZ_SOURCE_STATUS_HISTORY_DESC.clone(),
    is_retained_metrics_relation: false,
});

pub const MZ_SOURCE_STATUSES: BuiltinView = BuiltinView {
    name: "mz_source_statuses",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_source_statuses AS
WITH latest_events AS (
    SELECT DISTINCT ON(source_id) occurred_at, source_id, status, error, details
    FROM mz_internal.mz_source_status_history
    ORDER BY source_id, occurred_at DESC
)
SELECT
    mz_sources.id,
    name,
    mz_sources.type,
    occurred_at as last_status_change_at,
    coalesce(status, 'created') as status,
    error,
    details
FROM mz_sources
LEFT JOIN latest_events ON mz_sources.id = latest_events.source_id
WHERE
    -- This is a convenient way to filter out system sources, like the status_history table itself.
    mz_sources.id NOT LIKE 's%' and mz_sources.type != 'subsource'",
};

pub static MZ_SINK_STATUS_HISTORY: Lazy<BuiltinSource> = Lazy::new(|| BuiltinSource {
    name: "mz_sink_status_history",
    schema: MZ_INTERNAL_SCHEMA,
    data_source: Some(IntrospectionType::SinkStatusHistory),
    desc: MZ_SINK_STATUS_HISTORY_DESC.clone(),
    is_retained_metrics_relation: false,
});

pub const MZ_SINK_STATUSES: BuiltinView = BuiltinView {
    name: "mz_sink_statuses",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_sink_statuses AS
WITH latest_events AS (
    SELECT DISTINCT ON(sink_id) occurred_at, sink_id, status, error, details
    FROM mz_internal.mz_sink_status_history
    ORDER BY sink_id, occurred_at DESC
)
SELECT
    mz_sinks.id,
    name,
    mz_sinks.type,
    occurred_at as last_status_change_at,
    coalesce(status, 'created') as status,
    error,
    details
FROM mz_sinks
LEFT JOIN latest_events ON mz_sinks.id = latest_events.sink_id
WHERE
    -- This is a convenient way to filter out system sinks, like the status_history table itself.
    mz_sinks.id NOT LIKE 's%'",
};

pub static MZ_STORAGE_USAGE_BY_SHARD: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_storage_usage_by_shard",
    schema: MZ_INTERNAL_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::UInt64.nullable(false))
        .with_column("shard_id", ScalarType::String.nullable(true))
        .with_column("size_bytes", ScalarType::UInt64.nullable(false))
        .with_column(
            "collection_timestamp",
            ScalarType::TimestampTz.nullable(false),
        ),
    is_retained_metrics_relation: false,
});

pub static MZ_EGRESS_IPS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_egress_ips",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty().with_column("egress_ip", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});

pub static MZ_AWS_PRIVATELINK_CONNECTIONS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_aws_privatelink_connections",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("principal", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});

pub static MZ_CLUSTER_REPLICA_METRICS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_cluster_replica_metrics",
    // TODO[btv] - make this public once we work out whether and how to fuse it with
    // the corresponding Storage tables.
    schema: MZ_INTERNAL_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("replica_id", ScalarType::UInt64.nullable(false))
        .with_column("process_id", ScalarType::UInt64.nullable(false))
        .with_column("cpu_nano_cores", ScalarType::UInt64.nullable(true))
        .with_column("memory_bytes", ScalarType::UInt64.nullable(true)),
    is_retained_metrics_relation: true,
});

pub static MZ_CLUSTER_REPLICA_FRONTIERS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_cluster_replica_frontiers",
    schema: MZ_INTERNAL_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("replica_id", ScalarType::UInt64.nullable(false))
        .with_column("export_id", ScalarType::String.nullable(false))
        .with_column("time", ScalarType::MzTimestamp.nullable(false)),
    is_retained_metrics_relation: false,
});

pub static MZ_SUBSCRIPTIONS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_subscriptions",
    schema: MZ_INTERNAL_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("user", ScalarType::String.nullable(false))
        .with_column("cluster_id", ScalarType::String.nullable(false))
        .with_column("created_at", ScalarType::TimestampTz.nullable(false))
        .with_column(
            "referenced_object_ids",
            ScalarType::List {
                element_type: Box::new(ScalarType::String),
                custom_id: None,
            }
            .nullable(false),
        ),
    is_retained_metrics_relation: false,
});

// These will be replaced with per-replica tables once source/sink multiplexing on
// a single cluster is supported.
pub static MZ_SOURCE_STATISTICS: Lazy<BuiltinSource> = Lazy::new(|| BuiltinSource {
    name: "mz_source_statistics",
    schema: MZ_INTERNAL_SCHEMA,
    data_source: Some(IntrospectionType::StorageSourceStatistics),
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("worker_id", ScalarType::UInt64.nullable(false))
        .with_column("snapshot_committed", ScalarType::Bool.nullable(false))
        .with_column("messages_received", ScalarType::UInt64.nullable(false))
        .with_column("updates_staged", ScalarType::UInt64.nullable(false))
        .with_column("updates_committed", ScalarType::UInt64.nullable(false))
        .with_column("bytes_received", ScalarType::UInt64.nullable(false)),
    is_retained_metrics_relation: true,
});
pub static MZ_SINK_STATISTICS: Lazy<BuiltinSource> = Lazy::new(|| BuiltinSource {
    name: "mz_sink_statistics",
    schema: MZ_INTERNAL_SCHEMA,
    data_source: Some(IntrospectionType::StorageSinkStatistics),
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("worker_id", ScalarType::UInt64.nullable(false))
        .with_column("messages_staged", ScalarType::UInt64.nullable(false))
        .with_column("messages_committed", ScalarType::UInt64.nullable(false))
        .with_column("bytes_staged", ScalarType::UInt64.nullable(false))
        .with_column("bytes_committed", ScalarType::UInt64.nullable(false)),
    is_retained_metrics_relation: true,
});

pub static MZ_STORAGE_SHARDS: Lazy<BuiltinSource> = Lazy::new(|| BuiltinSource {
    name: "mz_storage_shards",
    schema: MZ_INTERNAL_SCHEMA,
    data_source: Some(IntrospectionType::ShardMapping),
    desc: RelationDesc::empty()
        .with_column("object_id", ScalarType::String.nullable(false))
        .with_column("shard_id", ScalarType::String.nullable(false)),
    is_retained_metrics_relation: false,
});

pub static MZ_STORAGE_USAGE: Lazy<BuiltinView> = Lazy::new(|| BuiltinView {
    name: "mz_storage_usage",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_storage_usage (object_id, size_bytes, collection_timestamp) AS
SELECT
    object_id,
    sum(size_bytes)::uint8,
    collection_timestamp
FROM
    mz_internal.mz_storage_shards
    JOIN mz_internal.mz_storage_usage_by_shard USING (shard_id)
GROUP BY object_id, collection_timestamp",
});

pub const MZ_RELATIONS: BuiltinView = BuiltinView {
    name: "mz_relations",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_relations (id, oid, schema_id, name, type) AS
      SELECT id, oid, schema_id, name, 'table' FROM mz_catalog.mz_tables
UNION ALL SELECT id, oid, schema_id, name, 'source' FROM mz_catalog.mz_sources
UNION ALL SELECT id, oid, schema_id, name, 'view' FROM mz_catalog.mz_views
UNION ALL SELECT id, oid, schema_id, name, 'materialized-view' FROM mz_catalog.mz_materialized_views",
};

pub const MZ_OBJECTS: BuiltinView = BuiltinView {
    name: "mz_objects",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_objects (id, oid, schema_id, name, type) AS
    SELECT id, oid, schema_id, name, type FROM mz_catalog.mz_relations
UNION ALL
    SELECT id, oid, schema_id, name, 'sink' FROM mz_catalog.mz_sinks
UNION ALL
    SELECT mz_indexes.id, mz_indexes.oid, mz_relations.schema_id, mz_indexes.name, 'index'
    FROM mz_catalog.mz_indexes
    JOIN mz_catalog.mz_relations ON mz_indexes.on_id = mz_relations.id
UNION ALL
    SELECT id, oid, schema_id, name, 'connection' FROM mz_catalog.mz_connections
UNION ALL
    SELECT id, oid, schema_id, name, 'type' FROM mz_catalog.mz_types
UNION ALL
    SELECT id, oid, schema_id, name, 'function' FROM mz_catalog.mz_functions
UNION ALL
    SELECT id, NULL::pg_catalog.oid, schema_id, name, 'secret' FROM mz_catalog.mz_secrets",
};

pub const MZ_DATAFLOWS: BuiltinView = BuiltinView {
    name: "mz_dataflows",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_dataflows AS SELECT
    mz_dataflow_addresses.id,
    mz_dataflow_addresses.worker_id,
    mz_dataflow_addresses.address[1] AS local_id,
    mz_dataflow_operators.name
FROM
    mz_internal.mz_dataflow_addresses,
    mz_internal.mz_dataflow_operators
WHERE
    mz_dataflow_addresses.id = mz_dataflow_operators.id AND
    mz_dataflow_addresses.worker_id = mz_dataflow_operators.worker_id AND
    mz_catalog.list_length(mz_dataflow_addresses.address) = 1",
};

pub const MZ_DATAFLOW_OPERATOR_DATAFLOWS: BuiltinView = BuiltinView {
    name: "mz_dataflow_operator_dataflows",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_dataflow_operator_dataflows AS SELECT
    mz_dataflow_operators.id,
    mz_dataflow_operators.name,
    mz_dataflow_operators.worker_id,
    mz_dataflows.id as dataflow_id,
    mz_dataflows.name as dataflow_name
FROM
    mz_internal.mz_dataflow_operators,
    mz_internal.mz_dataflow_addresses,
    mz_internal.mz_dataflows
WHERE
    mz_dataflow_operators.id = mz_dataflow_addresses.id AND
    mz_dataflow_operators.worker_id = mz_dataflow_addresses.worker_id AND
    mz_dataflows.local_id = mz_dataflow_addresses.address[1] AND
    mz_dataflows.worker_id = mz_dataflow_addresses.worker_id",
};

pub const MZ_COMPUTE_FRONTIERS: BuiltinView = BuiltinView {
    name: "mz_compute_frontiers",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_compute_frontiers AS SELECT
    export_id, pg_catalog.min(time) AS time
FROM mz_internal.mz_worker_compute_frontiers
GROUP BY export_id",
};

pub const MZ_DATAFLOW_CHANNEL_OPERATORS: BuiltinView = BuiltinView {
    name: "mz_dataflow_channel_operators",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_dataflow_channel_operators AS
WITH
channel_addresses(id, worker_id, address, from_index, to_index) AS (
     SELECT id, worker_id, address, from_index, to_index
     FROM mz_internal.mz_dataflow_channels mdc
     INNER JOIN mz_internal.mz_dataflow_addresses mda
     USING (id, worker_id)
),
operator_addresses(channel_id, worker_id, from_address, to_address) AS (
     SELECT id AS channel_id, worker_id,
            address || from_index AS from_address,
            address || to_index AS to_address
     FROM channel_addresses
)
SELECT channel_id AS id, oa.worker_id, from_ops.id AS from_operator_id, to_ops.id AS to_operator_id
FROM operator_addresses oa INNER JOIN mz_internal.mz_dataflow_addresses mda_from ON oa.from_address = mda_from.address AND oa.worker_id = mda_from.worker_id
                           INNER JOIN mz_internal.mz_dataflow_operators from_ops ON mda_from.id = from_ops.id AND oa.worker_id = from_ops.worker_id
                           INNER JOIN mz_internal.mz_dataflow_addresses mda_to ON oa.to_address = mda_to.address AND oa.worker_id = mda_to.worker_id
                           INNER JOIN mz_internal.mz_dataflow_operators to_ops ON mda_to.id = to_ops.id AND oa.worker_id = to_ops.worker_id
"
};

pub const MZ_COMPUTE_IMPORT_FRONTIERS: BuiltinView = BuiltinView {
    name: "mz_compute_import_frontiers",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_compute_import_frontiers AS SELECT
    export_id, import_id, pg_catalog.min(time) AS time
FROM mz_internal.mz_worker_compute_import_frontiers
GROUP BY export_id, import_id",
};

pub const MZ_RECORDS_PER_DATAFLOW_OPERATOR: BuiltinView = BuiltinView {
    name: "mz_records_per_dataflow_operator",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_records_per_dataflow_operator AS
WITH records_cte AS (
    SELECT
        operator_id,
        worker_id,
        pg_catalog.count(*) AS records
    FROM
        mz_internal.mz_arrangement_records_internal
    GROUP BY
        operator_id, worker_id
)
SELECT
    mz_dataflow_operator_dataflows.id,
    mz_dataflow_operator_dataflows.name,
    mz_dataflow_operator_dataflows.worker_id,
    mz_dataflow_operator_dataflows.dataflow_id,
    records_cte.records
FROM
    records_cte,
    mz_internal.mz_dataflow_operator_dataflows
WHERE
    mz_dataflow_operator_dataflows.id = records_cte.operator_id AND
    mz_dataflow_operator_dataflows.worker_id = records_cte.worker_id",
};

pub const MZ_RECORDS_PER_DATAFLOW: BuiltinView = BuiltinView {
    name: "mz_records_per_dataflow",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_records_per_dataflow AS SELECT
    mz_records_per_dataflow_operator.dataflow_id as id,
    mz_dataflows.name,
    mz_records_per_dataflow_operator.worker_id,
    pg_catalog.SUM(mz_records_per_dataflow_operator.records) as records
FROM
    mz_internal.mz_records_per_dataflow_operator,
    mz_internal.mz_dataflows
WHERE
    mz_records_per_dataflow_operator.dataflow_id = mz_dataflows.id AND
    mz_records_per_dataflow_operator.worker_id = mz_dataflows.worker_id
GROUP BY
    mz_records_per_dataflow_operator.dataflow_id,
    mz_dataflows.name,
    mz_records_per_dataflow_operator.worker_id",
};

pub const MZ_RECORDS_PER_DATAFLOW_GLOBAL: BuiltinView = BuiltinView {
    name: "mz_records_per_dataflow_global",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_records_per_dataflow_global AS SELECT
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name,
    pg_catalog.SUM(mz_records_per_dataflow.records) as records
FROM
    mz_internal.mz_records_per_dataflow
GROUP BY
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name",
};

pub const PG_NAMESPACE: BuiltinView = BuiltinView {
    name: "pg_namespace",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_namespace AS SELECT
s.oid AS oid,
s.name AS nspname,
NULL::pg_catalog.oid AS nspowner,
NULL::pg_catalog.text[] AS nspacl
FROM mz_catalog.mz_schemas s
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE s.database_id IS NULL OR d.name = pg_catalog.current_database()",
};

pub const PG_CLASS: BuiltinView = BuiltinView {
    name: "pg_class",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_class AS SELECT
    class_objects.oid,
    class_objects.name AS relname,
    mz_schemas.oid AS relnamespace,
    -- MZ doesn't support typed tables so reloftype is filled with 0
    0::pg_catalog.oid AS reloftype,
    NULL::pg_catalog.oid AS relowner,
    0::pg_catalog.oid AS relam,
    -- MZ doesn't have tablespaces so reltablespace is filled in with 0 implying the default tablespace
    0::pg_catalog.oid AS reltablespace,
    -- MZ doesn't use TOAST tables so reltoastrelid is filled with 0
    0::pg_catalog.oid AS reltoastrelid,
    EXISTS (SELECT id, oid, name, on_id, cluster_id FROM mz_catalog.mz_indexes where mz_indexes.on_id = class_objects.id) AS relhasindex,
    -- MZ doesn't have unlogged tables and because of (https://github.com/MaterializeInc/materialize/issues/8805)
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
    -- MZ doesn't support CHECK constraints so relchecks is filled with 0
    0::pg_catalog.int2 AS relchecks,
    -- MZ doesn't support creating rules so relhasrules is filled with false
    false AS relhasrules,
    -- MZ doesn't support creating triggers so relhastriggers is filled with false
    false AS relhastriggers,
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
    NULL::pg_catalog.text[] as reloptions
FROM (
    -- pg_class catalogs relations and indexes
    SELECT id, oid, schema_id, name, type FROM mz_catalog.mz_relations
    UNION ALL
        SELECT mz_indexes.id, mz_indexes.oid, mz_relations.schema_id, mz_indexes.name, 'index' AS type
        FROM mz_catalog.mz_indexes
        JOIN mz_catalog.mz_relations ON mz_indexes.on_id = mz_relations.id
) AS class_objects
JOIN mz_catalog.mz_schemas ON mz_schemas.id = class_objects.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = mz_schemas.database_id
WHERE mz_schemas.database_id IS NULL OR d.name = pg_catalog.current_database()",
};

pub const PG_DATABASE: BuiltinView = BuiltinView {
    name: "pg_database",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_database AS SELECT
    oid,
    name as datname,
    NULL::pg_catalog.oid AS datdba,
    6 as encoding,
    'C' as datcollate,
    'C' as datctype,
    NULL::pg_catalog.text[] as datacl
FROM mz_catalog.mz_databases d",
};

pub const PG_INDEX: BuiltinView = BuiltinView {
    name: "pg_index",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_index AS SELECT
    mz_indexes.oid AS indexrelid,
    mz_relations.oid AS indrelid,
    false::pg_catalog.bool AS indisprimary,
    -- MZ doesn't support creating unique indexes so indisunique is filled with false
    false::pg_catalog.bool AS indisunique,
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
};

pub const PG_DESCRIPTION: BuiltinView = BuiltinView {
    name: "pg_description",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_description AS SELECT
    c.oid as objoid,
    NULL::pg_catalog.oid as classoid,
    0::pg_catalog.int4 as objsubid,
    NULL::pg_catalog.text as description
FROM pg_catalog.pg_class c",
};

pub const PG_TYPE: BuiltinView = BuiltinView {
    name: "pg_type",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_type AS SELECT
    mz_types.oid,
    mz_types.name AS typname,
    mz_schemas.oid AS typnamespace,
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
    (CASE mztype WHEN 'a' THEN 'array_in' ELSE NULL END)::pg_catalog.regproc AS typinput,
    NULL::pg_catalog.oid AS typreceive,
    false::pg_catalog.bool AS typnotnull,
    0::pg_catalog.oid AS typbasetype,
    -1::pg_catalog.int4 AS typtypmod,
    -- MZ doesn't support COLLATE so typcollation is filled with 0
    0::pg_catalog.oid AS typcollation,
    NULL::pg_catalog.text AS typdefault
FROM
    mz_catalog.mz_types
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
    WHERE mz_schemas.database_id IS NULL OR d.name = pg_catalog.current_database()",
};

pub const PG_ATTRIBUTE: BuiltinView = BuiltinView {
    name: "pg_attribute",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_attribute AS SELECT
    class_objects.oid as attrelid,
    mz_columns.name as attname,
    mz_columns.type_oid AS atttypid,
    pg_type.typlen AS attlen,
    position::int8::int2 as attnum,
    -1::pg_catalog.int4 as atttypmod,
    NOT nullable as attnotnull,
    mz_columns.default IS NOT NULL as atthasdef,
    ''::pg_catalog.\"char\" as attidentity,
    -- MZ doesn't support generated columns so attgenerated is filled with ''
    ''::pg_catalog.\"char\" as attgenerated,
    FALSE as attisdropped,
    -- MZ doesn't support COLLATE so attcollation is filled with 0
    0::pg_catalog.oid as attcollation
FROM (
    -- pg_attribute catalogs columns on relations and indexes
    SELECT id, oid, schema_id, name, type FROM mz_catalog.mz_relations
    UNION ALL
        SELECT mz_indexes.id, mz_indexes.oid, mz_relations.schema_id, mz_indexes.name, 'index' AS type
        FROM mz_catalog.mz_indexes
        JOIN mz_catalog.mz_relations ON mz_indexes.on_id = mz_relations.id
) AS class_objects
JOIN mz_catalog.mz_columns ON class_objects.id = mz_columns.id
JOIN pg_catalog.pg_type ON pg_type.oid = mz_columns.type_oid
JOIN mz_catalog.mz_schemas ON mz_schemas.id = class_objects.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = mz_schemas.database_id
WHERE mz_schemas.database_id IS NULL OR d.name = pg_catalog.current_database()",
    // Since this depends on pg_type, its id must be higher due to initialization
    // ordering.
};

pub const PG_PROC: BuiltinView = BuiltinView {
    name: "pg_proc",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_proc AS SELECT
    mz_functions.oid,
    mz_functions.name AS proname,
    mz_schemas.oid AS pronamespace,
    NULL::pg_catalog.oid AS proowner,
    NULL::pg_catalog.text AS proargdefaults,
    ret_type.oid AS prorettype
FROM mz_catalog.mz_functions
JOIN mz_catalog.mz_schemas ON mz_functions.schema_id = mz_schemas.id
LEFT JOIN mz_catalog.mz_databases d ON d.id = mz_schemas.database_id
JOIN mz_catalog.mz_types AS ret_type ON mz_functions.return_type_id = ret_type.id
WHERE mz_schemas.database_id IS NULL OR d.name = pg_catalog.current_database()",
};

pub const PG_OPERATOR: BuiltinView = BuiltinView {
    name: "pg_operator",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_operator AS SELECT
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
};

pub const PG_RANGE: BuiltinView = BuiltinView {
    name: "pg_range",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_range AS SELECT
    NULL::pg_catalog.oid AS rngtypid,
    NULL::pg_catalog.oid AS rngsubtype
WHERE false",
};

pub const PG_ENUM: BuiltinView = BuiltinView {
    name: "pg_enum",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_enum AS SELECT
    NULL::pg_catalog.oid AS oid,
    NULL::pg_catalog.oid AS enumtypid,
    NULL::pg_catalog.float4 AS enumsortorder,
    NULL::pg_catalog.text AS enumlabel
WHERE false",
};

pub const PG_ATTRDEF: BuiltinView = BuiltinView {
    name: "pg_attrdef",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_attrdef AS SELECT
    NULL::pg_catalog.oid AS oid,
    mz_objects.oid AS adrelid,
    mz_columns.position::int8 AS adnum,
    mz_columns.default AS adbin,
    mz_columns.default AS adsrc
FROM mz_catalog.mz_columns
    JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())
    JOIN mz_catalog.mz_objects ON mz_columns.id = mz_objects.id
WHERE default IS NOT NULL",
};

pub const PG_SETTINGS: BuiltinView = BuiltinView {
    name: "pg_settings",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_settings AS SELECT
    name, setting
FROM (VALUES
    ('max_index_keys'::pg_catalog.text, '1000'::pg_catalog.text)
) AS _ (name, setting)",
};

pub const PG_AUTH_MEMBERS: BuiltinView = BuiltinView {
    name: "pg_auth_members",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_auth_members AS SELECT
    NULL::pg_catalog.oid as roleid,
    NULL::pg_catalog.oid as member,
    NULL::pg_catalog.oid as grantor,
    NULL::pg_catalog.bool as admin_option
WHERE false",
};

pub const MZ_PEEK_DURATIONS_HISTOGRAM: BuiltinView = BuiltinView {
    name: "mz_peek_durations_histogram",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_peek_durations_histogram AS SELECT
    worker_id, duration_ns, pg_catalog.count(*) AS count
FROM
    mz_internal.mz_peek_durations_histogram_internal
GROUP BY
    worker_id, duration_ns",
};

pub const MZ_SCHEDULING_ELAPSED: BuiltinView = BuiltinView {
    name: "mz_scheduling_elapsed",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_scheduling_elapsed AS SELECT
    id, worker_id, pg_catalog.count(*) AS elapsed_ns
FROM
    mz_internal.mz_scheduling_elapsed_internal
GROUP BY
    id, worker_id",
};

pub const MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM: BuiltinView = BuiltinView {
    name: "mz_compute_operator_durations_histogram",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_compute_operator_durations_histogram AS SELECT
    id, worker_id, duration_ns, pg_catalog.count(*) AS count
FROM
    mz_internal.mz_compute_operator_durations_histogram_internal
GROUP BY
    id, worker_id, duration_ns",
};

pub const MZ_SCHEDULING_PARKS_HISTOGRAM: BuiltinView = BuiltinView {
    name: "mz_scheduling_parks_histogram",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_scheduling_parks_histogram AS SELECT
    worker_id, slept_for_ns, requested_ns, pg_catalog.count(*) AS count
FROM
    mz_internal.mz_scheduling_parks_histogram_internal
GROUP BY
    worker_id, slept_for_ns, requested_ns",
};

pub const MZ_WORKER_COMPUTE_DELAYS_HISTOGRAM: BuiltinView = BuiltinView {
    name: "mz_worker_compute_delays_histogram",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_worker_compute_delays_histogram AS SELECT
    export_id, import_id, worker_id, delay_ns, pg_catalog.count(*) AS count
FROM
    mz_internal.mz_worker_compute_delays_histogram_internal
GROUP BY
    export_id, import_id, worker_id, delay_ns",
};

pub const MZ_MESSAGE_COUNTS: BuiltinView = BuiltinView {
    name: "mz_message_counts",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_message_counts AS
WITH sent_cte AS (
    SELECT
        channel_id,
        from_worker_id,
        to_worker_id,
        pg_catalog.count(*) AS sent
    FROM
        mz_internal.mz_message_counts_sent_internal
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
        mz_internal.mz_message_counts_received_internal
    GROUP BY
        channel_id, from_worker_id, to_worker_id
)
SELECT
    sent_cte.channel_id,
    sent_cte.from_worker_id,
    sent_cte.to_worker_id,
    sent_cte.sent,
    received_cte.received
FROM sent_cte JOIN received_cte USING (channel_id, from_worker_id, to_worker_id)",
};

pub const MZ_DATAFLOW_OPERATOR_REACHABILITY: BuiltinView = BuiltinView {
    name: "mz_dataflow_operator_reachability",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_dataflow_operator_reachability AS SELECT
    address,
    port,
    worker_id,
    update_type,
    time,
    pg_catalog.count(*) as count
FROM
    mz_internal.mz_dataflow_operator_reachability_internal
GROUP BY address, port, worker_id, update_type, time",
};

pub const MZ_ARRANGEMENT_SIZES: BuiltinView = BuiltinView {
    name: "mz_arrangement_sizes",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_arrangement_sizes AS
WITH batches_cte AS (
    SELECT
        operator_id,
        worker_id,
        pg_catalog.count(*) AS batches
    FROM
        mz_internal.mz_arrangement_batches_internal
    GROUP BY
        operator_id, worker_id
),
records_cte AS (
    SELECT
        operator_id,
        worker_id,
        pg_catalog.count(*) AS records
    FROM
        mz_internal.mz_arrangement_records_internal
    GROUP BY
        operator_id, worker_id
)
SELECT
    batches_cte.operator_id,
    batches_cte.worker_id,
    records_cte.records,
    batches_cte.batches
FROM batches_cte JOIN records_cte USING (operator_id, worker_id)",
};

pub const MZ_ARRANGEMENT_SHARING: BuiltinView = BuiltinView {
    name: "mz_arrangement_sharing",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_arrangement_sharing AS
SELECT
    operator_id,
    worker_id,
    pg_catalog.count(*) AS count
FROM mz_internal.mz_arrangement_sharing_internal
GROUP BY operator_id, worker_id",
};

pub const MZ_CLUSTER_REPLICA_UTILIZATION: BuiltinView = BuiltinView {
    name: "mz_cluster_replica_utilization",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_cluster_replica_utilization AS
SELECT
    r.id AS replica_id,
    m.process_id,
    m.cpu_nano_cores::float8 / s.cpu_nano_cores * 100 AS cpu_percent,
    m.memory_bytes::float8 / s.memory_bytes * 100 AS memory_percent
FROM
    mz_cluster_replicas AS r
        JOIN mz_internal.mz_cluster_replica_sizes AS s ON r.size = s.size
        JOIN mz_internal.mz_cluster_replica_metrics AS m ON m.replica_id = r.id",
};

pub const MZ_DATAFLOW_OPERATOR_PARENTS: BuiltinView = BuiltinView {
    name: "mz_dataflow_operator_parents",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_operator_parents AS
WITH operator_addrs AS(
    SELECT
        id, address, worker_id
    FROM mz_internal.mz_dataflow_addresses
        INNER JOIN mz_internal.mz_dataflow_operators
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
};

// NOTE: If you add real data to this implementation, then please update
// the related `pg_` function implementations (like `pg_get_constraintdef`)
pub const PG_CONSTRAINT: BuiltinView = BuiltinView {
    name: "pg_constraint",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_constraint AS SELECT
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
};

pub const PG_TABLES: BuiltinView = BuiltinView {
    name: "pg_tables",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_tables AS
SELECT n.nspname AS schemaname,
    c.relname AS tablename,
    pg_catalog.pg_get_userbyid(c.relowner) AS tableowner
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p')",
};

pub const PG_ACCESS_METHODS: BuiltinView = BuiltinView {
    name: "pg_am",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_am AS
SELECT NULL::pg_catalog.oid AS oid,
    NULL::pg_catalog.text AS amname,
    NULL::pg_catalog.regproc AS amhandler,
    NULL::pg_catalog.\"char\" AS amtype
WHERE false",
};

pub const PG_ROLES: BuiltinView = BuiltinView {
    name: "pg_roles",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_roles AS SELECT
    r.rolname,
    r.rolsuper,
    r.rolinherit,
    r.rolcreaterole,
    r.rolcreatedb,
    r.rolcanlogin,
    r.rolreplication,
    r.rolconnlimit,
    '********'::pg_catalog.text AS rolpassword,
    r.rolvaliduntil,
    r.rolbypassrls,
    r.oid AS oid
FROM pg_catalog.pg_authid r",
};

pub const PG_VIEWS: BuiltinView = BuiltinView {
    name: "pg_views",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_views AS SELECT
    s.name AS schemaname,
    v.name AS viewname,
    NULL::pg_catalog.oid AS viewowner,
    v.definition AS definition
FROM mz_catalog.mz_views v
LEFT JOIN mz_catalog.mz_schemas s ON s.id = v.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE s.database_id IS NULL OR d.name = current_database()",
};

pub const PG_MATVIEWS: BuiltinView = BuiltinView {
    name: "pg_matviews",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_matviews AS SELECT
    s.name AS schemaname,
    m.name AS matviewname,
    NULL::pg_catalog.oid AS matviewowner,
    m.definition AS definition
FROM mz_catalog.mz_materialized_views m
LEFT JOIN mz_catalog.mz_schemas s ON s.id = m.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE s.database_id IS NULL OR d.name = current_database()",
};

pub const INFORMATION_SCHEMA_COLUMNS: BuiltinView = BuiltinView {
    name: "columns",
    schema: INFORMATION_SCHEMA,
    sql: "CREATE VIEW information_schema.columns AS
SELECT
    current_database() as table_catalog,
    s.name AS table_schema,
    o.name AS table_name,
    c.name AS column_name,
    c.position::int8 AS ordinal_position,
    c.type AS data_type,
    NULL::pg_catalog.int4 AS character_maximum_length,
    NULL::pg_catalog.int4 AS numeric_precision,
    NULL::pg_catalog.int4 AS numeric_scale
FROM mz_catalog.mz_columns c
JOIN mz_catalog.mz_objects o ON o.id = c.id
JOIN mz_catalog.mz_schemas s ON s.id = o.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE s.database_id IS NULL OR d.name = current_database()",
};

pub const INFORMATION_SCHEMA_TABLES: BuiltinView = BuiltinView {
    name: "tables",
    schema: INFORMATION_SCHEMA,
    sql: "CREATE VIEW information_schema.tables AS SELECT
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
};

// MZ doesn't support COLLATE so the table is filled with NULLs and made empty. pg_database hard
// codes a collation of 'C' for every database, so we could copy that here.
pub const PG_COLLATION: BuiltinView = BuiltinView {
    name: "pg_collation",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_collation
AS SELECT
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
};

// MZ doesn't support row level security policies so the table is filled in with NULLs and made empty.
pub const PG_POLICY: BuiltinView = BuiltinView {
    name: "pg_policy",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_policy
AS SELECT
    NULL::pg_catalog.oid AS oid,
    NULL::pg_catalog.text AS polname,
    NULL::pg_catalog.oid AS polrelid,
    NULL::pg_catalog.\"char\" AS polcmd,
    NULL::pg_catalog.bool AS polpermissive,
    NULL::pg_catalog.oid[] AS polroles,
    NULL::pg_catalog.text AS polqual,
    NULL::pg_catalog.text AS polwithcheck
WHERE false",
};

// MZ doesn't support table inheritance so the table is filled in with NULLs and made empty.
pub const PG_INHERITS: BuiltinView = BuiltinView {
    name: "pg_inherits",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_inherits
AS SELECT
    NULL::pg_catalog.oid AS inhrelid,
    NULL::pg_catalog.oid AS inhparent,
    NULL::pg_catalog.int4 AS inhseqno,
    NULL::pg_catalog.bool AS inhdetachpending
WHERE false",
};

pub const PG_AUTHID: BuiltinView = BuiltinView {
    name: "pg_authid",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_authid
AS SELECT
    r.oid AS oid,
    r.name AS rolname,
    -- We determine superuser status each time a role logs in, so there's no way to accurately
    -- depict this in the catalog. Except for mz_system, which is always a superuser. For all other
    -- roles we hardcode NULL.
    CASE
        WHEN r.name = 'mz_system' THEN true
        ELSE NULL::pg_catalog.bool
    END AS rolsuper,
    inherit AS rolinherit,
    create_role AS rolcreaterole,
    create_db AS rolcreatedb,
    -- We determine login status each time a role logs in, so there's no way to accurately depict
    -- this in the catalog. Instead we just hardcode NULL.
    NULL::pg_catalog.bool AS rolcanlogin,
    -- MZ doesn't support replication in the same way Postgres does
    false AS rolreplication,
    -- MZ doesn't how row level security
    false AS rolbypassrls,
    -- MZ doesn't have a connection limit
    -1 AS rolconnlimit,
    -- MZ doesn't have role passwords
    NULL::pg_catalog.text AS rolpassword,
    -- MZ doesn't have role passwords
    NULL::pg_catalog.timestamptz AS rolvaliduntil
FROM mz_catalog.mz_roles r",
};

pub const MZ_SHOW_MATERIALIZED_VIEWS: BuiltinView = BuiltinView {
    name: "mz_show_materialized_views",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_show_materialized_views
AS SELECT mviews.name, clusters.name AS cluster, schema_id, cluster_id
FROM mz_materialized_views AS mviews
JOIN mz_clusters AS clusters ON clusters.id = mviews.cluster_id",
};

pub const MZ_SHOW_INDEXES: BuiltinView = BuiltinView {
    name: "mz_show_indexes",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE VIEW mz_internal.mz_show_indexes
AS SELECT
    idxs.name AS name,
    objs.name AS on,
    clusters.name AS cluster,
    COALESCE(keys.key, '{}'::_text) AS key,
    idxs.on_id AS on_id,
    objs.schema_id AS schema_id,
    clusters.id AS cluster_id
FROM
    mz_indexes AS idxs
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
            mz_indexes AS idxs
            JOIN mz_index_columns idx_cols ON idxs.id = idx_cols.index_id
            LEFT JOIN mz_columns obj_cols ON
                idxs.on_id = obj_cols.id AND idx_cols.on_position = obj_cols.position
        GROUP BY idxs.id) AS keys
    ON idxs.id = keys.id",
};

pub const MZ_SHOW_CLUSTER_REPLICAS: BuiltinView = BuiltinView {
    name: "mz_show_cluster_replicas",
    schema: MZ_INTERNAL_SCHEMA,
    sql: r#"CREATE VIEW mz_internal.mz_show_cluster_replicas
AS SELECT
    mz_catalog.mz_clusters.name AS cluster,
    mz_catalog.mz_cluster_replicas.name AS replica,
    mz_catalog.mz_cluster_replicas.size AS size,
    statuses.ready AS ready
FROM
    mz_catalog.mz_cluster_replicas
        JOIN mz_catalog.mz_clusters
            ON mz_catalog.mz_cluster_replicas.cluster_id = mz_catalog.mz_clusters.id
        JOIN
            (
                SELECT
                    replica_id,
                    mz_internal.mz_all(status = 'ready') AS ready
                FROM mz_internal.mz_cluster_replica_statuses
                GROUP BY replica_id
            ) AS statuses
            ON mz_catalog.mz_cluster_replicas.id = statuses.replica_id
ORDER BY 1, 2"#,
};

pub const MZ_SHOW_DATABASES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_databases_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_show_databases_ind
IN CLUSTER mz_introspection
ON mz_catalog.mz_databases (name)",
};

pub const MZ_SHOW_SCHEMAS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_schemas_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_show_schemas_ind
IN CLUSTER mz_introspection
ON mz_catalog.mz_schemas (database_id)",
};

pub const MZ_SHOW_CONNECTIONS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_connections_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_show_connections_ind
IN CLUSTER mz_introspection
ON mz_catalog.mz_connections (schema_id)",
};

pub const MZ_SHOW_TABLES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_tables_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_show_tables_ind
IN CLUSTER mz_introspection
ON mz_catalog.mz_tables (schema_id)",
};

pub const MZ_SHOW_SOURCES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_sources_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_show_sources_ind
IN CLUSTER mz_introspection
ON mz_catalog.mz_sources (schema_id)",
};

pub const MZ_SHOW_VIEWS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_views_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_show_views_ind
IN CLUSTER mz_introspection
ON mz_catalog.mz_views (schema_id)",
};

pub const MZ_SHOW_MATERIALIZED_VIEWS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_materialized_views_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_show_materialized_views_ind
IN CLUSTER mz_introspection
ON mz_internal.mz_show_materialized_views (schema_id, cluster_id)",
};

pub const MZ_SHOW_SINKS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_sinks_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_show_sinks_ind
IN CLUSTER mz_introspection
ON mz_catalog.mz_sinks (schema_id)",
};

pub const MZ_SHOW_TYPES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_types_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_show_types_ind
IN CLUSTER mz_introspection
ON mz_catalog.mz_types (schema_id)",
};

pub const MZ_SHOW_ALL_OBJECTS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_all_objects_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_show_all_objects_ind
IN CLUSTER mz_introspection
ON mz_catalog.mz_objects (schema_id)",
};

pub const MZ_SHOW_INDEXES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_indexes_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_show_indexes_ind
IN CLUSTER mz_introspection
ON mz_internal.mz_show_indexes (on_id, schema_id, cluster_id)",
};

pub const MZ_SHOW_COLUMNS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_columns_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_show_columns_ind
IN CLUSTER mz_introspection
ON mz_catalog.mz_columns (id)",
};

pub const MZ_SHOW_CLUSTERS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_clusters_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_show_clusters_ind
IN CLUSTER mz_introspection
ON mz_catalog.mz_clusters (name)",
};

pub const MZ_SHOW_CLUSTER_REPLICAS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_cluster_replicas_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_show_cluster_replicas_ind
IN CLUSTER mz_introspection
ON mz_internal.mz_show_cluster_replicas (cluster, replica, size, ready)",
};

pub const MZ_SHOW_SECRETS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_show_secrets_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_show_secrets_ind
IN CLUSTER mz_introspection
ON mz_catalog.mz_secrets (schema_id)",
};

pub const MZ_CLUSTERS_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_clusters_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_clusters_ind
IN CLUSTER mz_introspection
ON mz_catalog.mz_clusters (id)",
};

pub const MZ_SOURCE_STATUSES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_source_statuses_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_source_statuses_ind
IN CLUSTER mz_introspection
ON mz_internal.mz_source_statuses (id)",
};

pub const MZ_SOURCE_STATUS_HISTORY_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_source_status_history_ind",
    schema: MZ_INTERNAL_SCHEMA,
    sql: "CREATE INDEX mz_source_status_history_ind
IN CLUSTER mz_introspection
ON mz_internal.mz_source_status_history (source_id)",
};

pub static MZ_SYSTEM_ROLE: Lazy<BuiltinRole> = Lazy::new(|| BuiltinRole {
    name: &*SYSTEM_USER.name,
    attributes: RoleAttributes::new().with_all(),
});

pub static MZ_INTROSPECTION_ROLE: Lazy<BuiltinRole> = Lazy::new(|| BuiltinRole {
    name: &*INTROSPECTION_USER.name,
    attributes: RoleAttributes::new(),
});

pub static MZ_SYSTEM_CLUSTER: Lazy<BuiltinCluster> = Lazy::new(|| BuiltinCluster {
    name: &*SYSTEM_USER.name,
});

pub static MZ_SYSTEM_CLUSTER_REPLICA: Lazy<BuiltinClusterReplica> =
    Lazy::new(|| BuiltinClusterReplica {
        name: DEFAULT_CLUSTER_REPLICA_NAME,
        cluster_name: MZ_SYSTEM_CLUSTER.name,
    });

pub static MZ_INTROSPECTION_CLUSTER: Lazy<BuiltinCluster> = Lazy::new(|| BuiltinCluster {
    name: &*INTROSPECTION_USER.name,
});

pub static MZ_INTROSPECTION_CLUSTER_REPLICA: Lazy<BuiltinClusterReplica> =
    Lazy::new(|| BuiltinClusterReplica {
        name: DEFAULT_CLUSTER_REPLICA_NAME,
        cluster_name: MZ_INTROSPECTION_CLUSTER.name,
    });

/// List of all builtin objects sorted topologically by dependency.
pub static BUILTINS_STATIC: Lazy<Vec<Builtin<NameReference>>> = Lazy::new(|| {
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
    ];
    for (schema, funcs) in &[
        (PG_CATALOG_SCHEMA, &*mz_sql::func::PG_CATALOG_BUILTINS),
        (
            INFORMATION_SCHEMA,
            &*mz_sql::func::INFORMATION_SCHEMA_BUILTINS,
        ),
        (MZ_CATALOG_SCHEMA, &*mz_sql::func::MZ_CATALOG_BUILTINS),
        (MZ_INTERNAL_SCHEMA, &*mz_sql::func::MZ_INTERNAL_BUILTINS),
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
        Builtin::Log(&MZ_ARRANGEMENT_SHARING_INTERNAL),
        Builtin::Log(&MZ_ARRANGEMENT_BATCHES_INTERNAL),
        Builtin::Log(&MZ_ARRANGEMENT_RECORDS_INTERNAL),
        Builtin::Log(&MZ_DATAFLOW_CHANNELS),
        Builtin::Log(&MZ_DATAFLOW_OPERATORS),
        Builtin::Log(&MZ_DATAFLOW_OPERATORS_ADDRESSES),
        Builtin::Log(&MZ_DATAFLOW_OPERATOR_REACHABILITY_INTERNAL),
        Builtin::Log(&MZ_COMPUTE_EXPORTS),
        Builtin::Log(&MZ_WORKER_COMPUTE_DEPENDENCIES),
        Builtin::Log(&MZ_MESSAGE_COUNTS_RECEIVED_INTERNAL),
        Builtin::Log(&MZ_MESSAGE_COUNTS_SENT_INTERNAL),
        Builtin::Log(&MZ_ACTIVE_PEEKS),
        Builtin::Log(&MZ_PEEK_DURATIONS_HISTOGRAM_INTERNAL),
        Builtin::Log(&MZ_SCHEDULING_ELAPSED_INTERNAL),
        Builtin::Log(&MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM_INTERNAL),
        Builtin::Log(&MZ_SCHEDULING_PARKS_HISTOGRAM_INTERNAL),
        Builtin::Log(&MZ_WORKER_COMPUTE_FRONTIERS),
        Builtin::Log(&MZ_WORKER_COMPUTE_IMPORT_FRONTIERS),
        Builtin::Log(&MZ_WORKER_COMPUTE_DELAYS_HISTOGRAM_INTERNAL),
        Builtin::Table(&MZ_VIEW_KEYS),
        Builtin::Table(&MZ_VIEW_FOREIGN_KEYS),
        Builtin::Table(&MZ_KAFKA_SINKS),
        Builtin::Table(&MZ_KAFKA_CONNECTIONS),
        Builtin::Table(&MZ_OBJECT_DEPENDENCIES),
        Builtin::Table(&MZ_DATABASES),
        Builtin::Table(&MZ_SCHEMAS),
        Builtin::Table(&MZ_COLUMNS),
        Builtin::Table(&MZ_INDEXES),
        Builtin::Table(&MZ_INDEX_COLUMNS),
        Builtin::Table(&MZ_TABLES),
        Builtin::Table(&MZ_SOURCES),
        Builtin::Table(&MZ_POSTGRES_SOURCES),
        Builtin::Table(&MZ_SINKS),
        Builtin::Table(&MZ_VIEWS),
        Builtin::Table(&MZ_MATERIALIZED_VIEWS),
        Builtin::Table(&MZ_TYPES),
        Builtin::Table(&MZ_ARRAY_TYPES),
        Builtin::Table(&MZ_BASE_TYPES),
        Builtin::Table(&MZ_LIST_TYPES),
        Builtin::Table(&MZ_MAP_TYPES),
        Builtin::Table(&MZ_ROLES),
        Builtin::Table(&MZ_ROLE_MEMBERS),
        Builtin::Table(&MZ_PSEUDO_TYPES),
        Builtin::Table(&MZ_FUNCTIONS),
        Builtin::Table(&MZ_OPERATORS),
        Builtin::Table(&MZ_CLUSTERS),
        Builtin::Table(&MZ_CLUSTER_LINKS),
        Builtin::Table(&MZ_SECRETS),
        Builtin::Table(&MZ_CONNECTIONS),
        Builtin::Table(&MZ_SSH_TUNNEL_CONNECTIONS),
        Builtin::Table(&MZ_CLUSTER_REPLICAS),
        Builtin::Table(&MZ_CLUSTER_REPLICA_FRONTIERS),
        Builtin::Table(&MZ_CLUSTER_REPLICA_METRICS),
        Builtin::Table(&MZ_CLUSTER_REPLICA_SIZES),
        Builtin::Table(&MZ_CLUSTER_REPLICA_STATUSES),
        Builtin::Table(&MZ_CLUSTER_REPLICA_HEARTBEATS),
        Builtin::Table(&MZ_AUDIT_EVENTS),
        Builtin::Table(&MZ_STORAGE_USAGE_BY_SHARD),
        Builtin::Table(&MZ_EGRESS_IPS),
        Builtin::Table(&MZ_AWS_PRIVATELINK_CONNECTIONS),
        Builtin::Table(&MZ_SUBSCRIPTIONS),
        Builtin::View(&MZ_RELATIONS),
        Builtin::View(&MZ_OBJECTS),
        Builtin::View(&MZ_ARRANGEMENT_SHARING),
        Builtin::View(&MZ_ARRANGEMENT_SIZES),
        Builtin::View(&MZ_DATAFLOWS),
        Builtin::View(&MZ_DATAFLOW_OPERATOR_DATAFLOWS),
        Builtin::View(&MZ_DATAFLOW_OPERATOR_REACHABILITY),
        Builtin::View(&MZ_CLUSTER_REPLICA_UTILIZATION),
        Builtin::View(&MZ_DATAFLOW_OPERATOR_PARENTS),
        Builtin::View(&MZ_COMPUTE_FRONTIERS),
        Builtin::View(&MZ_DATAFLOW_CHANNEL_OPERATORS),
        Builtin::View(&MZ_COMPUTE_IMPORT_FRONTIERS),
        Builtin::View(&MZ_MESSAGE_COUNTS),
        Builtin::View(&MZ_COMPUTE_OPERATOR_DURATIONS_HISTOGRAM),
        Builtin::View(&MZ_RECORDS_PER_DATAFLOW_OPERATOR),
        Builtin::View(&MZ_RECORDS_PER_DATAFLOW),
        Builtin::View(&MZ_RECORDS_PER_DATAFLOW_GLOBAL),
        Builtin::View(&MZ_PEEK_DURATIONS_HISTOGRAM),
        Builtin::View(&MZ_SCHEDULING_ELAPSED),
        Builtin::View(&MZ_SCHEDULING_PARKS_HISTOGRAM),
        Builtin::View(&MZ_WORKER_COMPUTE_DELAYS_HISTOGRAM),
        Builtin::View(&MZ_SHOW_MATERIALIZED_VIEWS),
        Builtin::View(&MZ_SHOW_INDEXES),
        Builtin::View(&MZ_SHOW_CLUSTER_REPLICAS),
        Builtin::View(&PG_NAMESPACE),
        Builtin::View(&PG_CLASS),
        Builtin::View(&PG_DATABASE),
        Builtin::View(&PG_INDEX),
        Builtin::View(&PG_DESCRIPTION),
        Builtin::View(&PG_TYPE),
        Builtin::View(&PG_ATTRIBUTE),
        Builtin::View(&PG_PROC),
        Builtin::View(&PG_OPERATOR),
        Builtin::View(&PG_RANGE),
        Builtin::View(&PG_ENUM),
        Builtin::View(&PG_ATTRDEF),
        Builtin::View(&PG_SETTINGS),
        Builtin::View(&PG_AUTH_MEMBERS),
        Builtin::View(&PG_CONSTRAINT),
        Builtin::View(&PG_TABLES),
        Builtin::View(&PG_ACCESS_METHODS),
        Builtin::View(&PG_AUTHID),
        Builtin::View(&PG_ROLES),
        Builtin::View(&PG_VIEWS),
        Builtin::View(&PG_MATVIEWS),
        Builtin::View(&PG_COLLATION),
        Builtin::View(&PG_POLICY),
        Builtin::View(&PG_INHERITS),
        Builtin::View(&INFORMATION_SCHEMA_COLUMNS),
        Builtin::View(&INFORMATION_SCHEMA_TABLES),
        Builtin::Source(&MZ_SINK_STATUS_HISTORY),
        Builtin::View(&MZ_SINK_STATUSES),
        Builtin::Source(&MZ_SOURCE_STATUS_HISTORY),
        Builtin::View(&MZ_SOURCE_STATUSES),
        Builtin::Source(&MZ_STORAGE_SHARDS),
        Builtin::Source(&MZ_SOURCE_STATISTICS),
        Builtin::Source(&MZ_SINK_STATISTICS),
        Builtin::View(&MZ_STORAGE_USAGE),
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
        Builtin::Index(&MZ_CLUSTERS_IND),
        Builtin::Index(&MZ_SOURCE_STATUSES_IND),
        Builtin::Index(&MZ_SOURCE_STATUS_HISTORY_IND),
    ]);

    builtins
});
pub static BUILTIN_ROLES: Lazy<Vec<&BuiltinRole>> =
    Lazy::new(|| vec![&*MZ_SYSTEM_ROLE, &*MZ_INTROSPECTION_ROLE]);
pub static BUILTIN_CLUSTERS: Lazy<Vec<&BuiltinCluster>> =
    Lazy::new(|| vec![&*MZ_SYSTEM_CLUSTER, &*MZ_INTROSPECTION_CLUSTER]);
pub static BUILTIN_CLUSTER_REPLICAS: Lazy<Vec<&BuiltinClusterReplica>> = Lazy::new(|| {
    vec![
        &*MZ_SYSTEM_CLUSTER_REPLICA,
        &*MZ_INTROSPECTION_CLUSTER_REPLICA,
    ]
});

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

    pub fn iter() -> impl Iterator<Item = &'static Builtin<NameReference>> {
        BUILTINS_STATIC.iter()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::env;

    use tokio_postgres::NoTls;

    use mz_ore::now::{NOW_ZERO, SYSTEM_TIME};
    use mz_ore::task;
    use mz_pgrepr::oid::{FIRST_MATERIALIZE_OID, FIRST_UNPINNED_OID};
    use mz_sql::catalog::{CatalogSchema, SessionCatalog};
    use mz_sql::func::OP_IMPLS;
    use mz_sql::names::{PartialObjectName, ResolvedDatabaseSpecifier};

    use crate::catalog::{Catalog, CatalogItem, SYSTEM_CONN_ID};

    use super::*;

    // Connect to a running Postgres server and verify that our builtin
    // types and functions match it, in addition to some other things.
    #[tokio::test]
    async fn test_compare_builtins_postgres() {
        async fn inner(catalog: Catalog) {
            // Verify that all builtin functions:
            // - have a unique OID
            // - if they have a postgres counterpart (same oid) then they have matching name
            let (client, connection) = tokio_postgres::connect(
                &env::var("POSTGRES_URL").unwrap_or_else(|_| "host=localhost user=postgres".into()),
                NoTls,
            )
            .await
            .expect("failed to connect to Postgres");

            task::spawn(|| "compare_builtin_postgres", async move {
                if let Err(e) = connection.await {
                    panic!("connection error: {}", e);
                }
            });

            struct PgProc {
                name: String,
                arg_oids: Vec<u32>,
                ret_oid: Option<u32>,
                ret_set: bool,
            }

            struct PgType {
                name: String,
                ty: String,
                elem: u32,
                array: u32,
            }

            struct PgOper {
                oprresult: u32,
                name: String,
            }

            let pg_proc: BTreeMap<_, _> = client
                .query(
                    "SELECT
                    p.oid,
                    proname,
                    proargtypes,
                    prorettype,
                    proretset
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid",
                    &[],
                )
                .await
                .expect("pg query failed")
                .into_iter()
                .map(|row| {
                    let oid: u32 = row.get("oid");
                    let pg_proc = PgProc {
                        name: row.get("proname"),
                        arg_oids: row.get("proargtypes"),
                        ret_oid: row.get("prorettype"),
                        ret_set: row.get("proretset"),
                    };
                    (oid, pg_proc)
                })
                .collect();

            let pg_type: BTreeMap<_, _> = client
                .query(
                    "SELECT oid, typname, typtype::text, typelem, typarray FROM pg_type",
                    &[],
                )
                .await
                .expect("pg query failed")
                .into_iter()
                .map(|row| {
                    let oid: u32 = row.get("oid");
                    let pg_type = PgType {
                        name: row.get("typname"),
                        ty: row.get("typtype"),
                        elem: row.get("typelem"),
                        array: row.get("typarray"),
                    };
                    (oid, pg_type)
                })
                .collect();

            let pg_oper: BTreeMap<_, _> = client
                .query("SELECT oid, oprname, oprresult FROM pg_operator", &[])
                .await
                .expect("pg query failed")
                .into_iter()
                .map(|row| {
                    let oid: u32 = row.get("oid");
                    let pg_oper = PgOper {
                        name: row.get("oprname"),
                        oprresult: row.get("oprresult"),
                    };
                    (oid, pg_oper)
                })
                .collect();

            let conn_catalog = catalog.for_system_session();
            let resolve_type_oid = |item: &str| {
                conn_catalog
                    .resolve_item(&PartialObjectName {
                        database: None,
                        // All functions we check exist in PG, so the types must, as
                        // well
                        schema: Some(PG_CATALOG_SCHEMA.into()),
                        item: item.to_string(),
                    })
                    .expect("unable to resolve type")
                    .oid()
            };

            let mut all_oids = BTreeSet::new();

            for builtin in BUILTINS::iter() {
                match builtin {
                    Builtin::Type(ty) => {
                        assert!(all_oids.insert(ty.oid), "{} reused oid {}", ty.name, ty.oid);

                        if ty.oid >= FIRST_MATERIALIZE_OID {
                            // High OIDs are reserved in Materialize and don't have
                            // PostgreSQL counterparts.
                            continue;
                        }

                        // For types that have a PostgreSQL counterpart, verify that
                        // the name and oid match.
                        let pg_ty = pg_type.get(&ty.oid).unwrap_or_else(|| {
                            panic!("pg_proc missing type {}: oid {}", ty.name, ty.oid)
                        });
                        assert_eq!(
                            ty.name, pg_ty.name,
                            "oid {} has name {} in postgres; expected {}",
                            ty.oid, pg_ty.name, ty.name,
                        );

                        // Ensure the type matches.
                        match &ty.details.typ {
                            CatalogType::Array { element_reference } => {
                                let elem_ty = BUILTINS::iter()
                                    .filter_map(|builtin| match builtin {
                                        Builtin::Type(ty @ BuiltinType { name, .. })
                                            if element_reference == name =>
                                        {
                                            Some(ty)
                                        }
                                        _ => None,
                                    })
                                    .next();
                                let elem_ty = match elem_ty {
                                    Some(ty) => ty,
                                    None => {
                                        panic!("{} is unexpectedly not a type", element_reference)
                                    }
                                };
                                assert_eq!(
                                    pg_ty.elem, elem_ty.oid,
                                    "type {} has mismatched element OIDs",
                                    ty.name
                                )
                            }
                            CatalogType::Pseudo => {
                                assert_eq!(
                                    pg_ty.ty, "p",
                                    "type {} is not a pseudo type as expected",
                                    ty.name
                                )
                            }
                            CatalogType::Range { .. } => {
                                assert_eq!(
                                    pg_ty.ty, "r",
                                    "type {} is not a range type as expected",
                                    ty.name
                                );
                            }
                            _ => {
                                assert_eq!(
                                    pg_ty.ty, "b",
                                    "type {} is not a base type as expected",
                                    ty.name
                                )
                            }
                        }

                        // Ensure the array type reference is correct.
                        let schema = catalog
                            .resolve_schema_in_database(
                                &ResolvedDatabaseSpecifier::Ambient,
                                ty.schema,
                                SYSTEM_CONN_ID,
                            )
                            .expect("unable to resolve schema");
                        let allocated_type = catalog
                            .resolve_entry(
                                None,
                                &vec![(ResolvedDatabaseSpecifier::Ambient, schema.id().clone())],
                                &PartialObjectName {
                                    database: None,
                                    schema: Some(schema.name().schema.clone()),
                                    item: ty.name.to_string(),
                                },
                                SYSTEM_CONN_ID,
                            )
                            .expect("unable to resolve type");
                        let ty = if let CatalogItem::Type(ty) = &allocated_type.item {
                            ty
                        } else {
                            panic!("unexpectedly not a type")
                        };
                        match ty.details.array_id {
                            Some(array_id) => {
                                let array_ty = catalog.get_entry(&array_id);
                                assert_eq!(
                                    pg_ty.array, array_ty.oid,
                                    "type {} has mismatched array OIDs",
                                    allocated_type.name.item,
                                );
                            }
                            None => assert_eq!(
                                pg_ty.array, 0,
                                "type {} does not have an array type in mz but does in pg",
                                allocated_type.name.item,
                            ),
                        }
                    }
                    Builtin::Func(func) => {
                        for imp in func.inner.func_impls() {
                            assert!(
                                all_oids.insert(imp.oid),
                                "{} reused oid {}",
                                func.name,
                                imp.oid
                            );

                            // For functions that have a postgres counterpart, verify that the name and
                            // oid match.
                            let pg_fn = if imp.oid >= FIRST_UNPINNED_OID {
                                continue;
                            } else {
                                pg_proc.get(&imp.oid).unwrap_or_else(|| {
                                    panic!(
                                        "pg_proc missing function {}: oid {}",
                                        func.name, imp.oid
                                    )
                                })
                            };
                            assert_eq!(
                                func.name, pg_fn.name,
                                "funcs with oid {} don't match names: {} in mz, {} in pg",
                                imp.oid, func.name, pg_fn.name
                            );

                            // Complain, but don't fail, if argument oids don't match.
                            // TODO: make these match.
                            let imp_arg_oids = imp
                                .arg_typs
                                .iter()
                                .map(|item| resolve_type_oid(item))
                                .collect::<Vec<_>>();

                            if imp_arg_oids != pg_fn.arg_oids {
                                println!(
                                "funcs with oid {} ({}) don't match arguments: {:?} in mz, {:?} in pg",
                                imp.oid, func.name, imp_arg_oids, pg_fn.arg_oids
                            );
                            }

                            let imp_return_oid = imp.return_typ.map(|item| resolve_type_oid(item));

                            if imp_return_oid != pg_fn.ret_oid {
                                println!(
                                "funcs with oid {} ({}) don't match return types: {:?} in mz, {:?} in pg",
                                imp.oid, func.name, imp_return_oid, pg_fn.ret_oid
                            );
                            }

                            if imp.return_is_set != pg_fn.ret_set {
                                panic!(
                                "funcs with oid {} ({}) don't match set-returning value: {:?} in mz, {:?} in pg",
                                imp.oid, func.name, imp.return_is_set, pg_fn.ret_set
                            );
                            }
                        }
                    }
                    _ => (),
                }
            }

            for (op, func) in OP_IMPLS.iter() {
                for imp in func.func_impls() {
                    assert!(all_oids.insert(imp.oid), "{} reused oid {}", op, imp.oid);

                    // For operators that have a postgres counterpart, verify that the name and oid match.
                    let pg_op = if imp.oid >= FIRST_UNPINNED_OID {
                        continue;
                    } else {
                        pg_oper.get(&imp.oid).unwrap_or_else(|| {
                            panic!("pg_operator missing operator {}: oid {}", op, imp.oid)
                        })
                    };

                    assert_eq!(*op, pg_op.name);

                    let imp_return_oid = imp
                        .return_typ
                        .map(|item| resolve_type_oid(item))
                        .expect("must have oid");
                    if imp_return_oid != pg_op.oprresult {
                        println!(
                            "operators with oid {} ({}) don't match return typs: {} in mz, {} in pg",
                            imp.oid,
                            op,
                            imp_return_oid,
                            pg_op.oprresult
                        );
                    }
                }
            }
        }

        Catalog::with_debug(NOW_ZERO.clone(), inner).await
    }

    // Make sure pg views don't use types that only exist in Materialize.
    #[tokio::test]
    async fn test_pg_views_forbidden_types() {
        Catalog::with_debug(SYSTEM_TIME.clone(), |catalog| async move {
            let conn_catalog = catalog.for_system_session();

            for view in BUILTINS::views().filter(|view| {
                view.schema == PG_CATALOG_SCHEMA || view.schema == INFORMATION_SCHEMA
            }) {
                let item = conn_catalog
                    .resolve_item(&PartialObjectName {
                        database: None,
                        schema: Some(view.schema.to_string()),
                        item: view.name.to_string(),
                    })
                    .expect("unable to resolve view");
                let full_name = conn_catalog.resolve_full_name(item.name());
                for col_type in item
                    .desc(&full_name)
                    .expect("invalid item type")
                    .iter_types()
                {
                    match &col_type.scalar_type {
                        typ @ ScalarType::UInt16
                        | typ @ ScalarType::UInt32
                        | typ @ ScalarType::UInt64
                        | typ @ ScalarType::MzTimestamp
                        | typ @ ScalarType::List { .. }
                        | typ @ ScalarType::Map { .. } => {
                            panic!("{typ:?} type found in {full_name}");
                        }
                        ScalarType::Bool
                        | ScalarType::Int16
                        | ScalarType::Int32
                        | ScalarType::Int64
                        | ScalarType::Float32
                        | ScalarType::Float64
                        | ScalarType::Numeric { .. }
                        | ScalarType::Date
                        | ScalarType::Time
                        | ScalarType::Timestamp
                        | ScalarType::TimestampTz
                        | ScalarType::Interval
                        | ScalarType::PgLegacyChar
                        | ScalarType::Bytes
                        | ScalarType::String
                        | ScalarType::Char { .. }
                        | ScalarType::VarChar { .. }
                        | ScalarType::Jsonb
                        | ScalarType::Uuid
                        | ScalarType::Array(_)
                        | ScalarType::Record { .. }
                        | ScalarType::Oid
                        | ScalarType::RegProc
                        | ScalarType::RegType
                        | ScalarType::RegClass
                        | ScalarType::Int2Vector
                        | ScalarType::Range { .. } => {}
                    }
                }
            }
        })
        .await
    }
}
