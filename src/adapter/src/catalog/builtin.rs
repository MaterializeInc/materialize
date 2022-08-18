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
//! <https://materialize.com/docs/sql/system-tables/>.

use std::hash::Hash;

use differential_dataflow::Hashable;
use once_cell::sync::Lazy;
use serde::Serialize;

use mz_compute_client::logging::{ComputeLog, DifferentialLog, LogVariant, TimelyLog};
use mz_repr::{RelationDesc, ScalarType};
use mz_sql::catalog::{CatalogType, CatalogTypeDetails, NameReference, TypeReference};

use crate::catalog::SYSTEM_USER;

pub const MZ_TEMP_SCHEMA: &str = "mz_temp";
pub const MZ_CATALOG_SCHEMA: &str = "mz_catalog";
pub const PG_CATALOG_SCHEMA: &str = "pg_catalog";
pub const MZ_INTERNAL_SCHEMA: &str = "mz_internal";
pub const INFORMATION_SCHEMA: &str = "information_schema";

pub enum Builtin<T: 'static + TypeReference> {
    Log(&'static BuiltinLog),
    Table(&'static BuiltinTable),
    View(&'static BuiltinView),
    Type(&'static BuiltinType<T>),
    Func(BuiltinFunc),
    StorageCollection(&'static BuiltinStorageCollection),
}

impl<T: TypeReference> Builtin<T> {
    pub fn name(&self) -> &'static str {
        match self {
            Builtin::Log(log) => log.name,
            Builtin::Table(table) => table.name,
            Builtin::View(view) => view.name,
            Builtin::Type(typ) => typ.name,
            Builtin::Func(func) => func.name,
            Builtin::StorageCollection(coll) => coll.name,
        }
    }

    pub fn schema(&self) -> &'static str {
        match self {
            Builtin::Log(log) => log.schema,
            Builtin::Table(table) => table.schema,
            Builtin::View(view) => view.schema,
            Builtin::Type(typ) => typ.schema,
            Builtin::Func(func) => func.schema,
            Builtin::StorageCollection(coll) => coll.schema,
        }
    }
}

#[derive(Clone, Debug, Hash, Serialize)]
pub struct BuiltinLog {
    pub variant: LogVariant,
    pub name: &'static str,
    pub schema: &'static str,
}

#[derive(Hash)]
pub struct BuiltinTable {
    pub name: &'static str,
    pub schema: &'static str,
    pub desc: RelationDesc,
}

#[derive(Clone, Debug, Hash, Serialize)]
pub struct BuiltinStorageCollection {
    pub name: &'static str,
    pub schema: &'static str,
    pub desc: RelationDesc,
}

#[derive(Hash)]
pub struct BuiltinView {
    pub name: &'static str,
    pub schema: &'static str,
    pub sql: &'static str,
}

pub struct BuiltinType<T: TypeReference> {
    pub name: &'static str,
    pub schema: &'static str,
    pub oid: u32,
    pub details: CatalogTypeDetails<T>,
}

pub struct BuiltinFunc {
    pub schema: &'static str,
    pub name: &'static str,
    pub inner: &'static mz_sql::func::Func,
}

pub static BUILTIN_ROLE_PREFIXES: Lazy<Vec<&str>> = Lazy::new(|| vec!["mz_", "pg_"]);

pub struct BuiltinRole {
    /// Name of the builtin role.
    ///
    /// IMPORTANT: Must start with a prefix from [`BUILTIN_ROLE_PREFIXES`].
    pub name: &'static str,
}

pub trait Fingerprint {
    fn fingerprint(&self) -> u64;
}

impl<T: Hash> Fingerprint for &T {
    fn fingerprint(&self) -> u64 {
        self.hashed()
    }
}

// Types and Funcs never change fingerprints so we just return constant 0
impl<T: TypeReference> Fingerprint for &BuiltinType<T> {
    fn fingerprint(&self) -> u64 {
        0
    }
}
impl Fingerprint for BuiltinFunc {
    fn fingerprint(&self) -> u64 {
        0
    }
}

impl<T: TypeReference> Fingerprint for &Builtin<T> {
    fn fingerprint(&self) -> u64 {
        match self {
            Builtin::Log(log) => log.fingerprint(),
            Builtin::Table(table) => table.fingerprint(),
            Builtin::View(view) => view.fingerprint(),
            Builtin::Type(typ) => typ.fingerprint(),
            Builtin::Func(func) => func.fingerprint(),
            Builtin::StorageCollection(coll) => coll.fingerprint(),
        }
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
    oid: 16,
    details: CatalogTypeDetails {
        typ: CatalogType::Bool,
        array_id: None,
    },
};

pub const TYPE_BYTEA: BuiltinType<NameReference> = BuiltinType {
    name: "bytea",
    schema: PG_CATALOG_SCHEMA,
    oid: 17,
    details: CatalogTypeDetails {
        typ: CatalogType::Bytes,
        array_id: None,
    },
};

pub const TYPE_INT8: BuiltinType<NameReference> = BuiltinType {
    name: "int8",
    schema: PG_CATALOG_SCHEMA,
    oid: 20,
    details: CatalogTypeDetails {
        typ: CatalogType::Int64,
        array_id: None,
    },
};

pub const TYPE_INT4: BuiltinType<NameReference> = BuiltinType {
    name: "int4",
    schema: PG_CATALOG_SCHEMA,
    oid: 23,
    details: CatalogTypeDetails {
        typ: CatalogType::Int32,
        array_id: None,
    },
};

pub const TYPE_TEXT: BuiltinType<NameReference> = BuiltinType {
    name: "text",
    schema: PG_CATALOG_SCHEMA,
    oid: 25,
    details: CatalogTypeDetails {
        typ: CatalogType::String,
        array_id: None,
    },
};

pub const TYPE_OID: BuiltinType<NameReference> = BuiltinType {
    name: "oid",
    schema: PG_CATALOG_SCHEMA,
    oid: 26,
    details: CatalogTypeDetails {
        typ: CatalogType::Oid,
        array_id: None,
    },
};

pub const TYPE_FLOAT4: BuiltinType<NameReference> = BuiltinType {
    name: "float4",
    schema: PG_CATALOG_SCHEMA,
    oid: 700,
    details: CatalogTypeDetails {
        typ: CatalogType::Float32,
        array_id: None,
    },
};

pub const TYPE_FLOAT8: BuiltinType<NameReference> = BuiltinType {
    name: "float8",
    schema: PG_CATALOG_SCHEMA,
    oid: 701,
    details: CatalogTypeDetails {
        typ: CatalogType::Float64,
        array_id: None,
    },
};

pub const TYPE_BOOL_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_bool",
    schema: PG_CATALOG_SCHEMA,
    oid: 1000,
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
    oid: 1001,
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
    oid: 1007,
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
    oid: 1009,
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
    oid: 1016,
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
    oid: 1021,
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
    oid: 1022,
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
    oid: 1028,
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
    oid: 1082,
    details: CatalogTypeDetails {
        typ: CatalogType::Date,
        array_id: None,
    },
};

pub const TYPE_TIME: BuiltinType<NameReference> = BuiltinType {
    name: "time",
    schema: PG_CATALOG_SCHEMA,
    oid: 1083,
    details: CatalogTypeDetails {
        typ: CatalogType::Time,
        array_id: None,
    },
};

pub const TYPE_TIMESTAMP: BuiltinType<NameReference> = BuiltinType {
    name: "timestamp",
    schema: PG_CATALOG_SCHEMA,
    oid: 1114,
    details: CatalogTypeDetails {
        typ: CatalogType::Timestamp,
        array_id: None,
    },
};

pub const TYPE_TIMESTAMP_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_timestamp",
    schema: PG_CATALOG_SCHEMA,
    oid: 1115,
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
    oid: 1182,
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
    oid: 1183,
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
    oid: 1184,
    details: CatalogTypeDetails {
        typ: CatalogType::TimestampTz,
        array_id: None,
    },
};

pub const TYPE_TIMESTAMPTZ_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_timestamptz",
    schema: PG_CATALOG_SCHEMA,
    oid: 1185,
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
    oid: 1186,
    details: CatalogTypeDetails {
        typ: CatalogType::Interval,
        array_id: None,
    },
};

pub const TYPE_INTERVAL_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_interval",
    schema: PG_CATALOG_SCHEMA,
    oid: 1187,
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
    oid: 1700,
    details: CatalogTypeDetails {
        typ: CatalogType::Numeric,
        array_id: None,
    },
};

pub const TYPE_NUMERIC_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_numeric",
    schema: PG_CATALOG_SCHEMA,
    oid: 1231,
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
    oid: 2249,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_RECORD_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_record",
    schema: PG_CATALOG_SCHEMA,
    oid: 2287,
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
    oid: 2950,
    details: CatalogTypeDetails {
        typ: CatalogType::Uuid,
        array_id: None,
    },
};

pub const TYPE_UUID_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_uuid",
    schema: PG_CATALOG_SCHEMA,
    oid: 2951,
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
    oid: 3802,
    details: CatalogTypeDetails {
        typ: CatalogType::Jsonb,
        array_id: None,
    },
};

pub const TYPE_JSONB_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_jsonb",
    schema: PG_CATALOG_SCHEMA,
    oid: 3807,
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
    oid: 2276,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "anyarray",
    schema: PG_CATALOG_SCHEMA,
    oid: 2277,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYELEMENT: BuiltinType<NameReference> = BuiltinType {
    name: "anyelement",
    schema: PG_CATALOG_SCHEMA,
    oid: 2283,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYNONARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "anynonarray",
    schema: PG_CATALOG_SCHEMA,
    oid: 2776,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_CHAR: BuiltinType<NameReference> = BuiltinType {
    name: "char",
    schema: PG_CATALOG_SCHEMA,
    oid: 18,
    details: CatalogTypeDetails {
        typ: CatalogType::PgLegacyChar,
        array_id: None,
    },
};

pub const TYPE_VARCHAR: BuiltinType<NameReference> = BuiltinType {
    name: "varchar",
    schema: PG_CATALOG_SCHEMA,
    oid: 1043,
    details: CatalogTypeDetails {
        typ: CatalogType::VarChar,
        array_id: None,
    },
};

pub const TYPE_INT2: BuiltinType<NameReference> = BuiltinType {
    name: "int2",
    schema: PG_CATALOG_SCHEMA,
    oid: 21,
    details: CatalogTypeDetails {
        typ: CatalogType::Int16,
        array_id: None,
    },
};

pub const TYPE_INT2_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_int2",
    schema: PG_CATALOG_SCHEMA,
    oid: 1005,
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
    oid: 1042,
    details: CatalogTypeDetails {
        typ: CatalogType::Char,
        array_id: None,
    },
};

pub const TYPE_CHAR_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_char",
    schema: PG_CATALOG_SCHEMA,
    oid: 1002,
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
    oid: 1015,
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
    oid: 1014,
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
    oid: 24,
    details: CatalogTypeDetails {
        typ: CatalogType::RegProc,
        array_id: None,
    },
};

pub const TYPE_REGPROC_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_regproc",
    schema: PG_CATALOG_SCHEMA,
    oid: 1008,
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
    oid: 2206,
    details: CatalogTypeDetails {
        typ: CatalogType::RegType,
        array_id: None,
    },
};

pub const TYPE_REGTYPE_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_regtype",
    schema: PG_CATALOG_SCHEMA,
    oid: 2211,
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
    oid: 2205,
    details: CatalogTypeDetails {
        typ: CatalogType::RegClass,
        array_id: None,
    },
};

pub const TYPE_REGCLASS_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_regclass",
    schema: PG_CATALOG_SCHEMA,
    oid: 2210,
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
    oid: 22,
    details: CatalogTypeDetails {
        typ: CatalogType::Int2Vector,
        array_id: None,
    },
};

pub const TYPE_INT2_VECTOR_ARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "_int2vector",
    schema: PG_CATALOG_SCHEMA,
    oid: 1006,
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
    oid: 5077,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYCOMPATIBLEARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "anycompatiblearray",
    schema: PG_CATALOG_SCHEMA,
    oid: 5078,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYCOMPATIBLENONARRAY: BuiltinType<NameReference> = BuiltinType {
    name: "anycompatiblenonarray",
    schema: PG_CATALOG_SCHEMA,
    oid: 5079,
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

pub const MZ_DATAFLOW_OPERATORS: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_operators",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Operates),
};

pub const MZ_DATAFLOW_OPERATORS_ADDRESSES: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_addresses",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Addresses),
};

pub const MZ_DATAFLOW_CHANNELS: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_channels",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Channels),
};

pub const MZ_SCHEDULING_ELAPSED_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_scheduling_elapsed_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Elapsed),
};

pub const MZ_SCHEDULING_HISTOGRAM_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_scheduling_histogram_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Histogram),
};

pub const MZ_SCHEDULING_PARKS_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_scheduling_parks_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Parks),
};

pub const MZ_ARRANGEMENT_BATCHES_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_arrangement_batches_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Differential(DifferentialLog::ArrangementBatches),
};

pub const MZ_ARRANGEMENT_SHARING_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_arrangement_sharing_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Differential(DifferentialLog::Sharing),
};

pub const MZ_MATERIALIZATIONS: BuiltinLog = BuiltinLog {
    name: "mz_materializations",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Compute(ComputeLog::DataflowCurrent),
};

pub const MZ_MATERIALIZATION_DEPENDENCIES: BuiltinLog = BuiltinLog {
    name: "mz_materialization_dependencies",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Compute(ComputeLog::DataflowDependency),
};

pub const MZ_WORKER_MATERIALIZATION_FRONTIERS: BuiltinLog = BuiltinLog {
    name: "mz_worker_materialization_frontiers",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Compute(ComputeLog::FrontierCurrent),
};

pub const MZ_WORKER_MATERIALIZATION_SOURCE_FRONTIERS: BuiltinLog = BuiltinLog {
    name: "mz_worker_materialization_source_frontiers",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Compute(ComputeLog::SourceFrontierCurrent),
};

pub const MZ_WORKER_MATERIALIZATION_DELAYS: BuiltinLog = BuiltinLog {
    name: "mz_worker_materialization_delays",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Compute(ComputeLog::FrontierDelay),
};

pub const MZ_PEEK_ACTIVE: BuiltinLog = BuiltinLog {
    name: "mz_peek_active",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Compute(ComputeLog::PeekCurrent),
};

pub const MZ_PEEK_DURATIONS: BuiltinLog = BuiltinLog {
    name: "mz_peek_durations",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Compute(ComputeLog::PeekDuration),
};

pub const MZ_MESSAGE_COUNTS_RECEIVED_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_message_counts_received_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::MessagesReceived),
};

pub const MZ_MESSAGE_COUNTS_SENT_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_message_counts_sent_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::MessagesSent),
};

pub const MZ_DATAFLOW_OPERATOR_REACHABILITY_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_operator_reachability_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Reachability),
};

pub const MZ_ARRANGEMENT_RECORDS_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_arrangement_records_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Differential(DifferentialLog::ArrangementRecords),
};

pub static MZ_VIEW_KEYS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_view_keys",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("global_id", ScalarType::String.nullable(false))
        .with_column("column", ScalarType::Int64.nullable(false))
        .with_column("key_group", ScalarType::Int64.nullable(false)),
});
pub static MZ_VIEW_FOREIGN_KEYS: Lazy<BuiltinTable> = Lazy::new(|| {
    BuiltinTable {
        name: "mz_view_foreign_keys",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("child_id", ScalarType::String.nullable(false))
            .with_column("child_column", ScalarType::Int64.nullable(false))
            .with_column("parent_id", ScalarType::String.nullable(false))
            .with_column("parent_column", ScalarType::Int64.nullable(false))
            .with_column("key_group", ScalarType::Int64.nullable(false))
            .with_key(vec![0, 1, 4]), // TODO: explain why this is a key.
    }
});
pub static MZ_KAFKA_SINKS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_kafka_sinks",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("sink_id", ScalarType::String.nullable(false))
        .with_column("topic", ScalarType::String.nullable(false))
        .with_column("consistency_topic", ScalarType::String.nullable(true))
        .with_key(vec![0]),
});
pub static MZ_DATABASES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_databases",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::Int64.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("name", ScalarType::String.nullable(false)),
});
pub static MZ_SCHEMAS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_schemas",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::Int64.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("database_id", ScalarType::Int64.nullable(true))
        .with_column("name", ScalarType::String.nullable(false)),
});
pub static MZ_COLUMNS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_columns",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("position", ScalarType::Int64.nullable(false))
        .with_column("nullable", ScalarType::Bool.nullable(false))
        .with_column("type", ScalarType::String.nullable(false))
        .with_column("default", ScalarType::String.nullable(true))
        .with_column("type_oid", ScalarType::Oid.nullable(false)),
});
pub static MZ_INDEXES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_indexes",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("on_id", ScalarType::String.nullable(false))
        .with_column("cluster_id", ScalarType::Int64.nullable(false)),
});
pub static MZ_INDEX_COLUMNS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_index_columns",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("index_id", ScalarType::String.nullable(false))
        .with_column("index_position", ScalarType::Int64.nullable(false))
        .with_column("on_position", ScalarType::Int64.nullable(true))
        .with_column("on_expression", ScalarType::String.nullable(true))
        .with_column("nullable", ScalarType::Bool.nullable(false)),
});
pub static MZ_TABLES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_tables",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::Int64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false)),
});
pub static MZ_CONNECTIONS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_connections",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::Int64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("type", ScalarType::String.nullable(false)),
});
pub static MZ_SSH_TUNNEL_CONNECTIONS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_ssh_tunnel_connections",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("public_key", ScalarType::String.nullable(false)),
});
pub static MZ_SOURCES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_sources",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::Int64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("type", ScalarType::String.nullable(false)),
});
pub static MZ_SINKS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_sinks",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::Int64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("type", ScalarType::String.nullable(false)),
});
pub static MZ_VIEWS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_views",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::Int64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("definition", ScalarType::String.nullable(false)),
});
pub static MZ_MATERIALIZED_VIEWS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_materialized_views",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::Int64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("cluster_id", ScalarType::Int64.nullable(false))
        .with_column("definition", ScalarType::String.nullable(false)),
});
pub static MZ_TYPES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_types",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::Int64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("category", ScalarType::String.nullable(false)),
});
pub static MZ_ARRAY_TYPES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_array_types",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("type_id", ScalarType::String.nullable(false))
        .with_column("element_id", ScalarType::String.nullable(false)),
});
pub static MZ_BASE_TYPES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_base_types",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty().with_column("type_id", ScalarType::String.nullable(false)),
});
pub static MZ_LIST_TYPES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_list_types",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("type_id", ScalarType::String.nullable(false))
        .with_column("element_id", ScalarType::String.nullable(false)),
});
pub static MZ_MAP_TYPES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_map_types",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("type_id", ScalarType::String.nullable(false))
        .with_column("key_id", ScalarType::String.nullable(false))
        .with_column("value_id", ScalarType::String.nullable(false)),
});
pub static MZ_ROLES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_roles",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("name", ScalarType::String.nullable(false)),
});
pub static MZ_PSEUDO_TYPES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_pseudo_types",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty().with_column("type_id", ScalarType::String.nullable(false)),
});
pub static MZ_FUNCTIONS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_functions",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("oid", ScalarType::Oid.nullable(false))
        .with_column("schema_id", ScalarType::Int64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column(
            "arg_ids",
            ScalarType::Array(Box::new(ScalarType::String)).nullable(false),
        )
        .with_column("variadic_id", ScalarType::String.nullable(true))
        .with_column("ret_id", ScalarType::String.nullable(true))
        .with_column("ret_set", ScalarType::Bool.nullable(false)),
});
pub static MZ_CLUSTERS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_clusters",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::Int64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false)),
});
pub static MZ_SECRETS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_secrets",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::String.nullable(false))
        .with_column("schema_id", ScalarType::Int64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false)),
});
pub static MZ_CLUSTER_REPLICAS_BASE: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_cluster_replicas_base",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("cluster_id", ScalarType::Int64.nullable(false))
        .with_column("id", ScalarType::Int64.nullable(false))
        .with_column("name", ScalarType::String.nullable(false))
        .with_column("size", ScalarType::String.nullable(true))
        .with_column("availability_zone", ScalarType::String.nullable(true)),
});

pub static MZ_CLUSTER_REPLICA_STATUSES: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_cluster_replica_statuses",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("replica_id", ScalarType::Int64.nullable(false))
        .with_column("process_id", ScalarType::Int64.nullable(false))
        .with_column("status", ScalarType::String.nullable(false))
        .with_column("last_update", ScalarType::TimestampTz.nullable(false)),
});

pub static MZ_CLUSTER_REPLICA_HEARTBEATS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_cluster_replica_heartbeats",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("replica_id", ScalarType::Int64.nullable(false))
        .with_column("last_heartbeat", ScalarType::TimestampTz.nullable(false)),
});

pub static MZ_AUDIT_EVENTS: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_audit_events",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::Int64.nullable(false))
        .with_column("event_type", ScalarType::String.nullable(false))
        .with_column("object_type", ScalarType::String.nullable(false))
        .with_column("event_details", ScalarType::Jsonb.nullable(false))
        .with_column("user", ScalarType::String.nullable(false))
        .with_column("occurred_at", ScalarType::TimestampTz.nullable(false)),
});

pub static MZ_SOURCE_STATUS_HISTORY: Lazy<BuiltinStorageCollection> =
    Lazy::new(|| BuiltinStorageCollection {
        name: "mz_source_status_history",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("timestamp", ScalarType::Timestamp.nullable(false))
            .with_column("source_name", ScalarType::String.nullable(false))
            .with_column("source_id", ScalarType::String.nullable(false))
            .with_column("source_type", ScalarType::String.nullable(false))
            .with_column("upstream_name", ScalarType::String.nullable(true))
            .with_column("worker_id", ScalarType::Int64.nullable(false))
            .with_column("worker_count", ScalarType::Int64.nullable(false))
            .with_column("status", ScalarType::String.nullable(false))
            .with_column("error", ScalarType::String.nullable(true))
            .with_column("metadata", ScalarType::Jsonb.nullable(true)),
    });
pub static MZ_STORAGE_USAGE: Lazy<BuiltinTable> = Lazy::new(|| BuiltinTable {
    name: "mz_storage_usage",
    schema: MZ_CATALOG_SCHEMA,
    desc: RelationDesc::empty()
        .with_column("id", ScalarType::Int64.nullable(false))
        .with_column("object_id", ScalarType::String.nullable(true))
        .with_column("size_bytes", ScalarType::Int64.nullable(false))
        .with_column(
            "collection_timestamp",
            ScalarType::TimestampTz.nullable(false),
        ),
});

pub const MZ_RELATIONS: BuiltinView = BuiltinView {
    name: "mz_relations",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_relations (id, oid, schema_id, name, type) AS
      SELECT id, oid, schema_id, name, 'table' FROM mz_catalog.mz_tables
UNION SELECT id, oid, schema_id, name, 'source' FROM mz_catalog.mz_sources
UNION SELECT id, oid, schema_id, name, 'view' FROM mz_catalog.mz_views
UNION SELECT id, oid, schema_id, name, 'materialized view' FROM mz_catalog.mz_materialized_views",
};

pub const MZ_OBJECTS: BuiltinView = BuiltinView {
    name: "mz_objects",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_objects (id, oid, schema_id, name, type) AS
    SELECT id, oid, schema_id, name, type FROM mz_catalog.mz_relations
UNION
    SELECT id, oid, schema_id, name, 'sink' FROM mz_catalog.mz_sinks
UNION
    SELECT mz_indexes.id, mz_indexes.oid, schema_id, mz_indexes.name, 'index'
    FROM mz_catalog.mz_indexes
    JOIN mz_catalog.mz_relations ON mz_indexes.on_id = mz_relations.id",
};

// For historical reasons, this view does not properly escape identifiers. For
// example, a table named 'cAp.S' in the default database and schema will be
// rendered as `materialize.public.cAp.S`. This is *not* a valid SQL identifier,
// as it has an extra dot, and the capitals will be folded to lowercase. This
// view is thus only fit for use in debugging queries where the names are for
// human consumption. Applications should instead pull the names of individual
// components out of mz_objects, mz_schemas, and mz_databases to avoid these
// escaping issues.
//
// TODO(benesch): deprecate this view.
pub const MZ_CATALOG_NAMES: BuiltinView = BuiltinView {
    name: "mz_catalog_names",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_catalog_names AS SELECT
    o.id AS global_id,
    coalesce(d.name || '.', '') || s.name || '.' || o.name AS name
FROM mz_catalog.mz_objects o
JOIN mz_catalog.mz_schemas s ON s.id = o.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id",
};

pub const MZ_DATAFLOW_NAMES: BuiltinView = BuiltinView {
    name: "mz_dataflow_names",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_dataflow_names AS SELECT
    mz_dataflow_addresses.id,
    mz_dataflow_addresses.worker,
    mz_dataflow_addresses.address[1] AS local_id,
    mz_dataflow_operators.name
FROM
    mz_catalog.mz_dataflow_addresses,
    mz_catalog.mz_dataflow_operators
WHERE
    mz_dataflow_addresses.id = mz_dataflow_operators.id AND
    mz_dataflow_addresses.worker = mz_dataflow_operators.worker AND
    mz_catalog.list_length(mz_dataflow_addresses.address) = 1",
};

pub const MZ_DATAFLOW_OPERATOR_DATAFLOWS: BuiltinView = BuiltinView {
    name: "mz_dataflow_operator_dataflows",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_dataflow_operator_dataflows AS SELECT
    mz_dataflow_operators.id,
    mz_dataflow_operators.name,
    mz_dataflow_operators.worker,
    mz_dataflow_names.id as dataflow_id,
    mz_dataflow_names.name as dataflow_name
FROM
    mz_catalog.mz_dataflow_operators,
    mz_catalog.mz_dataflow_addresses,
    mz_catalog.mz_dataflow_names
WHERE
    mz_dataflow_operators.id = mz_dataflow_addresses.id AND
    mz_dataflow_operators.worker = mz_dataflow_addresses.worker AND
    mz_dataflow_names.local_id = mz_dataflow_addresses.address[1] AND
    mz_dataflow_names.worker = mz_dataflow_addresses.worker",
};

pub const MZ_CLUSTER_REPLICAS: BuiltinView = BuiltinView {
    name: "mz_cluster_replicas",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_cluster_replicas AS
WITH counts AS (
    SELECT
        replica_id,
        count(*) AS total,
        sum(CASE WHEN status = 'ready' THEN 1 else 0 END) AS ready,
        sum(CASE WHEN status = 'not_ready' THEN 1 else 0 END) AS not_ready
    FROM mz_catalog.mz_cluster_replica_statuses
    GROUP BY replica_id
)
SELECT
    mz_cluster_replicas_base.*,
    CASE
        WHEN counts.total = 0 OR counts.not_ready > 0
            THEN 'unhealthy'
        WHEN counts.ready = counts.total
            THEN 'healthy'
        ELSE 'unknown'
        END AS status
FROM mz_catalog.mz_cluster_replicas_base
LEFT OUTER JOIN counts
    ON mz_cluster_replicas_base.id = counts.replica_id",
};

pub const MZ_MATERIALIZATION_FRONTIERS: BuiltinView = BuiltinView {
    name: "mz_materialization_frontiers",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_materialization_frontiers AS SELECT
    global_id, pg_catalog.min(time) AS time
FROM mz_catalog.mz_worker_materialization_frontiers
GROUP BY global_id",
};

pub const MZ_MATERIALIZATION_SOURCE_FRONTIERS: BuiltinView = BuiltinView {
    name: "mz_materialization_source_frontiers",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_materialization_source_frontiers AS SELECT
    global_id, source, pg_catalog.min(time) AS time
FROM mz_catalog.mz_worker_materialization_source_frontiers
GROUP BY global_id, source",
};

pub const MZ_RECORDS_PER_DATAFLOW_OPERATOR: BuiltinView = BuiltinView {
    name: "mz_records_per_dataflow_operator",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_records_per_dataflow_operator AS
WITH records_cte AS (
    SELECT
        operator,
        worker,
        pg_catalog.count(*) AS records
    FROM
        mz_catalog.mz_arrangement_records_internal
    GROUP BY
        operator, worker
)
SELECT
    mz_dataflow_operator_dataflows.id,
    mz_dataflow_operator_dataflows.name,
    mz_dataflow_operator_dataflows.worker,
    mz_dataflow_operator_dataflows.dataflow_id,
    records_cte.records
FROM
    records_cte,
    mz_catalog.mz_dataflow_operator_dataflows
WHERE
    mz_dataflow_operator_dataflows.id = records_cte.operator AND
    mz_dataflow_operator_dataflows.worker = records_cte.worker",
};

pub const MZ_RECORDS_PER_DATAFLOW: BuiltinView = BuiltinView {
    name: "mz_records_per_dataflow",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_records_per_dataflow AS SELECT
    mz_records_per_dataflow_operator.dataflow_id as id,
    mz_dataflow_names.name,
    mz_records_per_dataflow_operator.worker,
    pg_catalog.SUM(mz_records_per_dataflow_operator.records) as records
FROM
    mz_catalog.mz_records_per_dataflow_operator,
    mz_catalog.mz_dataflow_names
WHERE
    mz_records_per_dataflow_operator.dataflow_id = mz_dataflow_names.id AND
    mz_records_per_dataflow_operator.worker = mz_dataflow_names.worker
GROUP BY
    mz_records_per_dataflow_operator.dataflow_id,
    mz_dataflow_names.name,
    mz_records_per_dataflow_operator.worker",
};

pub const MZ_RECORDS_PER_DATAFLOW_GLOBAL: BuiltinView = BuiltinView {
    name: "mz_records_per_dataflow_global",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_records_per_dataflow_global AS SELECT
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name,
    pg_catalog.SUM(mz_records_per_dataflow.records) as records
FROM
    mz_catalog.mz_records_per_dataflow
GROUP BY
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name",
};

pub const MZ_PERF_ARRANGEMENT_RECORDS: BuiltinView = BuiltinView {
    name: "mz_perf_arrangement_records",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_perf_arrangement_records AS
WITH records_cte AS (
    SELECT
        operator,
        worker,
        pg_catalog.count(*) AS records
    FROM
        mz_catalog.mz_arrangement_records_internal
    GROUP BY
        operator, worker
)
SELECT mas.worker, name, records, operator
FROM
    records_cte mas LEFT JOIN mz_catalog.mz_dataflow_operators mdo
        ON mdo.id = mas.operator AND mdo.worker = mas.worker",
};

pub const MZ_PERF_PEEK_DURATIONS_CORE: BuiltinView = BuiltinView {
    name: "mz_perf_peek_durations_core",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_perf_peek_durations_core AS SELECT
    d_upper.worker,
    d_upper.duration_ns::pg_catalog.text AS le,
    pg_catalog.sum(d_summed.count) AS count
FROM
    mz_catalog.mz_peek_durations AS d_upper,
    mz_catalog.mz_peek_durations AS d_summed
WHERE
    d_upper.worker = d_summed.worker AND
    d_upper.duration_ns >= d_summed.duration_ns
GROUP BY d_upper.worker, d_upper.duration_ns",
};

pub const MZ_PERF_PEEK_DURATIONS_BUCKET: BuiltinView = BuiltinView {
    name: "mz_perf_peek_durations_bucket",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_perf_peek_durations_bucket AS
(
    SELECT * FROM mz_catalog.mz_perf_peek_durations_core
) UNION (
    SELECT worker, '+Inf', pg_catalog.max(count) AS count FROM mz_catalog.mz_perf_peek_durations_core
    GROUP BY worker
)",
};

pub const MZ_PERF_PEEK_DURATIONS_AGGREGATES: BuiltinView = BuiltinView {
    name: "mz_perf_peek_durations_aggregates",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_perf_peek_durations_aggregates AS SELECT worker, pg_catalog.sum(duration_ns * count) AS sum, pg_catalog.sum(count) AS count
FROM mz_catalog.mz_peek_durations lpd
GROUP BY worker",
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
JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())",
};

pub const PG_CLASS: BuiltinView = BuiltinView {
    name: "pg_class",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_class AS SELECT
    mz_objects.oid,
    mz_objects.name AS relname,
    mz_schemas.oid AS relnamespace,
    -- MZ doesn't support typed tables so reloftype is filled with 0
    0::pg_catalog.oid AS reloftype,
    NULL::pg_catalog.oid AS relowner,
    0::pg_catalog.oid AS relam,
    -- MZ doesn't have tablespaces so reltablespace is filled in with 0 implying the default tablespace
    0::pg_catalog.oid AS reltablespace,
    -- MZ doesn't use TOAST tables so reltoastrelid is filled with 0
    0::pg_catalog.oid AS reltoastrelid,
    EXISTS (SELECT * FROM mz_catalog.mz_indexes where mz_indexes.on_id = mz_objects.id) AS relhasindex,
    -- MZ doesn't have unlogged tables and because of (https://github.com/MaterializeInc/materialize/issues/8805)
    -- temporary objects don't show up here, so relpersistence is filled with 'p' for permanent.
    -- TODO(jkosh44): update this column when issue is resolved.
    'p'::pg_catalog.\"char\" AS relpersistence,
    CASE
        WHEN mz_objects.type = 'table' THEN 'r'
        WHEN mz_objects.type = 'source' THEN 'r'
        WHEN mz_objects.type = 'index' THEN 'i'
        WHEN mz_objects.type = 'view' THEN 'v'
        WHEN mz_objects.type = 'materialized view' THEN 'm'
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
FROM mz_catalog.mz_objects
JOIN mz_catalog.mz_schemas ON mz_schemas.id = mz_objects.schema_id
JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())",
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
    mz_objects.oid AS indrelid,
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
    pg_catalog.string_agg(coalesce(mz_index_columns.on_position, 0)::pg_catalog.text, ' ' ORDER BY mz_index_columns.index_position)::pg_catalog.int2vector AS indkey,
    -- MZ doesn't have per-column flags, so returning a 0 for each column in the index
    pg_catalog.string_agg('0', ' ')::pg_catalog.int2vector AS indoption,
    -- Index expressions are returned in MZ format
    CASE pg_catalog.string_agg(mz_index_columns.on_expression, ' ' ORDER BY mz_index_columns.index_position)
    WHEN NULL THEN NULL
    ELSE '{' || pg_catalog.string_agg(mz_index_columns.on_expression, '}, {' ORDER BY mz_index_columns.index_position) || '}'
    END AS indexprs,
    -- MZ doesn't support indexes with predicates
    NULL::pg_catalog.text AS indpred
FROM mz_catalog.mz_indexes
JOIN mz_catalog.mz_objects ON mz_indexes.on_id = mz_objects.id
JOIN mz_catalog.mz_index_columns ON mz_index_columns.index_id = mz_indexes.id
JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())
GROUP BY mz_indexes.oid, mz_objects.oid",
};

pub const PG_DESCRIPTION: BuiltinView = BuiltinView {
    name: "pg_description",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_description AS SELECT
    c.oid as objoid,
    NULL::pg_catalog.oid as classoid,
    0::pg_catalog.int4 as objsubid,
    NULL::pg_catalog.text as description
FROM pg_catalog.pg_class c
JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())",
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
    END) AS typcategory,
    0::pg_catalog.oid AS typrelid,
    NULL::pg_catalog.oid AS typelem,
    coalesce(
        (
            SELECT
                t.oid
            FROM
                mz_catalog.mz_array_types AS a
                JOIN mz_catalog.mz_types AS t ON a.type_id = t.id
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
            SELECT type_id, 'a' AS mztype FROM mz_catalog.mz_array_types
            UNION ALL SELECT type_id, 'b' FROM mz_catalog.mz_base_types
            UNION ALL SELECT type_id, 'l' FROM mz_catalog.mz_list_types
            UNION ALL SELECT type_id, 'm' FROM mz_catalog.mz_map_types
            UNION ALL SELECT type_id, 'p' FROM mz_catalog.mz_pseudo_types
        )
            AS t ON mz_types.id = t.type_id
    JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())",
};

pub const PG_ATTRIBUTE: BuiltinView = BuiltinView {
    name: "pg_attribute",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_attribute AS SELECT
    mz_objects.oid as attrelid,
    mz_columns.name as attname,
    mz_columns.type_oid AS atttypid,
    pg_type.typlen AS attlen,
    position as attnum,
    -1::pg_catalog.int4 as atttypmod,
    NOT nullable as attnotnull,
    mz_columns.default IS NOT NULL as atthasdef,
    ''::pg_catalog.\"char\" as attidentity,
    -- MZ doesn't support generated columns so attgenerated is filled with ''
    ''::pg_catalog.\"char\" as attgenerated,
    FALSE as attisdropped,
    -- MZ doesn't support COLLATE so attcollation is filled with 0
    0::pg_catalog.oid as attcollation
FROM mz_catalog.mz_objects
JOIN mz_catalog.mz_columns ON mz_objects.id = mz_columns.id
JOIN pg_catalog.pg_type ON pg_type.oid = mz_columns.type_oid
JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())",
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
    NULL::pg_catalog.text AS proargdefaults
FROM mz_catalog.mz_functions
JOIN mz_catalog.mz_schemas ON mz_functions.schema_id = mz_schemas.id
JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())",
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
    mz_columns.position AS adnum,
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
    *
FROM (VALUES
    ('max_index_keys'::pg_catalog.text, '1000'::pg_catalog.text)
) AS _ (name, setting)",
};

pub const MZ_SCHEDULING_ELAPSED: BuiltinView = BuiltinView {
    name: "mz_scheduling_elapsed",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_scheduling_elapsed AS SELECT
    id, worker, pg_catalog.count(*) AS elapsed_ns
FROM
    mz_catalog.mz_scheduling_elapsed_internal
GROUP BY
    id, worker",
};

pub const MZ_SCHEDULING_HISTOGRAM: BuiltinView = BuiltinView {
    name: "mz_scheduling_histogram",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_scheduling_histogram AS SELECT
    id, worker, duration_ns, pg_catalog.count(*) AS count
FROM
    mz_catalog.mz_scheduling_histogram_internal
GROUP BY
    id, worker, duration_ns",
};

pub const MZ_SCHEDULING_PARKS: BuiltinView = BuiltinView {
    name: "mz_scheduling_parks",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_scheduling_parks AS SELECT
    worker, slept_for, requested, pg_catalog.count(*) AS count
FROM
    mz_catalog.mz_scheduling_parks_internal
GROUP BY
    worker, slept_for, requested",
};

pub const MZ_MESSAGE_COUNTS: BuiltinView = BuiltinView {
    name: "mz_message_counts",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_message_counts AS
WITH sent_cte AS (
    SELECT
        channel,
        source_worker,
        target_worker,
        pg_catalog.count(*) AS sent
    FROM
        mz_catalog.mz_message_counts_sent_internal
    GROUP BY
        channel, source_worker, target_worker
),
received_cte AS (
    SELECT
        channel,
        source_worker,
        target_worker,
        pg_catalog.count(*) AS received
    FROM
        mz_catalog.mz_message_counts_received_internal
    GROUP BY
        channel, source_worker, target_worker
)
SELECT
    sent_cte.channel,
    sent_cte.source_worker,
    sent_cte.target_worker,
    sent_cte.sent,
    received_cte.received
FROM sent_cte JOIN received_cte USING (channel, source_worker, target_worker)",
};

pub const MZ_DATAFLOW_OPERATOR_REACHABILITY: BuiltinView = BuiltinView {
    name: "mz_dataflow_operator_reachability",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_dataflow_operator_reachability AS SELECT
    address,
    port,
    worker,
    update_type,
    timestamp,
    pg_catalog.count(*) as count
FROM
    mz_catalog.mz_dataflow_operator_reachability_internal
GROUP BY address, port, worker, update_type, timestamp",
};

pub const MZ_ARRANGEMENT_SIZES: BuiltinView = BuiltinView {
    name: "mz_arrangement_sizes",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_arrangement_sizes AS
WITH batches_cte AS (
    SELECT
        operator,
        worker,
        pg_catalog.count(*) AS batches
    FROM
        mz_catalog.mz_arrangement_batches_internal
    GROUP BY
        operator, worker
),
records_cte AS (
    SELECT
        operator,
        worker,
        pg_catalog.count(*) AS records
    FROM
        mz_catalog.mz_arrangement_records_internal
    GROUP BY
        operator, worker
)
SELECT
    batches_cte.operator,
    batches_cte.worker,
    records_cte.records,
    batches_cte.batches
FROM batches_cte JOIN records_cte USING (operator, worker)",
};

pub const MZ_ARRANGEMENT_SHARING: BuiltinView = BuiltinView {
    name: "mz_arrangement_sharing",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_catalog.mz_arrangement_sharing AS
SELECT
    operator,
    worker,
    pg_catalog.count(*) AS count
FROM mz_catalog.mz_arrangement_sharing_internal
GROUP BY operator, worker",
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
JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())
WHERE c.relkind = ANY (ARRAY['r','p'])",
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
WHERE d.name = pg_catalog.current_database()",
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
WHERE d.name = pg_catalog.current_database()",
};

pub const INFORMATION_SCHEMA_COLUMNS: BuiltinView = BuiltinView {
    name: "columns",
    schema: INFORMATION_SCHEMA,
    sql: "CREATE VIEW information_schema.columns AS
SELECT
    d.name as table_catalog,
    s.name AS table_schema,
    o.name AS table_name,
    c.name AS column_name,
    c.position AS ordinal_position,
    c.type AS data_type,
    NULL::pg_catalog.int4 AS character_maximum_length,
    NULL::pg_catalog.int4 AS numeric_precision,
    NULL::pg_catalog.int4 AS numeric_scale
FROM mz_catalog.mz_columns c
JOIN mz_catalog.mz_objects o ON o.id = c.id
JOIN mz_catalog.mz_schemas s ON s.id = o.schema_id
JOIN mz_catalog.mz_databases d on s.database_id = d.id",
};

pub const INFORMATION_SCHEMA_TABLES: BuiltinView = BuiltinView {
    name: "tables",
    schema: INFORMATION_SCHEMA,
    sql: "CREATE VIEW information_schema.tables AS SELECT
    d.name as table_catalog,
    s.name AS table_schema,
    r.name AS table_name,
    CASE r.type
        WHEN 'table' THEN 'BASE TABLE'
        ELSE pg_catalog.upper(r.type)
    END AS table_type
FROM mz_catalog.mz_relations r
JOIN mz_catalog.mz_schemas s ON s.id = r.schema_id
JOIN mz_catalog.mz_databases d on s.database_id = d.id",
};

// MZ doesn't support COLLATE so the table is filled with NULLs and made empty. pg_database hard
// codes a collation of 'C' for every database, so we could copy that here.
pub const PG_COLLATION: BuiltinView = BuiltinView {
    name: "pg_collation",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_catalog.pg_class
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
    sql: "CREATE VIEW pg_catalog.pg_class
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
    CASE
        WHEN r.name = 'mz_system' THEN true
        ELSE false
    END AS rolsuper,
    -- MZ doesn't have role inheritence
    false AS rolinherit,
    -- All roles can create other roles
    true AS rolcreaterole,
    -- All roles can create other dbs
    true AS rolcreatedb,
    -- All roles can login
    true AS rolcanlogin,
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
FROM mz_catalog.mz_roles r
JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())",
};

pub const MZ_SYSTEM: BuiltinRole = BuiltinRole { name: SYSTEM_USER };

pub static BUILTINS_STATIC: Lazy<Vec<Builtin<NameReference>>> = Lazy::new(|| {
    let mut builtins = vec![
        Builtin::Type(&TYPE_ANY),
        Builtin::Type(&TYPE_ANYARRAY),
        Builtin::Type(&TYPE_ANYELEMENT),
        Builtin::Type(&TYPE_ANYNONARRAY),
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
        Builtin::Log(&MZ_MATERIALIZATIONS),
        Builtin::Log(&MZ_MATERIALIZATION_DEPENDENCIES),
        Builtin::Log(&MZ_MESSAGE_COUNTS_RECEIVED_INTERNAL),
        Builtin::Log(&MZ_MESSAGE_COUNTS_SENT_INTERNAL),
        Builtin::Log(&MZ_PEEK_ACTIVE),
        Builtin::Log(&MZ_PEEK_DURATIONS),
        Builtin::Log(&MZ_SCHEDULING_ELAPSED_INTERNAL),
        Builtin::Log(&MZ_SCHEDULING_HISTOGRAM_INTERNAL),
        Builtin::Log(&MZ_SCHEDULING_PARKS_INTERNAL),
        Builtin::Log(&MZ_WORKER_MATERIALIZATION_FRONTIERS),
        Builtin::Log(&MZ_WORKER_MATERIALIZATION_SOURCE_FRONTIERS),
        Builtin::Log(&MZ_WORKER_MATERIALIZATION_DELAYS),
        Builtin::Table(&MZ_VIEW_KEYS),
        Builtin::Table(&MZ_VIEW_FOREIGN_KEYS),
        Builtin::Table(&MZ_KAFKA_SINKS),
        Builtin::Table(&MZ_DATABASES),
        Builtin::Table(&MZ_SCHEMAS),
        Builtin::Table(&MZ_COLUMNS),
        Builtin::Table(&MZ_INDEXES),
        Builtin::Table(&MZ_INDEX_COLUMNS),
        Builtin::Table(&MZ_TABLES),
        Builtin::Table(&MZ_SOURCES),
        Builtin::Table(&MZ_SINKS),
        Builtin::Table(&MZ_VIEWS),
        Builtin::Table(&MZ_MATERIALIZED_VIEWS),
        Builtin::Table(&MZ_TYPES),
        Builtin::Table(&MZ_ARRAY_TYPES),
        Builtin::Table(&MZ_BASE_TYPES),
        Builtin::Table(&MZ_LIST_TYPES),
        Builtin::Table(&MZ_MAP_TYPES),
        Builtin::Table(&MZ_ROLES),
        Builtin::Table(&MZ_PSEUDO_TYPES),
        Builtin::Table(&MZ_FUNCTIONS),
        Builtin::Table(&MZ_CLUSTERS),
        Builtin::Table(&MZ_SECRETS),
        Builtin::Table(&MZ_CONNECTIONS),
        Builtin::Table(&MZ_SSH_TUNNEL_CONNECTIONS),
        Builtin::Table(&MZ_CLUSTER_REPLICAS_BASE),
        Builtin::Table(&MZ_CLUSTER_REPLICA_STATUSES),
        Builtin::Table(&MZ_CLUSTER_REPLICA_HEARTBEATS),
        Builtin::Table(&MZ_AUDIT_EVENTS),
        Builtin::Table(&MZ_STORAGE_USAGE),
        Builtin::View(&MZ_RELATIONS),
        Builtin::View(&MZ_OBJECTS),
        Builtin::View(&MZ_CATALOG_NAMES),
        Builtin::View(&MZ_ARRANGEMENT_SHARING),
        Builtin::View(&MZ_ARRANGEMENT_SIZES),
        Builtin::View(&MZ_DATAFLOW_NAMES),
        Builtin::View(&MZ_DATAFLOW_OPERATOR_DATAFLOWS),
        Builtin::View(&MZ_DATAFLOW_OPERATOR_REACHABILITY),
        Builtin::View(&MZ_CLUSTER_REPLICAS),
        Builtin::View(&MZ_MATERIALIZATION_FRONTIERS),
        Builtin::View(&MZ_MATERIALIZATION_SOURCE_FRONTIERS),
        Builtin::View(&MZ_MESSAGE_COUNTS),
        Builtin::View(&MZ_PERF_ARRANGEMENT_RECORDS),
        Builtin::View(&MZ_PERF_PEEK_DURATIONS_AGGREGATES),
        Builtin::View(&MZ_PERF_PEEK_DURATIONS_CORE),
        Builtin::View(&MZ_PERF_PEEK_DURATIONS_BUCKET),
        Builtin::View(&MZ_RECORDS_PER_DATAFLOW_OPERATOR),
        Builtin::View(&MZ_RECORDS_PER_DATAFLOW),
        Builtin::View(&MZ_RECORDS_PER_DATAFLOW_GLOBAL),
        Builtin::View(&MZ_SCHEDULING_ELAPSED),
        Builtin::View(&MZ_SCHEDULING_HISTOGRAM),
        Builtin::View(&MZ_SCHEDULING_PARKS),
        Builtin::View(&PG_NAMESPACE),
        Builtin::View(&PG_CLASS),
        Builtin::View(&PG_DATABASE),
        Builtin::View(&PG_INDEX),
        Builtin::View(&PG_DESCRIPTION),
        Builtin::View(&PG_TYPE),
        Builtin::View(&PG_ATTRIBUTE),
        Builtin::View(&PG_PROC),
        Builtin::View(&PG_RANGE),
        Builtin::View(&PG_ENUM),
        Builtin::View(&PG_ATTRDEF),
        Builtin::View(&PG_SETTINGS),
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
        // This is disabled for the moment because it has unusual upper
        // advancement behavior.
        // See: https://materializeinc.slack.com/archives/C01CFKM1QRF/p1660726837927649
        // Builtin::StorageCollection(&MZ_SOURCE_STATUS_HISTORY),
    ]);

    builtins
});
pub static BUILTIN_ROLES: Lazy<Vec<BuiltinRole>> = Lazy::new(|| vec![MZ_SYSTEM]);

#[allow(non_snake_case)]
pub mod BUILTINS {
    use super::*;

    pub fn logs() -> impl Iterator<Item = &'static BuiltinLog> {
        BUILTINS_STATIC.iter().filter_map(|b| match b {
            Builtin::Log(log) => Some(*log),
            _ => None,
        })
    }

    // TODO(lh): Once we remove legacy logs, this function should not be needed anymore
    pub fn variant_to_builtin(variant: LogVariant) -> Option<&'static BuiltinLog> {
        for x in logs() {
            if x.variant == variant {
                return Some(x);
            }
        }
        return None;
    }

    pub fn types() -> impl Iterator<Item = &'static BuiltinType<NameReference>> {
        BUILTINS_STATIC.iter().filter_map(|b| match b {
            Builtin::Type(typ) => Some(*typ),
            _ => None,
        })
    }

    pub fn iter() -> impl Iterator<Item = &'static Builtin<NameReference>> {
        BUILTINS_STATIC.iter()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::env;

    use tokio_postgres::NoTls;

    use mz_ore::now::NOW_ZERO;
    use mz_ore::task;
    use mz_pgrepr::oid::{FIRST_MATERIALIZE_OID, FIRST_UNPINNED_OID};
    use mz_sql::catalog::{CatalogSchema, SessionCatalog};
    use mz_sql::names::{PartialObjectName, ResolvedDatabaseSpecifier};

    use crate::catalog::{Catalog, CatalogItem, SYSTEM_CONN_ID};

    use super::*;

    // Connect to a running Postgres server and verify that our builtin
    // types and functions match it, in addition to some other things.
    #[tokio::test]
    async fn compare_builtins_postgres() -> Result<(), anyhow::Error> {
        // Verify that all builtin functions:
        // - have a unique OID
        // - if they have a postgres counterpart (same oid) then they have matching name
        // Note: Use Postgres 13 when testing because older version don't have all
        // functions.
        let (client, connection) = tokio_postgres::connect(
            &env::var("POSTGRES_URL").unwrap_or_else(|_| "host=localhost user=postgres".into()),
            NoTls,
        )
        .await?;

        task::spawn(|| "compare_builtin_postgres", async move {
            if let Err(e) = connection.await {
                panic!("connection error: {}", e);
            }
        });

        struct PgProc {
            name: String,
            schema: String,
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

        let pg_proc: HashMap<_, _> = client
            .query(
                "SELECT
                    p.oid,
                    proname,
                    nspname,
                    proargtypes,
                    prorettype,
                    proretset
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| {
                let oid: u32 = row.get("oid");
                let pg_proc = PgProc {
                    name: row.get("proname"),
                    schema: row.get("nspname"),
                    arg_oids: row.get("proargtypes"),
                    ret_oid: row.get("prorettype"),
                    ret_set: row.get("proretset"),
                };
                (oid, pg_proc)
            })
            .collect();

        let pg_proc_by_name: HashMap<_, _> = pg_proc
            .iter()
            .map(|(_, proc)| ((&*proc.schema, &*proc.name), proc))
            .collect();

        let pg_type: HashMap<_, _> = client
            .query(
                "SELECT oid, typname, typtype::text, typelem, typarray FROM pg_type",
                &[],
            )
            .await?
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

        let catalog = Catalog::open_debug_sqlite(NOW_ZERO.clone()).await?;
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
                .unwrap()
                .oid()
        };

        let mut proc_oids = HashSet::new();
        let mut type_oids = HashSet::new();

        for builtin in BUILTINS::iter() {
            match builtin {
                Builtin::Type(ty) => {
                    // Verify that all type OIDs are unique.
                    assert!(
                        type_oids.insert(ty.oid),
                        "{} reused oid {}",
                        ty.name,
                        ty.oid
                    );

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
                                None => panic!("{} is unexpectedly not a type", element_reference),
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
                        _ => {
                            assert_eq!(
                                pg_ty.ty, "b",
                                "type {} is not a base type as expected",
                                ty.name
                            )
                        }
                    }

                    // Ensure the array type reference is correct.
                    let schema = catalog.resolve_schema_in_database(
                        &ResolvedDatabaseSpecifier::Ambient,
                        ty.schema,
                        SYSTEM_CONN_ID,
                    )?;
                    let allocated_type = catalog.resolve_entry(
                        None,
                        &vec![(ResolvedDatabaseSpecifier::Ambient, schema.id().clone())],
                        &PartialObjectName {
                            database: None,
                            schema: Some(schema.name().schema.clone()),
                            item: ty.name.to_string(),
                        },
                        SYSTEM_CONN_ID,
                    )?;
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
                        // Verify that all function OIDs are unique.
                        assert!(
                            proc_oids.insert(imp.oid),
                            "{} reused oid {}",
                            func.name,
                            imp.oid
                        );

                        if imp.oid >= FIRST_MATERIALIZE_OID {
                            // High OIDs are reserved in Materialize and don't have
                            // postgres counterparts.
                            continue;
                        }

                        // For functions that have a postgres counterpart, verify that the name and
                        // oid match.
                        let pg_fn = if imp.oid >= FIRST_UNPINNED_OID {
                            pg_proc_by_name
                                .get(&(func.schema, func.name))
                                .unwrap_or_else(|| {
                                    panic!("pg_proc missing function {}.{}", func.schema, func.name)
                                })
                        } else {
                            pg_proc.get(&imp.oid).unwrap_or_else(|| {
                                panic!("pg_proc missing function {}: oid {}", func.name, imp.oid)
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
                            println!(
                                "funcs with oid {} ({}) don't match set-returning value: {:?} in mz, {:?} in pg",
                                imp.oid, func.name, imp.return_is_set, pg_fn.ret_set
                            );
                        }
                    }
                }
                _ => (),
            }
        }

        Ok(())
    }
}
