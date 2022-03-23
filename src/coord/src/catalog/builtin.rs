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

use std::collections::{BTreeMap, BTreeSet};

use lazy_static::lazy_static;

use mz_dataflow_types::logging::{DifferentialLog, LogVariant, MaterializedLog, TimelyLog};
use mz_expr::GlobalId;
use mz_repr::{RelationDesc, ScalarType};
use mz_sql::catalog::{CatalogType, CatalogTypeDetails};

pub const MZ_TEMP_SCHEMA: &str = "mz_temp";
pub const MZ_CATALOG_SCHEMA: &str = "mz_catalog";
pub const PG_CATALOG_SCHEMA: &str = "pg_catalog";
pub const MZ_INTERNAL_SCHEMA: &str = "mz_internal";
pub const INFORMATION_SCHEMA: &str = "information_schema";

pub enum Builtin {
    Log(&'static BuiltinLog),
    Table(&'static BuiltinTable),
    View(&'static BuiltinView),
    Type(&'static BuiltinType),
    Func(BuiltinFunc),
}

impl Builtin {
    pub fn name(&self) -> &'static str {
        match self {
            Builtin::Log(log) => log.name,
            Builtin::Table(table) => table.name,
            Builtin::View(view) => view.name,
            Builtin::Type(typ) => typ.name,
            Builtin::Func(func) => func.name,
        }
    }

    pub fn schema(&self) -> &'static str {
        match self {
            Builtin::Log(log) => log.schema,
            Builtin::Table(table) => table.schema,
            Builtin::View(view) => view.schema,
            Builtin::Type(typ) => typ.schema,
            Builtin::Func(func) => func.schema,
        }
    }

    pub fn id(&self) -> GlobalId {
        match self {
            Builtin::Log(log) => log.id,
            Builtin::Table(table) => table.id,
            Builtin::View(view) => view.id,
            Builtin::Type(typ) => typ.id,
            Builtin::Func(func) => func.id,
        }
    }
}

pub struct BuiltinLog {
    pub variant: LogVariant,
    pub name: &'static str,
    pub schema: &'static str,
    pub id: GlobalId,
}

pub struct BuiltinTable {
    pub name: &'static str,
    pub schema: &'static str,
    pub desc: RelationDesc,
    pub id: GlobalId,
    pub persistent: bool,
}

pub struct BuiltinView {
    pub name: &'static str,
    pub schema: &'static str,
    pub sql: &'static str,
    pub id: GlobalId,
}

pub struct BuiltinType {
    pub name: &'static str,
    pub schema: &'static str,
    pub id: GlobalId,
    pub oid: u32,
    pub details: CatalogTypeDetails,
}

pub struct BuiltinFunc {
    pub schema: &'static str,
    pub name: &'static str,
    pub id: GlobalId,
    pub inner: &'static mz_sql::func::Func,
}

pub struct BuiltinRole {
    pub name: &'static str,
    pub id: i64,
}

// Builtin definitions below. Ensure you add new builtins to the `BUILTINS` map.
//
// You SHOULD NOT change the ID of any builtins, nor delete a builtin. If you
// do, you will break any downstream user objects that depended on the builtin.
//
// Builtins are loaded in ID order, so sorting them by global ID makes the
// source code definition order match the load order. A builtin must appear
// AFTER any items it depends upon.
//
// NOTE(benesch): loading builtins in ID order means we're hosed if an existing
// view needs to take on a new dependency on a *new* builtin, i.e., a builtin
// with a global ID that is greater than the existing builtin.
//
// Allocate IDs from the following ranges based on the item's type:
//
// | Item type | ID range  | Notes
// | ----------|-----------|-------------------------
// | Types     | 1000-1999 |                        |
// | Funcs     | 2000-2999 |                        |
// | Logs      | 3000-3999 |                        |
// | Tables    | 4000-4999 |                        |
// | Views     | 5000-5999 |                        |
// | Indexes   | 1000000+  | Dynamically allocated  |
//
// WARNING: if you change the definition of an existing builtin item, you must
// be careful to maintain backwards compatibility! Adding new columns is safe.
// Removing a column, changing the name of a column, or changing the type of a
// column is not safe, as persisted user views may depend upon that column.

pub const FIRST_SYSTEM_INDEX_ID: u64 = 1000000;

// The following types are the list of builtin data types available
// in Materialize. This list is derived from the `pg_type` table in PostgreSQL.
//
// Builtin types cannot be created, updated, or deleted. Their OIDs
// are static, unlike other objects, to match the type OIDs defined by Postgres.

pub const TYPE_BOOL: BuiltinType = BuiltinType {
    name: "bool",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1000),
    oid: 16,
    details: CatalogTypeDetails {
        typ: CatalogType::Bool,
        array_id: Some(GlobalId::System(1008)),
    },
};

pub const TYPE_BYTEA: BuiltinType = BuiltinType {
    name: "bytea",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1001),
    oid: 17,
    details: CatalogTypeDetails {
        typ: CatalogType::Bytes,
        array_id: Some(GlobalId::System(1009)),
    },
};

pub const TYPE_INT8: BuiltinType = BuiltinType {
    name: "int8",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1002),
    oid: 20,
    details: CatalogTypeDetails {
        typ: CatalogType::Int64,
        array_id: Some(GlobalId::System(1012)),
    },
};

pub const TYPE_INT4: BuiltinType = BuiltinType {
    name: "int4",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1003),
    oid: 23,
    details: CatalogTypeDetails {
        typ: CatalogType::Int32,
        array_id: Some(GlobalId::System(1010)),
    },
};

pub const TYPE_TEXT: BuiltinType = BuiltinType {
    name: "text",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1004),
    oid: 25,
    details: CatalogTypeDetails {
        typ: CatalogType::String,
        array_id: Some(GlobalId::System(1011)),
    },
};

pub const TYPE_OID: BuiltinType = BuiltinType {
    name: "oid",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1005),
    oid: 26,
    details: CatalogTypeDetails {
        typ: CatalogType::Oid,
        array_id: Some(GlobalId::System(1015)),
    },
};

pub const TYPE_FLOAT4: BuiltinType = BuiltinType {
    name: "float4",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1006),
    oid: 700,
    details: CatalogTypeDetails {
        typ: CatalogType::Float32,
        array_id: Some(GlobalId::System(1013)),
    },
};

pub const TYPE_FLOAT8: BuiltinType = BuiltinType {
    name: "float8",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1007),
    oid: 701,
    details: CatalogTypeDetails {
        typ: CatalogType::Float64,
        array_id: Some(GlobalId::System(1014)),
    },
};

pub const TYPE_BOOL_ARRAY: BuiltinType = BuiltinType {
    name: "_bool",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1008),
    oid: 1000,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1000),
        },
        array_id: None,
    },
};

pub const TYPE_BYTEA_ARRAY: BuiltinType = BuiltinType {
    name: "_bytea",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1009),
    oid: 1001,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1001),
        },
        array_id: None,
    },
};

pub const TYPE_INT4_ARRAY: BuiltinType = BuiltinType {
    name: "_int4",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1010),
    oid: 1007,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1003),
        },
        array_id: None,
    },
};

pub const TYPE_TEXT_ARRAY: BuiltinType = BuiltinType {
    name: "_text",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1011),
    oid: 1009,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1004),
        },
        array_id: None,
    },
};

pub const TYPE_INT8_ARRAY: BuiltinType = BuiltinType {
    name: "_int8",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1012),
    oid: 1016,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1002),
        },
        array_id: None,
    },
};

pub const TYPE_FLOAT4_ARRAY: BuiltinType = BuiltinType {
    name: "_float4",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1013),
    oid: 1021,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1006),
        },
        array_id: None,
    },
};

pub const TYPE_FLOAT8_ARRAY: BuiltinType = BuiltinType {
    name: "_float8",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1014),
    oid: 1022,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1007),
        },
        array_id: None,
    },
};

pub const TYPE_OID_ARRAY: BuiltinType = BuiltinType {
    name: "_oid",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1015),
    oid: 1028,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1005),
        },
        array_id: None,
    },
};

pub const TYPE_DATE: BuiltinType = BuiltinType {
    name: "date",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1016),
    oid: 1082,
    details: CatalogTypeDetails {
        typ: CatalogType::Date,
        array_id: Some(GlobalId::System(1020)),
    },
};

pub const TYPE_TIME: BuiltinType = BuiltinType {
    name: "time",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1017),
    oid: 1083,
    details: CatalogTypeDetails {
        typ: CatalogType::Time,
        array_id: Some(GlobalId::System(1021)),
    },
};

pub const TYPE_TIMESTAMP: BuiltinType = BuiltinType {
    name: "timestamp",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1018),
    oid: 1114,
    details: CatalogTypeDetails {
        typ: CatalogType::Timestamp,
        array_id: Some(GlobalId::System(1019)),
    },
};

pub const TYPE_TIMESTAMP_ARRAY: BuiltinType = BuiltinType {
    name: "_timestamp",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1019),
    oid: 1115,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1018),
        },
        array_id: None,
    },
};

pub const TYPE_DATE_ARRAY: BuiltinType = BuiltinType {
    name: "_date",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1020),
    oid: 1182,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1016),
        },
        array_id: None,
    },
};

pub const TYPE_TIME_ARRAY: BuiltinType = BuiltinType {
    name: "_time",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1021),
    oid: 1183,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1017),
        },
        array_id: None,
    },
};

pub const TYPE_TIMESTAMPTZ: BuiltinType = BuiltinType {
    name: "timestamptz",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1022),
    oid: 1184,
    details: CatalogTypeDetails {
        typ: CatalogType::TimestampTz,
        array_id: Some(GlobalId::System(1023)),
    },
};

pub const TYPE_TIMESTAMPTZ_ARRAY: BuiltinType = BuiltinType {
    name: "_timestamptz",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1023),
    oid: 1185,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1022),
        },
        array_id: None,
    },
};

pub const TYPE_INTERVAL: BuiltinType = BuiltinType {
    name: "interval",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1024),
    oid: 1186,
    details: CatalogTypeDetails {
        typ: CatalogType::Interval,
        array_id: Some(GlobalId::System(1025)),
    },
};

pub const TYPE_INTERVAL_ARRAY: BuiltinType = BuiltinType {
    name: "_interval",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1025),
    oid: 1187,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1024),
        },
        array_id: None,
    },
};

pub const TYPE_NUMERIC: BuiltinType = BuiltinType {
    name: "numeric",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1026),
    oid: 1700,
    details: CatalogTypeDetails {
        typ: CatalogType::Numeric,
        array_id: Some(GlobalId::System(1027)),
    },
};

pub const TYPE_NUMERIC_ARRAY: BuiltinType = BuiltinType {
    name: "_numeric",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1027),
    oid: 1231,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1026),
        },
        array_id: None,
    },
};

pub const TYPE_RECORD: BuiltinType = BuiltinType {
    name: "record",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1028),
    oid: 2249,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: Some(GlobalId::System(1029)),
    },
};

pub const TYPE_RECORD_ARRAY: BuiltinType = BuiltinType {
    name: "_record",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1029),
    oid: 2287,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1028),
        },
        array_id: None,
    },
};

pub const TYPE_UUID: BuiltinType = BuiltinType {
    name: "uuid",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1030),
    oid: 2950,
    details: CatalogTypeDetails {
        typ: CatalogType::Uuid,
        array_id: Some(GlobalId::System(1031)),
    },
};

pub const TYPE_UUID_ARRAY: BuiltinType = BuiltinType {
    name: "_uuid",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1031),
    oid: 2951,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1030),
        },
        array_id: None,
    },
};

pub const TYPE_JSONB: BuiltinType = BuiltinType {
    name: "jsonb",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1032),
    oid: 3802,
    details: CatalogTypeDetails {
        typ: CatalogType::Jsonb,
        array_id: Some(GlobalId::System(1033)),
    },
};

pub const TYPE_JSONB_ARRAY: BuiltinType = BuiltinType {
    name: "_jsonb",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1033),
    oid: 3807,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1032),
        },
        array_id: None,
    },
};

pub const TYPE_ANY: BuiltinType = BuiltinType {
    name: "any",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1034),
    oid: 2276,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYARRAY: BuiltinType = BuiltinType {
    name: "anyarray",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1035),
    oid: 2277,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYELEMENT: BuiltinType = BuiltinType {
    name: "anyelement",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1036),
    oid: 2283,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_ANYNONARRAY: BuiltinType = BuiltinType {
    name: "anynonarray",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1037),
    oid: 2776,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_CHAR: BuiltinType = BuiltinType {
    name: "char",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1038),
    oid: 18,
    details: CatalogTypeDetails {
        typ: CatalogType::PgLegacyChar,
        array_id: Some(GlobalId::System(1043)),
    },
};

pub const TYPE_VARCHAR: BuiltinType = BuiltinType {
    name: "varchar",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1039),
    oid: 1043,
    details: CatalogTypeDetails {
        typ: CatalogType::VarChar,
        array_id: Some(GlobalId::System(1044)),
    },
};

pub const TYPE_INT2: BuiltinType = BuiltinType {
    name: "int2",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1040),
    oid: 21,
    details: CatalogTypeDetails {
        typ: CatalogType::Int16,
        array_id: Some(GlobalId::System(1041)),
    },
};

pub const TYPE_INT2_ARRAY: BuiltinType = BuiltinType {
    name: "_int2",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1041),
    oid: 1005,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1040),
        },
        array_id: None,
    },
};

pub const TYPE_BPCHAR: BuiltinType = BuiltinType {
    name: "bpchar",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1042),
    oid: 1042,
    details: CatalogTypeDetails {
        typ: CatalogType::Char,
        array_id: Some(GlobalId::System(1045)),
    },
};

pub const TYPE_CHAR_ARRAY: BuiltinType = BuiltinType {
    name: "_char",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1043),
    oid: 1002,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1038),
        },
        array_id: None,
    },
};

pub const TYPE_VARCHAR_ARRAY: BuiltinType = BuiltinType {
    name: "_varchar",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1044),
    oid: 1015,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1039),
        },
        array_id: None,
    },
};

pub const TYPE_BPCHAR_ARRAY: BuiltinType = BuiltinType {
    name: "_bpchar",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1045),
    oid: 1014,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1042),
        },
        array_id: None,
    },
};

pub const TYPE_REGPROC: BuiltinType = BuiltinType {
    name: "regproc",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1046),
    oid: 24,
    details: CatalogTypeDetails {
        typ: CatalogType::RegProc,
        array_id: Some(GlobalId::System(1047)),
    },
};

pub const TYPE_REGPROC_ARRAY: BuiltinType = BuiltinType {
    name: "_regproc",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1047),
    oid: 1008,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1046),
        },
        array_id: None,
    },
};

pub const TYPE_REGTYPE: BuiltinType = BuiltinType {
    name: "regtype",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1048),
    oid: 2206,
    details: CatalogTypeDetails {
        typ: CatalogType::RegType,
        array_id: Some(GlobalId::System(1049)),
    },
};

pub const TYPE_REGTYPE_ARRAY: BuiltinType = BuiltinType {
    name: "_regtype",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1049),
    oid: 2211,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1048),
        },
        array_id: None,
    },
};

pub const TYPE_REGCLASS: BuiltinType = BuiltinType {
    name: "regclass",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1050),
    oid: 2205,
    details: CatalogTypeDetails {
        typ: CatalogType::RegClass,
        array_id: Some(GlobalId::System(1051)),
    },
};

pub const TYPE_REGCLASS_ARRAY: BuiltinType = BuiltinType {
    name: "_regclass",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1051),
    oid: 2210,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1050),
        },
        array_id: None,
    },
};

pub const TYPE_INT2_VECTOR: BuiltinType = BuiltinType {
    name: "int2vector",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1052),
    oid: 22,
    details: CatalogTypeDetails {
        typ: CatalogType::Int2Vector,
        array_id: Some(GlobalId::System(1053)),
    },
};

pub const TYPE_INT2_VECTOR_ARRAY: BuiltinType = BuiltinType {
    name: "_int2vector",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1053),
    oid: 1006,
    details: CatalogTypeDetails {
        typ: CatalogType::Array {
            element_id: GlobalId::System(1052),
        },
        array_id: None,
    },
};

pub const TYPE_LIST: BuiltinType = BuiltinType {
    name: "list",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1998),
    oid: mz_pgrepr::oid::TYPE_LIST_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const TYPE_MAP: BuiltinType = BuiltinType {
    name: "map",
    schema: PG_CATALOG_SCHEMA,
    id: GlobalId::System(1999),
    oid: mz_pgrepr::oid::TYPE_MAP_OID,
    details: CatalogTypeDetails {
        typ: CatalogType::Pseudo,
        array_id: None,
    },
};

pub const MZ_DATAFLOW_OPERATORS: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_operators",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Operates),
    id: GlobalId::System(3000),
};

pub const MZ_DATAFLOW_OPERATORS_ADDRESSES: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_operator_addresses",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Addresses),
    id: GlobalId::System(3002),
};

pub const MZ_DATAFLOW_CHANNELS: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_channels",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Channels),
    id: GlobalId::System(3004),
};

pub const MZ_SCHEDULING_ELAPSED_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_scheduling_elapsed_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Elapsed),
    id: GlobalId::System(3006),
};

pub const MZ_SCHEDULING_HISTOGRAM_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_scheduling_histogram_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Histogram),
    id: GlobalId::System(3008),
};

pub const MZ_SCHEDULING_PARKS_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_scheduling_parks_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Parks),
    id: GlobalId::System(3010),
};

pub const MZ_ARRANGEMENT_BATCHES_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_arrangement_batches_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Differential(DifferentialLog::ArrangementBatches),
    id: GlobalId::System(3012),
};

pub const MZ_ARRANGEMENT_SHARING_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_arrangement_sharing_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Differential(DifferentialLog::Sharing),
    id: GlobalId::System(3014),
};

pub const MZ_MATERIALIZATIONS: BuiltinLog = BuiltinLog {
    name: "mz_materializations",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Materialized(MaterializedLog::DataflowCurrent),
    id: GlobalId::System(3016),
};

pub const MZ_MATERIALIZATION_DEPENDENCIES: BuiltinLog = BuiltinLog {
    name: "mz_materialization_dependencies",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Materialized(MaterializedLog::DataflowDependency),
    id: GlobalId::System(3018),
};

pub const MZ_WORKER_MATERIALIZATION_FRONTIERS: BuiltinLog = BuiltinLog {
    name: "mz_worker_materialization_frontiers",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Materialized(MaterializedLog::FrontierCurrent),
    id: GlobalId::System(3020),
};

pub const MZ_PEEK_ACTIVE: BuiltinLog = BuiltinLog {
    name: "mz_peek_active",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Materialized(MaterializedLog::PeekCurrent),
    id: GlobalId::System(3022),
};

pub const MZ_PEEK_DURATIONS: BuiltinLog = BuiltinLog {
    name: "mz_peek_durations",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Materialized(MaterializedLog::PeekDuration),
    id: GlobalId::System(3024),
};

pub const MZ_SOURCE_INFO: BuiltinLog = BuiltinLog {
    name: "mz_source_info",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Materialized(MaterializedLog::SourceInfo),
    id: GlobalId::System(3026),
};

pub const MZ_MESSAGE_COUNTS_RECEIVED_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_message_counts_received_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::MessagesReceived),
    id: GlobalId::System(3028),
};

pub const MZ_MESSAGE_COUNTS_SENT_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_message_counts_sent_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::MessagesSent),
    id: GlobalId::System(3036),
};

pub const MZ_DATAFLOW_OPERATOR_REACHABILITY_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_dataflow_operator_reachability_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Timely(TimelyLog::Reachability),
    id: GlobalId::System(3034),
};

pub const MZ_ARRANGEMENT_RECORDS_INTERNAL: BuiltinLog = BuiltinLog {
    name: "mz_arrangement_records_internal",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Differential(DifferentialLog::ArrangementRecords),
    id: GlobalId::System(3038),
};

pub const MZ_KAFKA_SOURCE_STATISTICS: BuiltinLog = BuiltinLog {
    name: "mz_kafka_source_statistics",
    schema: MZ_CATALOG_SCHEMA,
    variant: LogVariant::Materialized(MaterializedLog::KafkaSourceStatistics),
    id: GlobalId::System(3040),
};

// Next id BuiltinLog: 3042

lazy_static! {
    pub static ref MZ_VIEW_KEYS: BuiltinTable = BuiltinTable {
        name: "mz_view_keys",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("global_id", ScalarType::String.nullable(false))
            .with_column("column", ScalarType::Int64.nullable(false))
            .with_column("key_group", ScalarType::Int64.nullable(false)),
        id: GlobalId::System(4001),
        persistent: false,
    };
    pub static ref MZ_VIEW_FOREIGN_KEYS: BuiltinTable = BuiltinTable {
        name: "mz_view_foreign_keys",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("child_id", ScalarType::String.nullable(false))
            .with_column("child_column", ScalarType::Int64.nullable(false))
            .with_column("parent_id", ScalarType::String.nullable(false))
            .with_column("parent_column", ScalarType::Int64.nullable(false))
            .with_column("key_group", ScalarType::Int64.nullable(false))
            .with_key(vec![0, 1, 4]), // TODO: explain why this is a key.
        id: GlobalId::System(4003),
        persistent: false,
    };
    pub static ref MZ_KAFKA_SINKS: BuiltinTable = BuiltinTable {
        name: "mz_kafka_sinks",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("sink_id", ScalarType::String.nullable(false))
            .with_column("topic", ScalarType::String.nullable(false))
            .with_column("consistency_topic", ScalarType::String.nullable(true))
            .with_key(vec![0]),
        id: GlobalId::System(4005),
        persistent: false,
    };
    pub static ref MZ_AVRO_OCF_SINKS: BuiltinTable = BuiltinTable {
        name: "mz_avro_ocf_sinks",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("sink_id", ScalarType::String.nullable(false))
            .with_column("path", ScalarType::Bytes.nullable(false))
            .with_key(vec![0]),
        id: GlobalId::System(4007),
        persistent: false,
    };
    pub static ref MZ_DATABASES: BuiltinTable = BuiltinTable {
        name: "mz_databases",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::Int64.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("name", ScalarType::String.nullable(false)),
        id: GlobalId::System(4009),
        persistent: false,
    };
    pub static ref MZ_SCHEMAS: BuiltinTable = BuiltinTable {
        name: "mz_schemas",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::Int64.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("database_id", ScalarType::Int64.nullable(true))
            .with_column("name", ScalarType::String.nullable(false)),
        id: GlobalId::System(4011),
        persistent: false,
    };
    pub static ref MZ_COLUMNS: BuiltinTable = BuiltinTable {
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
        id: GlobalId::System(4013),
        persistent: false,
    };
    pub static ref MZ_INDEXES: BuiltinTable = BuiltinTable {
        name: "mz_indexes",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::String.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("on_id", ScalarType::String.nullable(false))
            .with_column("volatility", ScalarType::String.nullable(false))
            .with_column("enabled", ScalarType::Bool.nullable(false))
            .with_column("cluster_id", ScalarType::Int64.nullable(false)),
        id: GlobalId::System(4015),
        persistent: false,
    };
    pub static ref MZ_INDEX_COLUMNS: BuiltinTable = BuiltinTable {
        name: "mz_index_columns",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("index_id", ScalarType::String.nullable(false))
            .with_column("index_position", ScalarType::Int64.nullable(false))
            .with_column("on_position", ScalarType::Int64.nullable(true))
            .with_column("on_expression", ScalarType::String.nullable(true))
            .with_column("nullable", ScalarType::Bool.nullable(false)),
        id: GlobalId::System(4017),
        persistent: false,
    };
    pub static ref MZ_TABLES: BuiltinTable = BuiltinTable {
        name: "mz_tables",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::String.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("schema_id", ScalarType::Int64.nullable(false))
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("persisted_name", ScalarType::String.nullable(true)),
        id: GlobalId::System(4019),
        persistent: false,
    };
    pub static ref MZ_SOURCES: BuiltinTable = BuiltinTable {
        name: "mz_sources",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::String.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("schema_id", ScalarType::Int64.nullable(false))
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("connector_type", ScalarType::String.nullable(false))
            .with_column("volatility", ScalarType::String.nullable(false))
            .with_column("persisted_name", ScalarType::String.nullable(true)),
        id: GlobalId::System(4021),
        persistent: false,
    };
    pub static ref MZ_SINKS: BuiltinTable = BuiltinTable {
        name: "mz_sinks",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::String.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("schema_id", ScalarType::Int64.nullable(false))
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("connector_type", ScalarType::String.nullable(false))
            .with_column("volatility", ScalarType::String.nullable(false))
            .with_column("cluster_id", ScalarType::Int64.nullable(false)),
        id: GlobalId::System(4023),
        persistent: false,
    };
    pub static ref MZ_VIEWS: BuiltinTable = BuiltinTable {
        name: "mz_views",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::String.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("schema_id", ScalarType::Int64.nullable(false))
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("volatility", ScalarType::String.nullable(false)),
        id: GlobalId::System(4025),
        persistent: false,
    };
    pub static ref MZ_TYPES: BuiltinTable = BuiltinTable {
        name: "mz_types",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::String.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("schema_id", ScalarType::Int64.nullable(false))
            .with_column("name", ScalarType::String.nullable(false)),
        id: GlobalId::System(4027),
        persistent: false,
    };
    pub static ref MZ_ARRAY_TYPES: BuiltinTable = BuiltinTable {
        name: "mz_array_types",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("type_id", ScalarType::String.nullable(false))
            .with_column("element_id", ScalarType::String.nullable(false)),
            id: GlobalId::System(4029),
            persistent: false,
    };
    pub static ref MZ_BASE_TYPES: BuiltinTable = BuiltinTable {
        name: "mz_base_types",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("type_id", ScalarType::String.nullable(false)),
            id: GlobalId::System(4031),
            persistent: false,
    };
    pub static ref MZ_LIST_TYPES: BuiltinTable = BuiltinTable {
        name: "mz_list_types",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("type_id", ScalarType::String.nullable(false))
            .with_column("element_id", ScalarType::String.nullable(false)),
            id: GlobalId::System(4033),
            persistent: false,
    };
    pub static ref MZ_MAP_TYPES: BuiltinTable = BuiltinTable {
        name: "mz_map_types",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("type_id", ScalarType::String.nullable(false))
            .with_column("key_id", ScalarType::String.nullable(false))
            .with_column("value_id", ScalarType::String.nullable(false)),
            id: GlobalId::System(4035),
            persistent: false,
    };
    pub static ref MZ_ROLES: BuiltinTable = BuiltinTable {
        name: "mz_roles",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::Int64.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("name", ScalarType::String.nullable(false)),
        id: GlobalId::System(4037),
        persistent: false,
    };
    pub static ref MZ_PSEUDO_TYPES: BuiltinTable = BuiltinTable {
        name: "mz_pseudo_types",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("type_id", ScalarType::String.nullable(false)),
        id: GlobalId::System(4039),
        persistent: false,
    };
    pub static ref MZ_FUNCTIONS: BuiltinTable = BuiltinTable {
        name: "mz_functions",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::String.nullable(false))
            .with_column("oid", ScalarType::Oid.nullable(false))
            .with_column("schema_id", ScalarType::Int64.nullable(false))
            .with_column("name", ScalarType::String.nullable(false))
            .with_column("arg_ids", ScalarType::Array(Box::new(ScalarType::String)).nullable(false))
            .with_column("variadic_id", ScalarType::String.nullable(true))
            .with_column("ret_id", ScalarType::String.nullable(true))
            .with_column("ret_set", ScalarType::Bool.nullable(false)),
        id: GlobalId::System(4041),
        persistent: false,
    };
    pub static ref MZ_PROMETHEUS_READINGS: BuiltinTable = BuiltinTable {
        name: "mz_metrics",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
                .with_column("metric", ScalarType::String.nullable(false))
                .with_column("time", ScalarType::TimestampTz.nullable(false))
                .with_column("labels", ScalarType::Jsonb.nullable(false))
                .with_column("value", ScalarType::Float64.nullable(false))
                .with_key(vec![0, 1, 2]),
        // NB: Until the end of our persisted system tables experiment, give
        // persist team a heads up if you change this id, please!
        id: GlobalId::System(4043),
        // Note that the `system_table_enabled` field of PersistConfig (hooked
        // up to --disable-persistent-system-tables-test) also has to be true
        // for this to be persisted.
        persistent: true,
    };
    pub static ref MZ_PROMETHEUS_METRICS: BuiltinTable = BuiltinTable {
        name: "mz_metrics_meta",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
                .with_column("metric", ScalarType::String.nullable(false))
                .with_column("type", ScalarType::String.nullable(false))
                .with_column("help", ScalarType::String.nullable(false))
                .with_key(vec![0]),
        id: GlobalId::System(4045),
        persistent: false,
    };
    pub static ref MZ_PROMETHEUS_HISTOGRAMS: BuiltinTable = BuiltinTable {
        name: "mz_metric_histograms",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
                .with_column("metric", ScalarType::String.nullable(false))
                .with_column("time", ScalarType::Timestamp.nullable(false))
                .with_column("labels", ScalarType::Jsonb.nullable(false))
                .with_column("bound", ScalarType::Float64.nullable(false))
                .with_column("count", ScalarType::Int64.nullable(false))
                .with_key(vec![0, 1, 2]),
        // NB: Until the end of our persisted system tables experiment, give
        // persist team a heads up if you change this id, please!
        id: GlobalId::System(4047),
        // Note that the `system_table_enabled` field of PersistConfig (hooked
        // up to --disable-persistent-system-tables-test) also has to be true
        // for this to be persisted.
        persistent: true,
    };
    pub static ref MZ_CLUSTERS: BuiltinTable = BuiltinTable {
        name: "mz_clusters",
        schema: MZ_CATALOG_SCHEMA,
        desc: RelationDesc::empty()
            .with_column("id", ScalarType::Int64.nullable(false))
            .with_column("name", ScalarType::String.nullable(false)),
        id: GlobalId::System(4049),
        persistent: false,
    };
}

pub const MZ_RELATIONS: BuiltinView = BuiltinView {
    name: "mz_relations",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_relations (id, oid, schema_id, name, type) AS
      SELECT id, oid, schema_id, name, 'table' FROM mz_catalog.mz_tables
UNION SELECT id, oid, schema_id, name, 'source' FROM mz_catalog.mz_sources
UNION SELECT id, oid, schema_id, name, 'view' FROM mz_catalog.mz_views",
    id: GlobalId::System(5000),
};

pub const MZ_OBJECTS: BuiltinView = BuiltinView {
    name: "mz_objects",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_objects (id, oid, schema_id, name, type) AS
    SELECT id, oid, schema_id, name, type FROM mz_catalog.mz_relations
UNION
    SELECT id, oid, schema_id, name, 'sink' FROM mz_catalog.mz_sinks
UNION
    SELECT mz_indexes.id, mz_indexes.oid, schema_id, mz_indexes.name, 'index'
    FROM mz_catalog.mz_indexes
    JOIN mz_catalog.mz_relations ON mz_indexes.on_id = mz_relations.id",
    id: GlobalId::System(5001),
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
    sql: "CREATE VIEW mz_catalog_names AS SELECT
    o.id AS global_id,
    coalesce(d.name || '.', '') || s.name || '.' || o.name AS name
FROM mz_catalog.mz_objects o
JOIN mz_catalog.mz_schemas s ON s.id = o.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id",
    id: GlobalId::System(5002),
};

pub const MZ_DATAFLOW_NAMES: BuiltinView = BuiltinView {
    name: "mz_dataflow_names",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_dataflow_names AS SELECT
    mz_dataflow_operator_addresses.id,
    mz_dataflow_operator_addresses.worker,
    mz_dataflow_operator_addresses.address[1] AS local_id,
    mz_dataflow_operators.name
FROM
    mz_catalog.mz_dataflow_operator_addresses,
    mz_catalog.mz_dataflow_operators
WHERE
    mz_dataflow_operator_addresses.id = mz_dataflow_operators.id AND
    mz_dataflow_operator_addresses.worker = mz_dataflow_operators.worker AND
    mz_catalog.list_length(mz_dataflow_operator_addresses.address) = 1",
    id: GlobalId::System(5003),
};

pub const MZ_DATAFLOW_OPERATOR_DATAFLOWS: BuiltinView = BuiltinView {
    name: "mz_dataflow_operator_dataflows",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_dataflow_operator_dataflows AS SELECT
    mz_dataflow_operators.id,
    mz_dataflow_operators.name,
    mz_dataflow_operators.worker,
    mz_dataflow_names.id as dataflow_id,
    mz_dataflow_names.name as dataflow_name
FROM
    mz_catalog.mz_dataflow_operators,
    mz_catalog.mz_dataflow_operator_addresses,
    mz_catalog.mz_dataflow_names
WHERE
    mz_dataflow_operators.id = mz_dataflow_operator_addresses.id AND
    mz_dataflow_operators.worker = mz_dataflow_operator_addresses.worker AND
    mz_dataflow_names.local_id = mz_dataflow_operator_addresses.address[1] AND
    mz_dataflow_names.worker = mz_dataflow_operator_addresses.worker",
    id: GlobalId::System(5004),
};

pub const MZ_MATERIALIZATION_FRONTIERS: BuiltinView = BuiltinView {
    name: "mz_materialization_frontiers",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_materialization_frontiers AS SELECT
    global_id, pg_catalog.min(time) AS time
FROM mz_catalog.mz_worker_materialization_frontiers
GROUP BY global_id",
    id: GlobalId::System(5005),
};

pub const MZ_RECORDS_PER_DATAFLOW_OPERATOR: BuiltinView = BuiltinView {
    name: "mz_records_per_dataflow_operator",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_records_per_dataflow_operator AS
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
    id: GlobalId::System(5006),
};

pub const MZ_RECORDS_PER_DATAFLOW: BuiltinView = BuiltinView {
    name: "mz_records_per_dataflow",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_records_per_dataflow AS SELECT
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
    id: GlobalId::System(5007),
};

pub const MZ_RECORDS_PER_DATAFLOW_GLOBAL: BuiltinView = BuiltinView {
    name: "mz_records_per_dataflow_global",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_records_per_dataflow_global AS SELECT
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name,
    pg_catalog.SUM(mz_records_per_dataflow.records) as records
FROM
    mz_catalog.mz_records_per_dataflow
GROUP BY
    mz_records_per_dataflow.id,
    mz_records_per_dataflow.name",
    id: GlobalId::System(5008),
};

pub const MZ_PERF_ARRANGEMENT_RECORDS: BuiltinView = BuiltinView {
    name: "mz_perf_arrangement_records",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_perf_arrangement_records AS
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
    id: GlobalId::System(5009),
};

pub const MZ_PERF_PEEK_DURATIONS_CORE: BuiltinView = BuiltinView {
    name: "mz_perf_peek_durations_core",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_perf_peek_durations_core AS SELECT
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
    id: GlobalId::System(5010),
};

pub const MZ_PERF_PEEK_DURATIONS_BUCKET: BuiltinView = BuiltinView {
    name: "mz_perf_peek_durations_bucket",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_perf_peek_durations_bucket AS
(
    SELECT * FROM mz_catalog.mz_perf_peek_durations_core
) UNION (
    SELECT worker, '+Inf', pg_catalog.max(count) AS count FROM mz_catalog.mz_perf_peek_durations_core
    GROUP BY worker
)",
    id: GlobalId::System(5011),
};

pub const MZ_PERF_PEEK_DURATIONS_AGGREGATES: BuiltinView = BuiltinView {
    name: "mz_perf_peek_durations_aggregates",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_perf_peek_durations_aggregates AS SELECT worker, pg_catalog.sum(duration_ns * count) AS sum, pg_catalog.sum(count) AS count
FROM mz_catalog.mz_peek_durations lpd
GROUP BY worker",
    id: GlobalId::System(5012),
};

pub const MZ_PERF_DEPENDENCY_FRONTIERS: BuiltinView = BuiltinView {
    name: "mz_perf_dependency_frontiers",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_perf_dependency_frontiers AS SELECT DISTINCT
    mcn.name AS dataflow,
    mcn_source.name AS source,
    frontier_source.time - frontier_df.time AS lag_ms
FROM mz_catalog.mz_materialization_dependencies index_deps
JOIN mz_catalog.mz_materialization_frontiers frontier_source ON index_deps.source = frontier_source.global_id
JOIN mz_catalog.mz_materialization_frontiers frontier_df ON index_deps.dataflow = frontier_df.global_id
JOIN mz_catalog.mz_catalog_names mcn ON mcn.global_id = index_deps.dataflow
JOIN mz_catalog.mz_catalog_names mcn_source ON mcn_source.global_id = frontier_source.global_id
UNION
SELECT DISTINCT
    mcn.name AS dataflow,
    mcn_source.name AS source,
    CASE WHEN source_info.time < frontier_df.time THEN 0 ELSE source_info.time - frontier_df.time END AS lag_ms
FROM mz_catalog.mz_materialization_dependencies index_deps
JOIN (SELECT source_id, pg_catalog.MAX(timestamp) time FROM mz_catalog.mz_source_info GROUP BY source_id) source_info ON index_deps.source = source_info.source_id
JOIN mz_catalog.mz_materialization_frontiers frontier_df ON index_deps.dataflow = frontier_df.global_id
JOIN mz_catalog.mz_catalog_names mcn ON mcn.global_id = index_deps.dataflow
JOIN mz_catalog.mz_catalog_names mcn_source ON mcn_source.global_id = source_info.source_id",
    id: GlobalId::System(5013),
};

pub const PG_NAMESPACE: BuiltinView = BuiltinView {
    name: "pg_namespace",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_namespace AS SELECT
s.oid AS oid,
s.name AS nspname,
NULL::pg_catalog.oid AS nspowner,
NULL::pg_catalog.text[] AS nspacl
FROM mz_catalog.mz_schemas s
JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())",
    id: GlobalId::System(5014),
};

pub const PG_CLASS: BuiltinView = BuiltinView {
    name: "pg_class",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_class AS SELECT
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
    id: GlobalId::System(5015),
};

pub const PG_DATABASE: BuiltinView = BuiltinView {
    name: "pg_database",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_database AS SELECT
    oid,
    name as datname,
    NULL::pg_catalog.oid AS datdba,
    6 as encoding,
    'C' as datcollate,
    'C' as datctype,
    NULL::pg_catalog.text[] as datacl
FROM mz_catalog.mz_databases d
WHERE (d.id IS NULL OR d.name = pg_catalog.current_database())",
    id: GlobalId::System(5016),
};

pub const PG_INDEX: BuiltinView = BuiltinView {
    name: "pg_index",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_index AS SELECT
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
    id: GlobalId::System(5017),
};

pub const PG_DESCRIPTION: BuiltinView = BuiltinView {
    name: "pg_description",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_description AS SELECT
    c.oid as objoid,
    NULL::pg_catalog.oid as classoid,
    0::pg_catalog.int4 as objsubid,
    NULL::pg_catalog.text as description
FROM pg_catalog.pg_class c
JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())",
    id: GlobalId::System(5018),
};

pub const PG_TYPE: BuiltinView = BuiltinView {
    name: "pg_type",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_type AS SELECT
    mz_types.oid,
    mz_types.name AS typname,
    mz_schemas.oid AS typnamespace,
    NULL::pg_catalog.int2 AS typlen,
    -- 'a' is used internally to denote an array type, but in postgres they show up
    -- as 'b'.
    (CASE mztype WHEN 'a' THEN 'b' ELSE mztype END)::pg_catalog.char AS typtype,
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
    id: GlobalId::System(5019),
};

pub const PG_ATTRIBUTE: BuiltinView = BuiltinView {
    name: "pg_attribute",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_attribute AS SELECT
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
    id: GlobalId::System(5020),
};

pub const PG_PROC: BuiltinView = BuiltinView {
    name: "pg_proc",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_proc AS SELECT
    mz_functions.oid,
    mz_functions.name AS proname,
    mz_schemas.oid AS pronamespace,
    NULL::pg_catalog.text AS proargdefaults
FROM mz_catalog.mz_functions
JOIN mz_catalog.mz_schemas ON mz_functions.schema_id = mz_schemas.id
JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())",
    id: GlobalId::System(5021),
};

pub const PG_RANGE: BuiltinView = BuiltinView {
    name: "pg_range",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_range AS SELECT
    NULL::pg_catalog.oid AS rngtypid,
    NULL::pg_catalog.oid AS rngsubtype
WHERE false",
    id: GlobalId::System(5022),
};

pub const PG_ENUM: BuiltinView = BuiltinView {
    name: "pg_enum",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_enum AS SELECT
    NULL::pg_catalog.oid AS oid,
    NULL::pg_catalog.oid AS enumtypid,
    NULL::pg_catalog.float4 AS enumsortorder,
    NULL::pg_catalog.text AS enumlabel
WHERE false",
    id: GlobalId::System(5023),
};

pub const PG_ATTRDEF: BuiltinView = BuiltinView {
    name: "pg_attrdef",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_attrdef AS SELECT
    NULL::pg_catalog.oid AS oid,
    mz_objects.oid AS adrelid,
    mz_columns.position AS adnum,
    mz_columns.default AS adbin,
    mz_columns.default AS adsrc
FROM mz_catalog.mz_columns
    JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())
    JOIN mz_catalog.mz_objects ON mz_columns.id = mz_objects.id
WHERE default IS NOT NULL",
    id: GlobalId::System(5025),
};

pub const PG_SETTINGS: BuiltinView = BuiltinView {
    name: "pg_settings",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_settings AS SELECT
    *
FROM (VALUES
    ('max_index_keys'::pg_catalog.text, '1000'::pg_catalog.text)
) AS _ (name, setting)",
    id: GlobalId::System(5026),
};

pub const MZ_SCHEDULING_ELAPSED: BuiltinView = BuiltinView {
    name: "mz_scheduling_elapsed",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_scheduling_elapsed AS SELECT
    id, worker, pg_catalog.count(*) AS elapsed_ns
FROM
    mz_catalog.mz_scheduling_elapsed_internal
GROUP BY
    id, worker",
    id: GlobalId::System(5027),
};

pub const MZ_SCHEDULING_HISTOGRAM: BuiltinView = BuiltinView {
    name: "mz_scheduling_histogram",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_scheduling_histogram AS SELECT
    id, worker, duration_ns, pg_catalog.count(*) AS count
FROM
    mz_catalog.mz_scheduling_histogram_internal
GROUP BY
    id, worker, duration_ns",
    id: GlobalId::System(5028),
};

pub const MZ_SCHEDULING_PARKS: BuiltinView = BuiltinView {
    name: "mz_scheduling_parks",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_scheduling_parks AS SELECT
    worker, slept_for, requested, pg_catalog.count(*) AS count
FROM
    mz_catalog.mz_scheduling_parks_internal
GROUP BY
    worker, slept_for, requested",
    id: GlobalId::System(5029),
};

pub const MZ_MESSAGE_COUNTS: BuiltinView = BuiltinView {
    name: "mz_message_counts",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_message_counts AS
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
    id: GlobalId::System(5030),
};

pub const MZ_DATAFLOW_OPERATOR_REACHABILITY: BuiltinView = BuiltinView {
    name: "mz_dataflow_operator_reachability",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_dataflow_operator_reachability AS SELECT
    address,
    port,
    worker,
    update_type,
    timestamp,
    pg_catalog.count(*) as count
FROM
    mz_catalog.mz_dataflow_operator_reachability_internal
GROUP BY address, port, worker, update_type, timestamp",
    id: GlobalId::System(5031),
};

pub const MZ_ARRANGEMENT_SIZES: BuiltinView = BuiltinView {
    name: "mz_arrangement_sizes",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_arrangement_sizes AS
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
    id: GlobalId::System(5032),
};

pub const MZ_ARRANGEMENT_SHARING: BuiltinView = BuiltinView {
    name: "mz_arrangement_sharing",
    schema: MZ_CATALOG_SCHEMA,
    sql: "CREATE VIEW mz_arrangement_sharing AS
SELECT
    operator,
    worker,
    pg_catalog.count(*) AS count
FROM mz_catalog.mz_arrangement_sharing_internal
GROUP BY operator, worker",
    id: GlobalId::System(5033),
};

// NOTE: If you add real data to this implementation, then please update
// the related `pg_` function implementations (like `pg_get_constraintdef`)
pub const PG_CONSTRAINT: BuiltinView = BuiltinView {
    name: "pg_constraint",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_constraint AS SELECT
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
    id: GlobalId::System(5034),
};

pub const PG_TABLES: BuiltinView = BuiltinView {
    name: "pg_tables",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_tables AS
SELECT n.nspname AS schemaname,
    c.relname AS tablename,
    pg_catalog.pg_get_userbyid(c.relowner) AS tableowner
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())
WHERE c.relkind = ANY (ARRAY['r','p'])",
    id: GlobalId::System(5035),
};

pub const PG_ACCESS_METHODS: BuiltinView = BuiltinView {
    name: "pg_am",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_am AS
SELECT NULL::pg_catalog.oid AS oid,
    NULL::pg_catalog.text AS amname,
    NULL::pg_catalog.regproc AS amhandler,
    NULL::pg_catalog.\"char\" AS amtype
WHERE false",
    id: GlobalId::System(5036),
};

pub const PG_ROLES: BuiltinView = BuiltinView {
    name: "pg_roles",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_roles AS SELECT
    r.name AS rolname,
    '********'::pg_catalog.text AS rolpassword,
    r.oid AS oid
FROM mz_catalog.mz_roles r
JOIN mz_catalog.mz_databases d ON (d.id IS NULL OR d.name = pg_catalog.current_database())",
    id: GlobalId::System(5037),
};

pub const PG_VIEWS: BuiltinView = BuiltinView {
    name: "pg_views",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_views AS SELECT
    s.name AS schemaname,
    v.name AS viewname,
    NULL::pg_catalog.oid AS viewowner
FROM mz_catalog.mz_views v
LEFT JOIN mz_catalog.mz_schemas s ON s.id = v.schema_id
LEFT JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE d.name = pg_catalog.current_database()",
    id: GlobalId::System(5038),
};

pub const INFORMATION_SCHEMA_COLUMNS: BuiltinView = BuiltinView {
    name: "columns",
    schema: INFORMATION_SCHEMA,
    sql: "CREATE VIEW columns AS
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
    id: GlobalId::System(5039),
};

pub const INFORMATION_SCHEMA_TABLES: BuiltinView = BuiltinView {
    name: "tables",
    schema: INFORMATION_SCHEMA,
    sql: "CREATE VIEW tables AS SELECT
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
    id: GlobalId::System(5040),
};

// MZ doesn't support COLLATE so the table is filled with NULLs and made empty. pg_database hard
// codes a collation of 'C' for every database, so we could copy that here.
pub const PG_COLLATION: BuiltinView = BuiltinView {
    name: "pg_collation",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_class
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
    id: GlobalId::System(5041),
};

// MZ doesn't support row level security policies so the table is filled in with NULLs and made empty.
pub const PG_POLICY: BuiltinView = BuiltinView {
    name: "pg_policy",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_class
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
    id: GlobalId::System(5042),
};

// MZ doesn't support table inheritance so the table is filled in with NULLs and made empty.
pub const PG_INHERITS: BuiltinView = BuiltinView {
    name: "pg_inherits",
    schema: PG_CATALOG_SCHEMA,
    sql: "CREATE VIEW pg_inherits
AS SELECT
    NULL::pg_catalog.oid AS inhrelid,
    NULL::pg_catalog.oid AS inhparent,
    NULL::pg_catalog.int4 AS inhseqno,
    NULL::pg_catalog.bool AS inhdetachpending
WHERE false",
    id: GlobalId::System(5043),
};

// Next id BuiltinView: 5044

pub const MZ_SYSTEM: BuiltinRole = BuiltinRole {
    name: "mz_system",
    id: -1,
};

lazy_static! {
    pub static ref BUILTINS: BTreeMap<GlobalId, Builtin> = {
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
            Builtin::Log(&MZ_ARRANGEMENT_SHARING_INTERNAL),
            Builtin::Log(&MZ_ARRANGEMENT_BATCHES_INTERNAL),
            Builtin::Log(&MZ_ARRANGEMENT_RECORDS_INTERNAL),
            Builtin::Log(&MZ_DATAFLOW_CHANNELS),
            Builtin::Log(&MZ_DATAFLOW_OPERATORS),
            Builtin::Log(&MZ_DATAFLOW_OPERATORS_ADDRESSES),
            Builtin::Log(&MZ_DATAFLOW_OPERATOR_REACHABILITY_INTERNAL),
            Builtin::Log(&MZ_KAFKA_SOURCE_STATISTICS),
            Builtin::Log(&MZ_MATERIALIZATIONS),
            Builtin::Log(&MZ_MATERIALIZATION_DEPENDENCIES),
            Builtin::Log(&MZ_MESSAGE_COUNTS_RECEIVED_INTERNAL),
            Builtin::Log(&MZ_MESSAGE_COUNTS_SENT_INTERNAL),
            Builtin::Log(&MZ_PEEK_ACTIVE),
            Builtin::Log(&MZ_PEEK_DURATIONS),
            Builtin::Log(&MZ_SCHEDULING_ELAPSED_INTERNAL),
            Builtin::Log(&MZ_SCHEDULING_HISTOGRAM_INTERNAL),
            Builtin::Log(&MZ_SCHEDULING_PARKS_INTERNAL),
            Builtin::Log(&MZ_SOURCE_INFO),
            Builtin::Log(&MZ_WORKER_MATERIALIZATION_FRONTIERS),
            Builtin::Table(&MZ_VIEW_KEYS),
            Builtin::Table(&MZ_VIEW_FOREIGN_KEYS),
            Builtin::Table(&MZ_KAFKA_SINKS),
            Builtin::Table(&MZ_AVRO_OCF_SINKS),
            Builtin::Table(&MZ_DATABASES),
            Builtin::Table(&MZ_SCHEMAS),
            Builtin::Table(&MZ_COLUMNS),
            Builtin::Table(&MZ_INDEXES),
            Builtin::Table(&MZ_INDEX_COLUMNS),
            Builtin::Table(&MZ_TABLES),
            Builtin::Table(&MZ_SOURCES),
            Builtin::Table(&MZ_SINKS),
            Builtin::Table(&MZ_VIEWS),
            Builtin::Table(&MZ_TYPES),
            Builtin::Table(&MZ_ARRAY_TYPES),
            Builtin::Table(&MZ_BASE_TYPES),
            Builtin::Table(&MZ_LIST_TYPES),
            Builtin::Table(&MZ_MAP_TYPES),
            Builtin::Table(&MZ_ROLES),
            Builtin::Table(&MZ_PSEUDO_TYPES),
            Builtin::Table(&MZ_FUNCTIONS),
            Builtin::Table(&MZ_PROMETHEUS_READINGS),
            Builtin::Table(&MZ_PROMETHEUS_HISTOGRAMS),
            Builtin::Table(&MZ_PROMETHEUS_METRICS),
            Builtin::Table(&MZ_CLUSTERS),
            Builtin::View(&MZ_CATALOG_NAMES),
            Builtin::View(&MZ_ARRANGEMENT_SHARING),
            Builtin::View(&MZ_ARRANGEMENT_SIZES),
            Builtin::View(&MZ_DATAFLOW_NAMES),
            Builtin::View(&MZ_DATAFLOW_OPERATOR_DATAFLOWS),
            Builtin::View(&MZ_DATAFLOW_OPERATOR_REACHABILITY),
            Builtin::View(&MZ_MATERIALIZATION_FRONTIERS),
            Builtin::View(&MZ_MESSAGE_COUNTS),
            Builtin::View(&MZ_OBJECTS),
            Builtin::View(&MZ_PERF_ARRANGEMENT_RECORDS),
            Builtin::View(&MZ_PERF_DEPENDENCY_FRONTIERS),
            Builtin::View(&MZ_PERF_PEEK_DURATIONS_AGGREGATES),
            Builtin::View(&MZ_PERF_PEEK_DURATIONS_BUCKET),
            Builtin::View(&MZ_PERF_PEEK_DURATIONS_CORE),
            Builtin::View(&MZ_RECORDS_PER_DATAFLOW),
            Builtin::View(&MZ_RECORDS_PER_DATAFLOW_GLOBAL),
            Builtin::View(&MZ_RECORDS_PER_DATAFLOW_OPERATOR),
            Builtin::View(&MZ_RELATIONS),
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
            Builtin::View(&PG_ROLES),
            Builtin::View(&PG_VIEWS),
            Builtin::View(&PG_COLLATION),
            Builtin::View(&PG_POLICY),
            Builtin::View(&PG_INHERITS),
            Builtin::View(&INFORMATION_SCHEMA_COLUMNS),
            Builtin::View(&INFORMATION_SCHEMA_TABLES),
        ];

        // TODO(sploiselle): assign static global IDs to functions
        let mut func_global_id_counter = 2000;

        for (schema, funcs) in &[
            (PG_CATALOG_SCHEMA, &*mz_sql::func::PG_CATALOG_BUILTINS),
            (INFORMATION_SCHEMA, &*mz_sql::func::INFORMATION_SCHEMA_BUILTINS),
            (MZ_CATALOG_SCHEMA, &*mz_sql::func::MZ_CATALOG_BUILTINS),
            (MZ_INTERNAL_SCHEMA, &*mz_sql::func::MZ_INTERNAL_BUILTINS),
        ] {
            for (name, func) in funcs.iter() {
                builtins.push(Builtin::Func(BuiltinFunc {
                    name,
                    schema,
                    id: GlobalId::System(func_global_id_counter),
                    inner: func,
                }));
                func_global_id_counter += 1;
            }
        }

        assert!(func_global_id_counter < 3000, "exhausted func global IDs");

        // Ensure the IDs we assign are all unique:
        let mut encountered = BTreeSet::<GlobalId>::new();
        let mut encounter =
            move |kind_name: &str, field_name: &str, identifier: &str, id: &GlobalId| {
                assert!(
                    encountered.insert(id.to_owned()),
                    "{} for {} {:?} is already used as a global ID: {:?}",
                    field_name,
                    kind_name,
                    identifier,
                    id,
                );
                assert!(
                    *id < GlobalId::System(FIRST_SYSTEM_INDEX_ID),
                    "{field_name} for {kind_name} {identifier:?} is not less \
                     than FIRST_SYSTEM_INDEX_ID ({FIRST_SYSTEM_INDEX_ID}): {id}"
                )
            };
        use Builtin::*;
        for b in builtins.iter() {
            match b {
                Type(BuiltinType{id, ..}) => {
                    let name = format!("with ID {:?}", id);
                    encounter("type", "id", &name, id);
                }
                Log(BuiltinLog { id, name, .. }) => {
                    encounter("type", "id", name, id);
                }
                Table(BuiltinTable { id, name, .. }) => {
                    encounter("builtin table", "id", name, id);
                }
                View(BuiltinView { id, name, .. }) => {
                    encounter("view", "id", name, id);
                }
                Func(BuiltinFunc { id, name, .. }) => {
                    encounter("function", "id", name, id);
                }
            }
        }

        builtins.into_iter().map(|b| (b.id(), b)).collect()
    };

    pub static ref BUILTIN_ROLES: Vec<BuiltinRole> = vec![MZ_SYSTEM];
}

impl BUILTINS {
    pub fn logs(&self) -> impl Iterator<Item = &'static BuiltinLog> + '_ {
        self.values().filter_map(|b| match b {
            Builtin::Log(log) => Some(*log),
            _ => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::env;

    use tokio_postgres::NoTls;

    use mz_ore::task;
    use mz_pgrepr::oid::{FIRST_MATERIALIZE_OID, FIRST_UNPINNED_OID};

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

        let mut proc_oids = HashSet::new();
        let mut type_oids = HashSet::new();

        for builtin in BUILTINS.values() {
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
                    match ty.details.typ {
                        CatalogType::Array { element_id } => {
                            let elem_ty = match BUILTINS.get(&element_id) {
                                Some(Builtin::Type(ty)) => ty,
                                _ => panic!("{} is unexpectedly not a type", element_id),
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
                    match ty.details.array_id {
                        Some(array_id) => {
                            let array_ty = match BUILTINS.get(&array_id) {
                                Some(Builtin::Type(ty)) => ty,
                                _ => panic!("{} is unexpectedly not a type", array_id),
                            };
                            assert_eq!(
                                pg_ty.array, array_ty.oid,
                                "type {} has mismatched array OIDs",
                                ty.name,
                            );
                        }
                        None => assert_eq!(
                            pg_ty.array, 0,
                            "type {} does not have an array type in mz but does in pg",
                            ty.name,
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
                        if imp.arg_oids != pg_fn.arg_oids {
                            println!(
                                "funcs with oid {} ({}) don't match arguments: {:?} in mz, {:?} in pg",
                                imp.oid, func.name, imp.arg_oids, pg_fn.arg_oids
                            );
                        }

                        if imp.return_oid != pg_fn.ret_oid {
                            println!(
                                "funcs with oid {} ({}) don't match return types: {:?} in mz, {:?} in pg",
                                imp.oid, func.name, imp.return_oid, pg_fn.ret_oid
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
