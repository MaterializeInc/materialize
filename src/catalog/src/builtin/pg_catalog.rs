// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Built-in catalog items for the `pg_catalog` schema.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use mz_pgrepr::oid;
use mz_repr::namespaces::PG_CATALOG_SCHEMA;
use mz_repr::{RelationDesc, SqlScalarType};
use mz_sql::catalog::{
    CatalogType, CatalogTypeDetails, CatalogTypePgMetadata, NameReference, ObjectType,
};
use mz_sql::rbac;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;

use super::{BuiltinType, BuiltinView, PUBLIC_SELECT};

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
    ontology: None,
});

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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    count(mz_index_columns.index_position)::pg_catalog.int2 AS indnatts,
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
        ontology: None,
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
    ontology: None,
});

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
    ontology: None,
});

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
    ontology: None,
});

/// <https://www.postgresql.org/docs/current/catalog-pg-attribute.html>
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
            .with_column("attndims", SqlScalarType::Int16.nullable(false))
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
    attrelid, attname, atttypid, attlen, attnum, atttypmod, attndims, attnotnull, atthasdef,
    attidentity, attgenerated, attisdropped, attcollation
FROM mz_internal.pg_attribute_all_databases
WHERE
  (database_name IS NULL OR database_name = pg_catalog.current_database()) AND
  (pg_type_database_name IS NULL OR pg_type_database_name = pg_catalog.current_database());",
        // Since this depends on pg_type, its id must be higher due to initialization
        // ordering.
        access: vec![PUBLIC_SELECT],
        ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
});

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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
        ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
        .with_column("passwd", SqlScalarType::String.nullable(false))
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
    '********' as passwd,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    // The `has_system_privilege` invocations for `rolcreaterole` and `rolcreatedb` get expanded
    // into very complex subqueries. If we put them into the SELECT clause directly, decorrelation
    // produces a very complex plan that the optimizer has a hard time dealing with. In particular,
    // the optimizer fails to reduce a query like `SELECT oid FROM pg_authid` to a simple lookup on
    // the `pg_authid_core` index and instead produces a large plan that contains a bunch of
    // expensive joins and arrangements.
    //
    // The proper fix is likely to implement `has_system_privileges` in Rust, but for now we work
    // around the issue by manually decorrelating `rolcreaterole` and `rolcreatedb`. Note that to
    // get the desired behavior we need to make sure that the join with `extra` doesn't change the
    // cardinality of `pg_authid_core` (otherwise it can never be optimized away). We ensure this
    // by:
    //  * using a `LEFT JOIN`, so the optimizer knows that left elements are never filtered
    //  * applying a `DISTINCT ON` to the CTE, so the optimizer knows that left elements are never
    //    duplicated
    sql: r#"
WITH extra AS (
    SELECT
        DISTINCT ON (oid)
        oid,
        mz_catalog.has_system_privilege(oid, 'CREATEROLE') AS rolcreaterole,
        mz_catalog.has_system_privilege(oid, 'CREATEDB') AS rolcreatedb
    FROM mz_internal.pg_authid_core
)
SELECT
    oid,
    rolname,
    rolsuper,
    rolinherit,
    extra.rolcreaterole,
    extra.rolcreatedb,
    rolcanlogin,
    rolreplication,
    rolbypassrls,
    rolconnlimit,
    rolpassword,
    rolvaliduntil
FROM mz_internal.pg_authid_core
LEFT JOIN extra USING (oid)"#,
    access: vec![rbac::owner_privilege(ObjectType::Table, MZ_SYSTEM_ROLE_ID)],
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
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
    ontology: None,
});
