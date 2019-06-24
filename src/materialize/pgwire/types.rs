// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! PostgreSQL types.
//!
//! Note that constants in this module follow PostgreSQL's naming convention,
//! which does not map exactly to Materialize's type names. See the
//! implementation of [`From<ScalarType>`] for the mapping.
//!
//! The source of truth for PostgreSQL OID mappings is the [pg_type.dat file] in
//! the PostgreSQL repository.
//!
//! [pg_type.dat file]:
//! https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat

use crate::repr::ScalarType;

/// PostgreSQL type metadata.
pub struct PgType {
    pub oid: u32,
    pub typlen: i16,
}

/// Boolean.
pub const BOOL: PgType = PgType { oid: 16, typlen: 1 };

/// Variable-length byte array.
pub const BYTEA: PgType = PgType {
    oid: 17,
    typlen: -1,
};

/// 64-bit integer.
pub const INT8: PgType = PgType { oid: 20, typlen: 8 };

/// 32-bit integer.
pub const INT4: PgType = PgType { oid: 23, typlen: 4 };

/// Variable-length string.
pub const TEXT: PgType = PgType {
    oid: 25,
    typlen: -1,
};

/// 32-bit float.
pub const FLOAT4: PgType = PgType {
    oid: 700,
    typlen: 8,
};

/// 64-bit float.
pub const FLOAT8: PgType = PgType {
    oid: 701,
    typlen: 8,
};

// /// A pseudo-type representing a composite record (i.e., a tuple) of any type.
// pub const RECORD: PgType = PgType {
//     oid: 2249,
//     typlen: -1,
// };

// /// A pseudo-type representing any type.
// pub const ANY: PgType = PgType {
//     oid: 2276,
//     typlen: 4,
// };

impl From<&ScalarType> for PgType {
    fn from(typ: &ScalarType) -> PgType {
        match typ {
            ScalarType::Null => BOOL,
            ScalarType::Bool => BOOL,
            ScalarType::Int32 => INT4,
            ScalarType::Int64 => INT8,
            ScalarType::Float32 => FLOAT4,
            ScalarType::Float64 => FLOAT8,
            ScalarType::Decimal(_, _) => unimplemented!(),
            ScalarType::Date => unimplemented!(),
            ScalarType::Timestamp => unimplemented!(),
            ScalarType::Time => unimplemented!(),
            ScalarType::Bytes => BYTEA,
            ScalarType::String => TEXT,
            ScalarType::Regex => panic!("ScalarType::Regex is not representable over pgwire"),
        }
    }
}
