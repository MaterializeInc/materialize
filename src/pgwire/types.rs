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

use repr::ScalarType;

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

/// Date. Represented as the 32-bit number of days since January 1, 2000.
pub const DATE: PgType = PgType {
    oid: 1082,
    typlen: 4,
};

/// Timestamp. Represented as the 64-bit number of microseconds since January 1,
/// 2000 00:00:00.000.
pub const TIMESTAMP: PgType = PgType {
    oid: 1114,
    typlen: 8,
};

/// Timestamp with time zone
pub const TIMESTAMPTZ: PgType = PgType {
    oid: 1184,
    typlen: 8,
};

/// Time interval.
pub const INTERVAL: PgType = PgType {
    oid: 1186,
    typlen: 16,
};

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

/// Arbitrary-precision decimal.
pub const NUMERIC: PgType = PgType {
    oid: 1700,
    typlen: -1,
};

/// Json in binary serialization
pub const JSONB: PgType = PgType {
    oid: 3802,
    typlen: -1,
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
            ScalarType::Decimal { .. } => NUMERIC,
            ScalarType::Date => DATE,
            ScalarType::Timestamp => TIMESTAMP,
            ScalarType::TimestampTz => TIMESTAMPTZ,
            ScalarType::Interval => INTERVAL,
            ScalarType::Bytes => BYTEA,
            ScalarType::String => TEXT,
            ScalarType::Jsonb => JSONB,
        }
    }
}
