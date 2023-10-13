// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// The number of bytes required **by PostgreSQL** to store a value of type
/// [`ScalarType::PgLegacyName`](crate::ScalarType::PgLegacyName).
///
/// Corresponds to the `NAMEDATALEN` constant in the PostgreSQL source code.
///
/// Note that the length contains an extra byte for the null terminator. This
/// does not directly correspond to Materialize, as Rust `String`s do not use
/// a null terminator. You should generally use `NAME_MAX_BYTES` instead, which
/// describes the maximum number of usable bytes in a `name` value.
const PG_NAMEDATALEN: usize = 64;

/// The maximum number of bytes that may be stored in a value of type
/// [`ScalarType::PgLegacyName`](crate::ScalarType::PgLegacyName).
pub const NAME_MAX_BYTES: usize = PG_NAMEDATALEN - 1;

/// A Rust type representing a PostgreSQL `name` type.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PgLegacyName<S>(pub S)
where
    S: AsRef<str>;
