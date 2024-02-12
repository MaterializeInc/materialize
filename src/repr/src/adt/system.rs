// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! System data types.

use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

/// A Rust type representing a PostgreSQL "char".
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PgLegacyChar(pub u8);

/// A Rust type representing a PostgreSQL object identifier (OID).
#[derive(
    Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Arbitrary,
)]
pub struct Oid(pub u32);

/// A Rust type representing the OID of a PostgreSQL class.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RegClass(pub u32);

/// A Rust type representing the OID of a PostgreSQL function name.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RegProc(pub u32);

/// A Rust type representing the OID of a PostgreSQL type.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RegType(pub u32);
