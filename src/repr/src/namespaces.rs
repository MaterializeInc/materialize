// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Namespace constants to share between high- and low-levels of the system.

// TODO: define default database, schema names

pub const MZ_TEMP_SCHEMA: &str = "mz_temp";
pub const MZ_CATALOG_SCHEMA: &str = "mz_catalog";
pub const MZ_CATALOG_UNSTABLE_SCHEMA: &str = "mz_catalog_unstable";
pub const PG_CATALOG_SCHEMA: &str = "pg_catalog";
pub const MZ_INTERNAL_SCHEMA: &str = "mz_internal";
pub const MZ_INTROSPECTION_SCHEMA: &str = "mz_introspection";
pub const INFORMATION_SCHEMA: &str = "information_schema";
pub const MZ_UNSAFE_SCHEMA: &str = "mz_unsafe";

pub const SYSTEM_SCHEMAS: &[&str] = &[
    MZ_CATALOG_SCHEMA,
    MZ_CATALOG_UNSTABLE_SCHEMA,
    PG_CATALOG_SCHEMA,
    MZ_INTERNAL_SCHEMA,
    MZ_INTROSPECTION_SCHEMA,
    INFORMATION_SCHEMA,
    MZ_UNSAFE_SCHEMA,
];

pub const UNSTABLE_SCHEMAS: &[&str] = &[
    MZ_CATALOG_UNSTABLE_SCHEMA,
    MZ_INTERNAL_SCHEMA,
    MZ_INTROSPECTION_SCHEMA,
    MZ_UNSAFE_SCHEMA,
];

/// Returns whether `name` identifies is a system schema.
pub fn is_system_schema(name: &str) -> bool {
    SYSTEM_SCHEMAS.contains(&name)
}

/// Returns whether `name` identifies is an unstable schema.
pub fn is_unstable_schema(name: &str) -> bool {
    UNSTABLE_SCHEMAS.contains(&name)
}
