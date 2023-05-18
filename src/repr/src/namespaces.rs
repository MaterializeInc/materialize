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
pub const PG_CATALOG_SCHEMA: &str = "pg_catalog";
pub const MZ_INTERNAL_SCHEMA: &str = "mz_internal";
pub const INFORMATION_SCHEMA: &str = "information_schema";
