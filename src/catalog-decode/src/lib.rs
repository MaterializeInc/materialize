// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Decoders for durable catalog data.
//!
//! The builtin catalog views derive their columns from `mz_catalog_raw`, which
//! exposes catalog objects in their durable encoding. The decoding logic lives
//! here, and `mz-expr` exposes each decoder as an `mz_internal.parse_*` scalar
//! function. It sits below `mz-expr` in the dependency graph, so it cannot use
//! the crates that own the decoded formats (`mz-catalog-protos`, `mz-sql`,
//! `mz-storage-types`, `mz-audit-log`) and mirrors their shapes instead.
//! Tests in those crates and the catalog sqllogictests catch drift.

pub mod audit_log;
pub mod create_sql;
pub mod durable;
