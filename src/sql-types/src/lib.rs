// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Leaf data types shared between [`mz_sql`](../mz_sql/index.html) and lower-level
//! crates such as `mz-catalog-protos`.
//!
//! These types were extracted from `mz-sql` so that crates needing only the plain
//! catalog/name/plan data types (not the planner) can depend on this small crate
//! instead of the full `mz-sql`, which sits high in the dependency graph. `mz-sql`
//! re-exports everything here at its original module paths.

pub mod catalog;
pub mod names;
pub mod plan;
pub mod rbac;
pub mod session;

mod parse_error;
pub use parse_error::ParseError;
