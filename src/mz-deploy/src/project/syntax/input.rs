// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Source-owned data types for a single parsed database object.
//!
//! [`DatabaseObject`] and [`ObjectVariant`] carry the parsed statements and
//! source locations for one `.sql` file (or a default + profile-override pair)
//! before semantic validation. Loading and assembly into per-schema groupings
//! live in [`crate::project::compiler`].

use crate::project::syntax::parser::LocatedStatement;
use std::path::PathBuf;

/// A single file variant of a database object (default or profile-specific).
#[derive(Debug, Clone)]
pub struct ObjectVariant {
    /// The full path to the file
    pub path: PathBuf,
    /// The profile name, or `None` for the default variant
    pub profile: Option<String>,
    /// The parsed SQL statements from the file, each with its byte offset.
    pub statements: Vec<LocatedStatement>,
}

/// A database object that may have multiple profile variants.
///
/// Represents one logical object name in a schema directory. The object may have
/// a default file and/or one or more profile-specific override files. All variants
/// are loaded and parsed; cross-variant validation and active-variant resolution
/// happen during object compilation.
///
/// # Contents
///
/// A typical object file contains:
/// - One primary CREATE statement (table, view, source, etc.)
/// - Zero or more supporting statements (indexes, grants, comments)
///
/// Example `users.sql`:
/// ```sql
/// CREATE TABLE users (
///     id INT,
///     name TEXT
/// );
///
/// CREATE INDEX users_id_idx ON users (id);
/// GRANT SELECT ON users TO analyst_role;
/// COMMENT ON TABLE users IS 'User data';
/// ```
///
/// All statements are parsed into `statements` field without validation of their
/// relationships or correctness.
#[derive(Debug, Clone)]
pub struct DatabaseObject {
    /// The name of the object (without extension or profile suffix)
    pub name: String,
    /// The directory-derived database name (no profile suffix applied).
    /// Per-object validation uses this to match the declared database in the
    /// user's SQL against the directory the file lives under; the suffix is
    /// reapplied to the compiled statement after assembly.
    pub database: String,
    /// The schema name (directory name)
    pub schema: String,
    /// All profile variants for this object (at least one)
    pub variants: Vec<ObjectVariant>,
}
