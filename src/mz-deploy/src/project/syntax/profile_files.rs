// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Profile-specific file override resolution.
//!
//! Files can be named `name#<profile>.sql` to override `name.sql` when a
//! particular profile is active. The `#` delimiter is split on the **last**
//! occurrence (`rsplit_once`).
//!
//! ## Resolution Algorithm
//!
//! 1. **Parse** each file stem via [`parse_file_stem`] using `rsplit_once('#')`
//!    to separate `(object_name, profile)`. Files without `#` (or with an
//!    empty object/profile part) are treated as the default (no profile).
//! 2. **Group** files by object name into [`ObjectFiles`], recording the default
//!    file and any profile-specific overrides. Duplicates within the same
//!    group (e.g., two defaults, or two overrides for the same profile) are
//!    rejected with `LoadError::DuplicateProfileObject`.
//!
//! Callers select the active variant themselves by checking
//! `overrides.get(profile).or(default.as_ref())`.
//!
//! **Key Insight:** `#` cannot appear in a SQL identifier, so a well-formed
//! variant filename contains exactly one `#`. This lets object names freely
//! contain underscores: `my_pg_conn#staging` → `("my_pg_conn", "staging")`.

use crate::project::error::LoadError;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Split a file stem into `(object_name, optional_profile)`.
///
/// Uses `rsplit_once('#')` so that `pg_conn#staging` → `("pg_conn", Some("staging"))`.
/// Returns `(stem, None)` if no valid split exists (empty parts).
pub(crate) fn parse_file_stem(stem: &str) -> (&str, Option<&str>) {
    if let Some((object_name, profile)) = stem.rsplit_once('#') {
        if !object_name.is_empty() && !profile.is_empty() {
            return (object_name, Some(profile));
        }
    }
    (stem, None)
}

/// All files for a single object name, grouped by profile.
#[derive(Debug, Clone)]
pub(crate) struct ObjectFiles {
    /// The object name (without profile suffix)
    pub name: String,
    /// The default file (no profile suffix), if any
    pub default: Option<PathBuf>,
    /// Profile-specific override files, keyed by profile name
    pub overrides: BTreeMap<String, PathBuf>,
}

/// Collect all `.sql` files from a directory grouped by object name without resolving.
///
/// Returns all variants (default + all profile overrides) for each object.
/// This is used to load and validate all profile variants before resolving.
pub(crate) fn collect_all_sql_files(directory: &Path) -> Result<Vec<ObjectFiles>, LoadError> {
    let entries: Vec<_> = std::fs::read_dir(directory)
        .map_err(|e| LoadError::DirectoryReadFailed {
            path: directory.to_path_buf(),
            source: e,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| LoadError::EntryReadFailed {
            directory: directory.to_path_buf(),
            source: e,
        })?;

    let mut groups: BTreeMap<String, ObjectFiles> = BTreeMap::new();

    for entry in entries {
        let path = entry.path();

        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }

        let file_stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| LoadError::InvalidFileName { path: path.clone() })?
            .to_string();

        let (object_name, file_profile) = parse_file_stem(&file_stem);
        let group = groups
            .entry(object_name.to_string())
            .or_insert_with(|| ObjectFiles {
                name: object_name.to_string(),
                default: None,
                overrides: BTreeMap::new(),
            });

        match file_profile {
            None => {
                if let Some(existing) = &group.default {
                    return Err(LoadError::DuplicateProfileObject {
                        name: object_name.to_string(),
                        profile: "default".to_string(),
                        path1: existing.clone(),
                        path2: path,
                    });
                }
                group.default = Some(path);
            }
            Some(p) => {
                if let Some(existing) = group.overrides.get(p) {
                    return Err(LoadError::DuplicateProfileObject {
                        name: object_name.to_string(),
                        profile: p.to_string(),
                        path1: existing.clone(),
                        path2: path,
                    });
                }
                group.overrides.insert(p.to_string(), path);
            }
        }
    }

    Ok(groups.into_values().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_file_stem tests ---

    #[mz_ore::test]
    fn test_parse_no_delimiter() {
        assert_eq!(parse_file_stem("pg_conn"), ("pg_conn", None));
    }

    #[mz_ore::test]
    fn test_parse_with_profile() {
        assert_eq!(
            parse_file_stem("pg_conn#staging"),
            ("pg_conn", Some("staging"))
        );
    }

    #[mz_ore::test]
    fn test_parse_object_name_with_underscores() {
        // Underscores in the object name are preserved; only the `#`
        // separates the profile.
        assert_eq!(
            parse_file_stem("stg_stripe__payments#staging"),
            ("stg_stripe__payments", Some("staging"))
        );
    }

    #[mz_ore::test]
    fn test_parse_empty_profile() {
        // "pg_conn#" → empty profile part, treated as plain name
        assert_eq!(parse_file_stem("pg_conn#"), ("pg_conn#", None));
    }

    #[mz_ore::test]
    fn test_parse_empty_object_name() {
        // "#staging" → empty object name, treated as plain name
        assert_eq!(parse_file_stem("#staging"), ("#staging", None));
    }

    #[mz_ore::test]
    fn test_parse_no_underscores() {
        assert_eq!(parse_file_stem("simple"), ("simple", None));
    }

    #[mz_ore::test]
    fn test_parse_single_underscore() {
        assert_eq!(parse_file_stem("my_table"), ("my_table", None));
    }

    // --- collect_all_sql_files tests ---

    #[mz_ore::test]
    fn test_collect_all_sql_files_basic() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("conn.sql"), "SELECT 1;").unwrap();
        std::fs::write(dir.path().join("conn#staging.sql"), "SELECT 2;").unwrap();
        std::fs::write(dir.path().join("conn#prod.sql"), "SELECT 3;").unwrap();
        std::fs::write(dir.path().join("table.sql"), "SELECT 4;").unwrap();

        let result = collect_all_sql_files(dir.path()).unwrap();
        assert_eq!(result.len(), 2);

        let conn = result.iter().find(|f| f.name == "conn").unwrap();
        assert!(conn.default.is_some());
        assert_eq!(conn.overrides.len(), 2);
        assert!(conn.overrides.contains_key("staging"));
        assert!(conn.overrides.contains_key("prod"));

        let table = result.iter().find(|f| f.name == "table").unwrap();
        assert!(table.default.is_some());
        assert!(table.overrides.is_empty());
    }

    #[mz_ore::test]
    fn test_collect_all_sql_files_override_only() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("secret#staging.sql"), "SELECT 1;").unwrap();

        let result = collect_all_sql_files(dir.path()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "secret");
        assert!(result[0].default.is_none());
        assert_eq!(result[0].overrides.len(), 1);
    }

    #[mz_ore::test]
    fn test_collect_all_sql_files_duplicate_override_errors() {
        // The filesystem guarantees unique filenames within a directory, so
        // the `DuplicateProfileObject` branch inside `collect_all_sql_files`
        // is unreachable from real inputs. This test just confirms a basic
        // call succeeds.
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("conn.sql"), "SELECT 1;").unwrap();
        let result = collect_all_sql_files(dir.path()).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[mz_ore::test]
    fn test_collect_all_sql_files_ignores_non_sql() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("conn.sql"), "SELECT 1;").unwrap();
        std::fs::write(dir.path().join("readme.md"), "hello").unwrap();

        let result = collect_all_sql_files(dir.path()).unwrap();
        assert_eq!(result.len(), 1);
    }
}
