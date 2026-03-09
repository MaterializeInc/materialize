//! Profile-specific file override resolution.
//!
//! Files can be named `name__<profile>.sql` to override `name.sql` when a
//! particular profile is active. The `__` delimiter is split on the **last**
//! occurrence (`rsplit_once`) so object names may themselves contain underscores.

use crate::project::error::LoadError;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Split a file stem into `(object_name, optional_profile)`.
///
/// Uses `rsplit_once("__")` so that `pg_conn__staging` → `("pg_conn", Some("staging"))`.
/// Returns `(stem, None)` if no valid split exists (empty parts).
pub fn parse_file_stem(stem: &str) -> (&str, Option<&str>) {
    if let Some((object_name, profile)) = stem.rsplit_once("__") {
        if !object_name.is_empty() && !profile.is_empty() {
            return (object_name, Some(profile));
        }
    }
    (stem, None)
}

/// Intermediate grouping for profile resolution.
#[derive(Default)]
struct FileGroup {
    default: Option<PathBuf>,
    profile_match: Option<PathBuf>,
}

/// Resolve profile-specific file overrides from a set of `(path, file_stem)` pairs.
///
/// For each object name:
/// - If a `name__<profile>` variant exists, use it instead of the default.
/// - If only a `name` default exists (no matching profile override), use the default.
/// - Files targeting other profiles are skipped.
///
/// Returns `(path, resolved_object_name)` pairs sorted by object name.
pub fn resolve_profile_files(
    entries: Vec<(PathBuf, String)>,
    profile: &str,
) -> Result<Vec<(PathBuf, String)>, LoadError> {
    let mut groups: BTreeMap<String, FileGroup> = BTreeMap::new();

    for (path, stem) in entries {
        let (object_name, file_profile) = parse_file_stem(&stem);
        let group = groups.entry(object_name.to_string()).or_default();

        match file_profile {
            None => {
                if let Some(existing) = &group.default {
                    return Err(LoadError::DuplicateProfileObject {
                        name: object_name.to_string(),
                        profile: profile.to_string(),
                        path1: existing.clone(),
                        path2: path,
                    });
                }
                group.default = Some(path);
            }
            Some(p) if p == profile => {
                if let Some(existing) = &group.profile_match {
                    return Err(LoadError::DuplicateProfileObject {
                        name: object_name.to_string(),
                        profile: profile.to_string(),
                        path1: existing.clone(),
                        path2: path,
                    });
                }
                group.profile_match = Some(path);
            }
            Some(_) => {
                // the object doesn't apply to this profile.
            }
        }
    }

    let mut result = Vec::new();
    for (object_name, group) in groups {
        if let Some(path) = group.profile_match {
            result.push((path, object_name));
        } else if let Some(path) = group.default {
            result.push((path, object_name));
        }
    }

    Ok(result)
}

/// Collect `.sql` files from a directory and resolve profile-specific overrides.
///
/// Reads the directory, filters to `.sql` files, extracts file stems,
/// then applies profile override resolution. Returns `(path, object_name)` pairs.
pub fn collect_and_resolve_sql_files(
    directory: &Path,
    profile: &str,
) -> Result<Vec<(PathBuf, String)>, LoadError> {
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

    let mut sql_entries = Vec::new();
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

        sql_entries.push((path, file_stem));
    }

    resolve_profile_files(sql_entries, profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_file_stem tests ---

    #[test]
    fn test_parse_no_delimiter() {
        assert_eq!(parse_file_stem("pg_conn"), ("pg_conn", None));
    }

    #[test]
    fn test_parse_with_profile() {
        assert_eq!(
            parse_file_stem("pg_conn__staging"),
            ("pg_conn", Some("staging"))
        );
    }

    #[test]
    fn test_parse_multiple_underscores() {
        assert_eq!(
            parse_file_stem("my_pg__conn__prod"),
            ("my_pg__conn", Some("prod"))
        );
    }

    #[test]
    fn test_parse_empty_profile() {
        // "pg_conn__" → empty profile part, treated as plain name
        assert_eq!(parse_file_stem("pg_conn__"), ("pg_conn__", None));
    }

    #[test]
    fn test_parse_empty_object_name() {
        // "__staging" → empty object name, treated as plain name
        assert_eq!(parse_file_stem("__staging"), ("__staging", None));
    }

    #[test]
    fn test_parse_no_underscores() {
        assert_eq!(parse_file_stem("simple"), ("simple", None));
    }

    #[test]
    fn test_parse_single_underscore() {
        assert_eq!(parse_file_stem("my_table"), ("my_table", None));
    }

    // --- resolve_profile_files tests ---

    #[test]
    fn test_resolve_default_only() {
        let entries = vec![(PathBuf::from("a/pg_conn.sql"), "pg_conn".to_string())];
        let result = resolve_profile_files(entries, "staging").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, "pg_conn");
        assert_eq!(result[0].0, PathBuf::from("a/pg_conn.sql"));
    }

    #[test]
    fn test_resolve_profile_override() {
        let entries = vec![
            (PathBuf::from("a/pg_conn.sql"), "pg_conn".to_string()),
            (
                PathBuf::from("a/pg_conn__staging.sql"),
                "pg_conn__staging".to_string(),
            ),
        ];
        let result = resolve_profile_files(entries, "staging").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, "pg_conn");
        assert_eq!(result[0].0, PathBuf::from("a/pg_conn__staging.sql"));
    }

    #[test]
    fn test_resolve_skips_other_profiles() {
        let entries = vec![
            (PathBuf::from("a/pg_conn.sql"), "pg_conn".to_string()),
            (
                PathBuf::from("a/pg_conn__prod.sql"),
                "pg_conn__prod".to_string(),
            ),
        ];
        let result = resolve_profile_files(entries, "staging").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, "pg_conn");
        assert_eq!(result[0].0, PathBuf::from("a/pg_conn.sql"));
    }

    #[test]
    fn test_resolve_profile_only_file() {
        // Object only exists for staging profile, no default
        let entries = vec![(
            PathBuf::from("a/secret__staging.sql"),
            "secret__staging".to_string(),
        )];
        let result = resolve_profile_files(entries, "staging").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, "secret");
        assert_eq!(result[0].0, PathBuf::from("a/secret__staging.sql"));
    }

    #[test]
    fn test_resolve_profile_only_file_wrong_profile() {
        // Object only exists for prod profile, should be skipped when staging
        let entries = vec![(
            PathBuf::from("a/secret__prod.sql"),
            "secret__prod".to_string(),
        )];
        let result = resolve_profile_files(entries, "staging").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_resolve_sorted_by_object_name() {
        let entries = vec![
            (PathBuf::from("a/zebra.sql"), "zebra".to_string()),
            (PathBuf::from("a/alpha.sql"), "alpha".to_string()),
        ];
        let result = resolve_profile_files(entries, "default").unwrap();
        assert_eq!(result[0].1, "alpha");
        assert_eq!(result[1].1, "zebra");
    }

    #[test]
    fn test_resolve_multiple_objects_mixed() {
        let entries = vec![
            (PathBuf::from("a/conn.sql"), "conn".to_string()),
            (
                PathBuf::from("a/conn__staging.sql"),
                "conn__staging".to_string(),
            ),
            (PathBuf::from("a/table.sql"), "table".to_string()),
            (
                PathBuf::from("a/secret__staging.sql"),
                "secret__staging".to_string(),
            ),
            (
                PathBuf::from("a/other__prod.sql"),
                "other__prod".to_string(),
            ),
        ];
        let result = resolve_profile_files(entries, "staging").unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].1, "conn");
        assert_eq!(result[0].0, PathBuf::from("a/conn__staging.sql"));
        assert_eq!(result[1].1, "secret");
        assert_eq!(result[1].0, PathBuf::from("a/secret__staging.sql"));
        assert_eq!(result[2].1, "table");
        assert_eq!(result[2].0, PathBuf::from("a/table.sql"));
    }

    #[test]
    fn test_resolve_duplicate_default_errors() {
        let entries = vec![
            (PathBuf::from("a/conn.sql"), "conn".to_string()),
            (PathBuf::from("b/conn.sql"), "conn".to_string()),
        ];
        let result = resolve_profile_files(entries, "default");
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_duplicate_profile_match_errors() {
        let entries = vec![
            (
                PathBuf::from("a/conn__staging.sql"),
                "conn__staging".to_string(),
            ),
            (
                PathBuf::from("b/conn__staging.sql"),
                "conn__staging".to_string(),
            ),
        ];
        let result = resolve_profile_files(entries, "staging");
        assert!(result.is_err());
    }
}
