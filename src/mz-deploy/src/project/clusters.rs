//! Cluster definition loading and validation.
//!
//! Loads cluster definitions from `<root>/clusters/` directory. Each `.sql` file
//! defines a single cluster with a required `CREATE CLUSTER` statement and optional
//! `GRANT` and `COMMENT` statements.

use crate::project::error::{
    LoadError, ProjectError, ValidationError, ValidationErrorKind, ValidationErrors,
};
use crate::project::parser::{parse_statements_with_context, statement_type_name};
use crate::project::profile_files::collect_all_sql_files;
use mz_sql_parser::ast::{
    ClusterOptionName, CommentObjectType, CommentStatement, CreateClusterStatement,
    GrantPrivilegesStatement, GrantTargetSpecification, GrantTargetSpecificationInner, Ident,
    ObjectType, Raw, RawClusterName, Statement, UnresolvedObjectName, WithOptionValue,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// A parsed cluster definition from a `.sql` file in the `clusters/` directory.
pub struct ClusterDefinition {
    /// Cluster name (derived from filename and validated against CREATE statement).
    pub name: String,
    /// Path to the source `.sql` file.
    pub path: PathBuf,
    /// The CREATE CLUSTER statement.
    pub create_stmt: CreateClusterStatement<Raw>,
    /// Optional GRANT statements targeting this cluster.
    pub grants: Vec<GrantPrivilegesStatement<Raw>>,
    /// Optional COMMENT statements targeting this cluster.
    pub comments: Vec<CommentStatement<Raw>>,
}

/// Load all cluster definitions from `<root>/clusters/`.
///
/// Returns an empty vec if `clusters/` doesn't exist (the directory is optional).
/// If `profile_suffix` is provided, each cluster definition is rewritten with the
/// suffix appended to all cluster name references (CREATE, GRANT, COMMENT).
pub fn load_clusters(
    root: &Path,
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
) -> Result<Vec<ClusterDefinition>, ProjectError> {
    let clusters_dir = root.join("clusters");

    if !clusters_dir.exists() {
        return Ok(vec![]);
    }

    if !clusters_dir.is_dir() {
        return Err(LoadError::RootNotDirectory { path: clusters_dir }.into());
    }

    let all_files = collect_all_sql_files(&clusters_dir)?;

    let mut definitions = Vec::new();
    let mut errors = Vec::new();

    for object_files in all_files {
        let expected_name = &object_files.name;

        // Validate all variants independently
        let mut all_variant_paths = Vec::new();
        if let Some(ref default_path) = object_files.default {
            all_variant_paths.push((default_path.clone(), None));
        }
        for (prof, override_path) in &object_files.overrides {
            all_variant_paths.push((override_path.clone(), Some(prof.as_str())));
        }

        // Validate each variant independently
        for (path, _) in &all_variant_paths {
            let sql = std::fs::read_to_string(path).map_err(|e| LoadError::FileReadFailed {
                path: path.clone(),
                source: e,
            })?;
            let statements: Vec<Statement<Raw>> =
                parse_statements_with_context(&sql, path.clone(), variables)?
                    .into_iter()
                    .map(|ls| ls.ast)
                    .collect();

            if let Err(mut errs) = classify_cluster_statements(expected_name, path, statements) {
                errors.append(&mut errs);
            }
        }

        // Resolve the active variant: prefer profile match, fall back to default
        let active_path = object_files
            .overrides
            .get(profile)
            .or(object_files.default.as_ref());

        let active_path = match active_path {
            Some(p) => p.clone(),
            None => continue, // no variant for this profile
        };

        let sql = std::fs::read_to_string(&active_path).map_err(|e| LoadError::FileReadFailed {
            path: active_path.clone(),
            source: e,
        })?;
        let statements: Vec<Statement<Raw>> =
            parse_statements_with_context(&sql, active_path.clone(), variables)?
                .into_iter()
                .map(|ls| ls.ast)
                .collect();

        match classify_cluster_statements(expected_name, &active_path, statements) {
            Ok(def) => definitions.push(def),
            Err(mut errs) => errors.append(&mut errs),
        }
    }

    if !errors.is_empty() {
        return Err(ValidationErrors::new(errors).into());
    }

    // Apply cluster suffix after validation (filename-vs-declared-name check uses original names)
    if let Some(suffix) = profile_suffix {
        for def in &mut definitions {
            apply_cluster_suffix(def, suffix);
        }
    }

    Ok(definitions)
}

/// Classify parsed statements into a `ClusterDefinition`, returning validation errors.
fn classify_cluster_statements(
    expected_name: &str,
    path: &Path,
    statements: Vec<Statement<Raw>>,
) -> Result<ClusterDefinition, Vec<ValidationError>> {
    let mut create_stmts: Vec<CreateClusterStatement<Raw>> = Vec::new();
    let mut grants: Vec<GrantPrivilegesStatement<Raw>> = Vec::new();
    let mut comments: Vec<CommentStatement<Raw>> = Vec::new();
    let mut errors = Vec::new();

    for stmt in statements {
        match stmt {
            Statement::CreateCluster(s) => {
                create_stmts.push(s);
            }
            Statement::GrantPrivileges(s) => {
                // Validate that the grant targets a cluster
                match &s.target {
                    GrantTargetSpecification::Object {
                        object_type: ObjectType::Cluster,
                        object_spec_inner: GrantTargetSpecificationInner::Objects { names },
                    } => {
                        // Validate cluster name matches
                        for name in names {
                            let target_name = name.to_string();
                            if target_name.to_lowercase() != expected_name.to_lowercase() {
                                errors.push(ValidationError::with_file_and_sql(
                                    ValidationErrorKind::ClusterGrantTargetMismatch {
                                        target: target_name,
                                        cluster_name: expected_name.to_string(),
                                    },
                                    path.to_path_buf(),
                                    s.to_string(),
                                ));
                            }
                        }
                        grants.push(s);
                    }
                    _ => {
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::InvalidClusterStatement {
                                statement_type: "GRANT (not targeting a cluster)".to_string(),
                                cluster_name: expected_name.to_string(),
                            },
                            path.to_path_buf(),
                            s.to_string(),
                        ));
                    }
                }
            }
            Statement::Comment(s) => {
                // Validate that the comment targets a cluster
                match &s.object {
                    CommentObjectType::Cluster { name } => {
                        let target_name = match name {
                            RawClusterName::Unresolved(ident) => ident.to_string(),
                            RawClusterName::Resolved(id) => id.clone(),
                        };
                        if target_name.to_lowercase() != expected_name.to_lowercase() {
                            errors.push(ValidationError::with_file_and_sql(
                                ValidationErrorKind::ClusterCommentTargetMismatch {
                                    target: target_name,
                                    cluster_name: expected_name.to_string(),
                                },
                                path.to_path_buf(),
                                s.to_string(),
                            ));
                        }
                        comments.push(s);
                    }
                    _ => {
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::InvalidClusterStatement {
                                statement_type: "COMMENT (not targeting a cluster)".to_string(),
                                cluster_name: expected_name.to_string(),
                            },
                            path.to_path_buf(),
                            s.to_string(),
                        ));
                    }
                }
            }
            other => {
                errors.push(ValidationError::with_file_and_sql(
                    ValidationErrorKind::InvalidClusterStatement {
                        statement_type: statement_type_name(&other).to_string(),
                        cluster_name: expected_name.to_string(),
                    },
                    path.to_path_buf(),
                    other.to_string(),
                ));
            }
        }
    }

    // Validate exactly one CREATE CLUSTER
    if create_stmts.is_empty() {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::ClusterMissingCreateStatement {
                cluster_name: expected_name.to_string(),
            },
            path.to_path_buf(),
        ));
    } else if create_stmts.len() > 1 {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::ClusterMultipleCreateStatements {
                cluster_name: expected_name.to_string(),
            },
            path.to_path_buf(),
        ));
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    let create_stmt = create_stmts.into_iter().next().unwrap();

    // Validate cluster name matches filename
    let declared_name = create_stmt.name.to_string();
    if declared_name.to_lowercase() != expected_name.to_lowercase() {
        return Err(vec![ValidationError::with_file(
            ValidationErrorKind::ClusterNameMismatch {
                declared: declared_name,
                expected: expected_name.to_string(),
            },
            path.to_path_buf(),
        )]);
    }

    Ok(ClusterDefinition {
        name: expected_name.to_string(),
        path: path.to_path_buf(),
        create_stmt,
        grants,
        comments,
    })
}

/// Apply a suffix to all cluster name references within a `ClusterDefinition`.
///
/// Rewrites the definition name, the CREATE statement name, GRANT target names,
/// and COMMENT target names.
fn apply_cluster_suffix(def: &mut ClusterDefinition, suffix: &str) {
    // Rewrite definition name first, then reference it for the CREATE statement
    def.name = format!("{}{}", def.name, suffix);
    def.create_stmt.name = Ident::new(&def.name).expect("valid cluster identifier");

    // Rewrite GRANT target cluster names
    for grant in &mut def.grants {
        if let GrantTargetSpecification::Object {
            object_type: ObjectType::Cluster,
            object_spec_inner: GrantTargetSpecificationInner::Objects { names },
        } = &mut grant.target
        {
            for name in names {
                if let UnresolvedObjectName::Cluster(ident) = name {
                    *ident = suffixed_ident(ident, suffix);
                }
            }
        }
    }

    // Rewrite COMMENT target cluster names
    for comment in &mut def.comments {
        if let CommentObjectType::Cluster { name } = &mut comment.object {
            if let RawClusterName::Unresolved(ident) = name {
                *ident = suffixed_ident(ident, suffix);
            }
        }
    }
}

/// Append a suffix to an `Ident`, returning a new `Ident`.
fn suffixed_ident(ident: &Ident, suffix: &str) -> Ident {
    Ident::new(&format!("{}{}", ident, suffix)).expect("valid cluster identifier")
}

/// Extract the desired SIZE from a CreateClusterStatement's options.
pub fn extract_size(create_stmt: &CreateClusterStatement<Raw>) -> Option<String> {
    for opt in &create_stmt.options {
        if opt.name == ClusterOptionName::Size {
            if let Some(WithOptionValue::Value(ref v)) = opt.value {
                return Some(v.to_string().trim_matches('\'').to_string());
            }
        }
    }
    None
}

/// Extract the desired REPLICATION FACTOR from a CreateClusterStatement's options.
pub fn extract_replication_factor(create_stmt: &CreateClusterStatement<Raw>) -> Option<u32> {
    for opt in &create_stmt.options {
        if opt.name == ClusterOptionName::ReplicationFactor {
            if let Some(WithOptionValue::Value(ref v)) = opt.value {
                return v.to_string().parse::<u32>().ok();
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    #[test]
    fn test_load_clusters_no_directory() {
        let dir = create_test_dir();
        let result = load_clusters(dir.path(), "default", None, &BTreeMap::new()).unwrap();
        assert!(
            result.is_empty(),
            "should return empty vec when clusters/ doesn't exist"
        );
    }

    #[test]
    fn test_load_clusters_basic() {
        let dir = create_test_dir();
        let clusters_dir = dir.path().join("clusters");
        fs::create_dir(&clusters_dir).unwrap();

        fs::write(
            clusters_dir.join("analytics.sql"),
            "CREATE CLUSTER analytics (SIZE = '100cc', REPLICATION FACTOR = 1);\n\
             GRANT USAGE ON CLUSTER analytics TO analyst_role;\n\
             COMMENT ON CLUSTER analytics IS 'Analytics workloads';",
        )
        .unwrap();

        let result = load_clusters(dir.path(), "default", None, &BTreeMap::new()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "analytics");
        assert_eq!(result[0].grants.len(), 1);
        assert_eq!(result[0].comments.len(), 1);

        // Verify extracted options
        assert_eq!(
            extract_size(&result[0].create_stmt),
            Some("100cc".to_string())
        );
        assert_eq!(extract_replication_factor(&result[0].create_stmt), Some(1));
    }

    #[test]
    fn test_load_clusters_create_only() {
        let dir = create_test_dir();
        let clusters_dir = dir.path().join("clusters");
        fs::create_dir(&clusters_dir).unwrap();

        fs::write(
            clusters_dir.join("quickstart.sql"),
            "CREATE CLUSTER quickstart (SIZE = '25cc');",
        )
        .unwrap();

        let result = load_clusters(dir.path(), "default", None, &BTreeMap::new()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "quickstart");
        assert!(result[0].grants.is_empty());
        assert!(result[0].comments.is_empty());
    }

    #[test]
    fn test_load_clusters_name_mismatch() {
        let dir = create_test_dir();
        let clusters_dir = dir.path().join("clusters");
        fs::create_dir(&clusters_dir).unwrap();

        fs::write(
            clusters_dir.join("analytics.sql"),
            "CREATE CLUSTER wrong_name (SIZE = '100cc');",
        )
        .unwrap();

        let result = load_clusters(dir.path(), "default", None, &BTreeMap::new());
        assert!(
            result.is_err(),
            "should error when cluster name doesn't match filename"
        );
    }

    #[test]
    fn test_load_clusters_missing_create() {
        let dir = create_test_dir();
        let clusters_dir = dir.path().join("clusters");
        fs::create_dir(&clusters_dir).unwrap();

        fs::write(
            clusters_dir.join("analytics.sql"),
            "GRANT USAGE ON CLUSTER analytics TO analyst_role;",
        )
        .unwrap();

        let result = load_clusters(dir.path(), "default", None, &BTreeMap::new());
        assert!(
            result.is_err(),
            "should error when no CREATE CLUSTER statement"
        );
    }

    #[test]
    fn test_load_clusters_unsupported_statement() {
        let dir = create_test_dir();
        let clusters_dir = dir.path().join("clusters");
        fs::create_dir(&clusters_dir).unwrap();

        fs::write(
            clusters_dir.join("analytics.sql"),
            "CREATE CLUSTER analytics (SIZE = '100cc');\n\
             CREATE TABLE foo (id INT);",
        )
        .unwrap();

        let result = load_clusters(dir.path(), "default", None, &BTreeMap::new());
        assert!(
            result.is_err(),
            "should error on unsupported statement type"
        );
    }

    #[test]
    fn test_load_clusters_grant_target_mismatch() {
        let dir = create_test_dir();
        let clusters_dir = dir.path().join("clusters");
        fs::create_dir(&clusters_dir).unwrap();

        fs::write(
            clusters_dir.join("analytics.sql"),
            "CREATE CLUSTER analytics (SIZE = '100cc');\n\
             GRANT USAGE ON CLUSTER other_cluster TO analyst_role;",
        )
        .unwrap();

        let result = load_clusters(dir.path(), "default", None, &BTreeMap::new());
        assert!(
            result.is_err(),
            "should error when grant targets wrong cluster"
        );
    }

    #[test]
    fn test_load_clusters_comment_target_mismatch() {
        let dir = create_test_dir();
        let clusters_dir = dir.path().join("clusters");
        fs::create_dir(&clusters_dir).unwrap();

        fs::write(
            clusters_dir.join("analytics.sql"),
            "CREATE CLUSTER analytics (SIZE = '100cc');\n\
             COMMENT ON CLUSTER other_cluster IS 'wrong target';",
        )
        .unwrap();

        let result = load_clusters(dir.path(), "default", None, &BTreeMap::new());
        assert!(
            result.is_err(),
            "should error when comment targets wrong cluster"
        );
    }

    #[test]
    fn test_load_clusters_multiple_files() {
        let dir = create_test_dir();
        let clusters_dir = dir.path().join("clusters");
        fs::create_dir(&clusters_dir).unwrap();

        fs::write(
            clusters_dir.join("analytics.sql"),
            "CREATE CLUSTER analytics (SIZE = '100cc');",
        )
        .unwrap();

        fs::write(
            clusters_dir.join("quickstart.sql"),
            "CREATE CLUSTER quickstart (SIZE = '25cc', REPLICATION FACTOR = 2);",
        )
        .unwrap();

        let result = load_clusters(dir.path(), "default", None, &BTreeMap::new()).unwrap();
        assert_eq!(result.len(), 2);
        // Sorted by filename
        assert_eq!(result[0].name, "analytics");
        assert_eq!(result[1].name, "quickstart");
    }

    #[test]
    fn test_load_clusters_multi_variant_valid() {
        let dir = create_test_dir();
        let clusters_dir = dir.path().join("clusters");
        fs::create_dir(&clusters_dir).unwrap();

        fs::write(
            clusters_dir.join("analytics.sql"),
            "CREATE CLUSTER analytics (SIZE = '100cc');",
        )
        .unwrap();
        fs::write(
            clusters_dir.join("analytics__staging.sql"),
            "CREATE CLUSTER analytics (SIZE = '25cc');",
        )
        .unwrap();

        // With staging profile, should pick staging variant
        let result = load_clusters(dir.path(), "staging", None, &BTreeMap::new()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "analytics");
        assert_eq!(
            extract_size(&result[0].create_stmt),
            Some("25cc".to_string())
        );
    }

    #[test]
    fn test_load_clusters_multi_variant_fallback_default() {
        let dir = create_test_dir();
        let clusters_dir = dir.path().join("clusters");
        fs::create_dir(&clusters_dir).unwrap();

        fs::write(
            clusters_dir.join("analytics.sql"),
            "CREATE CLUSTER analytics (SIZE = '100cc');",
        )
        .unwrap();
        fs::write(
            clusters_dir.join("analytics__staging.sql"),
            "CREATE CLUSTER analytics (SIZE = '25cc');",
        )
        .unwrap();

        // With prod profile (no match), should fall back to default
        let result = load_clusters(dir.path(), "prod", None, &BTreeMap::new()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "analytics");
        assert_eq!(
            extract_size(&result[0].create_stmt),
            Some("100cc".to_string())
        );
    }

    #[test]
    fn test_load_clusters_invalid_variant_errors() {
        let dir = create_test_dir();
        let clusters_dir = dir.path().join("clusters");
        fs::create_dir(&clusters_dir).unwrap();

        fs::write(
            clusters_dir.join("analytics.sql"),
            "CREATE CLUSTER analytics (SIZE = '100cc');",
        )
        .unwrap();
        // Invalid staging variant: name mismatch
        fs::write(
            clusters_dir.join("analytics__staging.sql"),
            "CREATE CLUSTER wrong_name (SIZE = '25cc');",
        )
        .unwrap();

        let result = load_clusters(dir.path(), "default", None, &BTreeMap::new());
        assert!(
            result.is_err(),
            "invalid variant should error even when not active profile"
        );
    }
}
