//! Cluster definition loading and validation.
//!
//! Loads cluster definitions from `<root>/clusters/` directory. Each `.sql` file
//! defines a single cluster with a required `CREATE CLUSTER` statement and optional
//! `GRANT` and `COMMENT` statements.

use crate::project::error::{
    LoadError, ProjectError, ValidationError, ValidationErrorKind, ValidationErrors,
};
use crate::project::parser::{parse_statements_with_context, statement_type_name};
use crate::project::profile_files::collect_and_resolve_sql_files;
use mz_sql_parser::ast::{
    ClusterOptionName, CommentObjectType, CommentStatement, CreateClusterStatement,
    GrantPrivilegesStatement, GrantTargetSpecification, GrantTargetSpecificationInner, ObjectType,
    Raw, RawClusterName, Statement, WithOptionValue,
};
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
pub fn load_clusters(root: &Path, profile: &str) -> Result<Vec<ClusterDefinition>, ProjectError> {
    let clusters_dir = root.join("clusters");

    if !clusters_dir.exists() {
        return Ok(vec![]);
    }

    if !clusters_dir.is_dir() {
        return Err(LoadError::RootNotDirectory { path: clusters_dir }.into());
    }

    let resolved = collect_and_resolve_sql_files(&clusters_dir, profile)?;

    let mut definitions = Vec::new();
    let mut errors = Vec::new();

    for (path, expected_name) in resolved {
        // Read file
        let sql = std::fs::read_to_string(&path).map_err(|e| LoadError::FileReadFailed {
            path: path.clone(),
            source: e,
        })?;

        // Parse SQL statements
        let statements = parse_statements_with_context(&sql, path.clone())?;

        // Classify statements
        match classify_cluster_statements(&expected_name, &path, statements) {
            Ok(def) => definitions.push(def),
            Err(mut errs) => errors.append(&mut errs),
        }
    }

    if !errors.is_empty() {
        return Err(ValidationErrors::new(errors).into());
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
        let result = load_clusters(dir.path(), "default").unwrap();
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

        let result = load_clusters(dir.path(), "default").unwrap();
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

        let result = load_clusters(dir.path(), "default").unwrap();
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

        let result = load_clusters(dir.path(), "default");
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

        let result = load_clusters(dir.path(), "default");
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

        let result = load_clusters(dir.path(), "default");
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

        let result = load_clusters(dir.path(), "default");
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

        let result = load_clusters(dir.path(), "default");
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

        let result = load_clusters(dir.path(), "default").unwrap();
        assert_eq!(result.len(), 2);
        // Sorted by filename
        assert_eq!(result[0].name, "analytics");
        assert_eq!(result[1].name, "quickstart");
    }
}
