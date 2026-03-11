//! Network policy definition loading and validation.
//!
//! Loads network policy definitions from `<root>/network_policies/` directory. Each `.sql` file
//! defines a single network policy with a required `CREATE NETWORK POLICY` statement and optional
//! `GRANT` and `COMMENT` statements.

use crate::project::error::{
    LoadError, ProjectError, ValidationError, ValidationErrorKind, ValidationErrors,
};
use crate::project::parser::{parse_statements_with_context, statement_type_name};
use crate::project::profile_files::collect_and_resolve_sql_files;
use mz_sql_parser::ast::{
    CommentObjectType, CommentStatement, CreateNetworkPolicyStatement, GrantPrivilegesStatement,
    GrantTargetSpecification, GrantTargetSpecificationInner, ObjectType, Raw, RawNetworkPolicyName,
    Statement,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// A parsed network policy definition from a `.sql` file in the `network_policies/` directory.
pub struct NetworkPolicyDefinition {
    /// Network policy name (derived from filename and validated against CREATE statement).
    pub name: String,
    /// Path to the source `.sql` file.
    pub path: PathBuf,
    /// The CREATE NETWORK POLICY statement.
    pub create_stmt: CreateNetworkPolicyStatement<Raw>,
    /// Optional GRANT statements targeting this network policy.
    pub grants: Vec<GrantPrivilegesStatement<Raw>>,
    /// Optional COMMENT statements targeting this network policy.
    pub comments: Vec<CommentStatement<Raw>>,
}

/// Load all network policy definitions from `<root>/network_policies/`.
///
/// Returns an empty vec if `network_policies/` doesn't exist (the directory is optional).
pub fn load_network_policies(
    root: &Path,
    profile: &str,
    variables: &BTreeMap<String, String>,
) -> Result<Vec<NetworkPolicyDefinition>, ProjectError> {
    let policies_dir = root.join("network-policies");

    if !policies_dir.exists() {
        return Ok(vec![]);
    }

    if !policies_dir.is_dir() {
        return Err(LoadError::RootNotDirectory { path: policies_dir }.into());
    }

    let resolved = collect_and_resolve_sql_files(&policies_dir, profile)?;

    let mut definitions = Vec::new();
    let mut errors = Vec::new();

    for (path, expected_name) in resolved {
        // Read file
        let sql = std::fs::read_to_string(&path).map_err(|e| LoadError::FileReadFailed {
            path: path.clone(),
            source: e,
        })?;

        // Parse SQL statements
        let statements = parse_statements_with_context(&sql, path.clone(), variables)?;

        // Classify statements
        match classify_network_policy_statements(&expected_name, &path, statements) {
            Ok(def) => definitions.push(def),
            Err(mut errs) => errors.append(&mut errs),
        }
    }

    if !errors.is_empty() {
        return Err(ValidationErrors::new(errors).into());
    }

    Ok(definitions)
}

/// Classify parsed statements into a `NetworkPolicyDefinition`, returning validation errors.
fn classify_network_policy_statements(
    expected_name: &str,
    path: &Path,
    statements: Vec<Statement<Raw>>,
) -> Result<NetworkPolicyDefinition, Vec<ValidationError>> {
    let mut create_stmts: Vec<CreateNetworkPolicyStatement<Raw>> = Vec::new();
    let mut grants: Vec<GrantPrivilegesStatement<Raw>> = Vec::new();
    let mut comments: Vec<CommentStatement<Raw>> = Vec::new();
    let mut errors = Vec::new();

    for stmt in statements {
        match stmt {
            Statement::CreateNetworkPolicy(s) => {
                create_stmts.push(s);
            }
            Statement::GrantPrivileges(s) => {
                // Validate that the grant targets a network policy
                match &s.target {
                    GrantTargetSpecification::Object {
                        object_type: ObjectType::NetworkPolicy,
                        object_spec_inner: GrantTargetSpecificationInner::Objects { names },
                    } => {
                        // Validate policy name matches
                        for name in names {
                            let target_name = name.to_string();
                            if target_name.to_lowercase() != expected_name.to_lowercase() {
                                errors.push(ValidationError::with_file_and_sql(
                                    ValidationErrorKind::NetworkPolicyGrantTargetMismatch {
                                        target: target_name,
                                        policy_name: expected_name.to_string(),
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
                            ValidationErrorKind::InvalidNetworkPolicyStatement {
                                statement_type: "GRANT (not targeting a network policy)"
                                    .to_string(),
                                policy_name: expected_name.to_string(),
                            },
                            path.to_path_buf(),
                            s.to_string(),
                        ));
                    }
                }
            }
            Statement::Comment(s) => {
                // Validate that the comment targets a network policy
                match &s.object {
                    CommentObjectType::NetworkPolicy { name } => {
                        let target_name = match name {
                            RawNetworkPolicyName::Unresolved(ident) => ident.to_string(),
                            RawNetworkPolicyName::Resolved(id) => id.clone(),
                        };
                        if target_name.to_lowercase() != expected_name.to_lowercase() {
                            errors.push(ValidationError::with_file_and_sql(
                                ValidationErrorKind::NetworkPolicyCommentTargetMismatch {
                                    target: target_name,
                                    policy_name: expected_name.to_string(),
                                },
                                path.to_path_buf(),
                                s.to_string(),
                            ));
                        }
                        comments.push(s);
                    }
                    _ => {
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::InvalidNetworkPolicyStatement {
                                statement_type: "COMMENT (not targeting a network policy)"
                                    .to_string(),
                                policy_name: expected_name.to_string(),
                            },
                            path.to_path_buf(),
                            s.to_string(),
                        ));
                    }
                }
            }
            other => {
                errors.push(ValidationError::with_file_and_sql(
                    ValidationErrorKind::InvalidNetworkPolicyStatement {
                        statement_type: statement_type_name(&other).to_string(),
                        policy_name: expected_name.to_string(),
                    },
                    path.to_path_buf(),
                    other.to_string(),
                ));
            }
        }
    }

    // Validate exactly one CREATE NETWORK POLICY
    if create_stmts.is_empty() {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::NetworkPolicyMissingCreateStatement {
                policy_name: expected_name.to_string(),
            },
            path.to_path_buf(),
        ));
    } else if create_stmts.len() > 1 {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::NetworkPolicyMultipleCreateStatements {
                policy_name: expected_name.to_string(),
            },
            path.to_path_buf(),
        ));
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    let create_stmt = create_stmts.into_iter().next().unwrap();

    // Validate policy name matches filename
    let declared_name = create_stmt.name.to_string();
    if declared_name.to_lowercase() != expected_name.to_lowercase() {
        return Err(vec![ValidationError::with_file(
            ValidationErrorKind::NetworkPolicyNameMismatch {
                declared: declared_name,
                expected: expected_name.to_string(),
            },
            path.to_path_buf(),
        )]);
    }

    Ok(NetworkPolicyDefinition {
        name: expected_name.to_string(),
        path: path.to_path_buf(),
        create_stmt,
        grants,
        comments,
    })
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
    fn test_load_network_policies_no_directory() {
        let dir = create_test_dir();
        let result = load_network_policies(dir.path(), "default", &BTreeMap::new()).unwrap();
        assert!(
            result.is_empty(),
            "should return empty vec when network_policies/ doesn't exist"
        );
    }

    #[test]
    fn test_load_network_policies_basic() {
        let dir = create_test_dir();
        let policies_dir = dir.path().join("network-policies");
        fs::create_dir(&policies_dir).unwrap();

        fs::write(
            policies_dir.join("office_access.sql"),
            "CREATE NETWORK POLICY office_access (RULES (office (action = 'allow', direction = 'ingress', address = '1.2.3.4/28')));\n\
             COMMENT ON NETWORK POLICY office_access IS 'Office network access';",
        )
        .unwrap();

        let result = load_network_policies(dir.path(), "default", &BTreeMap::new()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "office_access");
        assert_eq!(result[0].comments.len(), 1);
    }

    #[test]
    fn test_load_network_policies_create_only() {
        let dir = create_test_dir();
        let policies_dir = dir.path().join("network-policies");
        fs::create_dir(&policies_dir).unwrap();

        fs::write(
            policies_dir.join("office_access.sql"),
            "CREATE NETWORK POLICY office_access (RULES (office (action = 'allow', direction = 'ingress', address = '1.2.3.4/28')));",
        )
        .unwrap();

        let result = load_network_policies(dir.path(), "default", &BTreeMap::new()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "office_access");
        assert!(result[0].grants.is_empty());
        assert!(result[0].comments.is_empty());
    }

    #[test]
    fn test_load_network_policies_name_mismatch() {
        let dir = create_test_dir();
        let policies_dir = dir.path().join("network-policies");
        fs::create_dir(&policies_dir).unwrap();

        fs::write(
            policies_dir.join("office_access.sql"),
            "CREATE NETWORK POLICY wrong_name (RULES (office (action = 'allow', direction = 'ingress', address = '1.2.3.4/28')));",
        )
        .unwrap();

        let result = load_network_policies(dir.path(), "default", &BTreeMap::new());
        assert!(
            result.is_err(),
            "should error when policy name doesn't match filename"
        );
    }

    #[test]
    fn test_load_network_policies_missing_create() {
        let dir = create_test_dir();
        let policies_dir = dir.path().join("network-policies");
        fs::create_dir(&policies_dir).unwrap();

        fs::write(
            policies_dir.join("office_access.sql"),
            "COMMENT ON NETWORK POLICY office_access IS 'Office network access';",
        )
        .unwrap();

        let result = load_network_policies(dir.path(), "default", &BTreeMap::new());
        assert!(
            result.is_err(),
            "should error when no CREATE NETWORK POLICY statement"
        );
    }

    #[test]
    fn test_load_network_policies_unsupported_statement() {
        let dir = create_test_dir();
        let policies_dir = dir.path().join("network-policies");
        fs::create_dir(&policies_dir).unwrap();

        fs::write(
            policies_dir.join("office_access.sql"),
            "CREATE NETWORK POLICY office_access (RULES (office (action = 'allow', direction = 'ingress', address = '1.2.3.4/28')));\n\
             CREATE TABLE foo (id INT);",
        )
        .unwrap();

        let result = load_network_policies(dir.path(), "default", &BTreeMap::new());
        assert!(
            result.is_err(),
            "should error on unsupported statement type"
        );
    }

    #[test]
    fn test_load_network_policies_comment_target_mismatch() {
        let dir = create_test_dir();
        let policies_dir = dir.path().join("network-policies");
        fs::create_dir(&policies_dir).unwrap();

        fs::write(
            policies_dir.join("office_access.sql"),
            "CREATE NETWORK POLICY office_access (RULES (office (action = 'allow', direction = 'ingress', address = '1.2.3.4/28')));\n\
             COMMENT ON NETWORK POLICY other_policy IS 'wrong target';",
        )
        .unwrap();

        let result = load_network_policies(dir.path(), "default", &BTreeMap::new());
        assert!(
            result.is_err(),
            "should error when comment targets wrong policy"
        );
    }

    #[test]
    fn test_load_network_policies_multiple_files() {
        let dir = create_test_dir();
        let policies_dir = dir.path().join("network-policies");
        fs::create_dir(&policies_dir).unwrap();

        fs::write(
            policies_dir.join("office_access.sql"),
            "CREATE NETWORK POLICY office_access (RULES (office (action = 'allow', direction = 'ingress', address = '1.2.3.4/28')));",
        )
        .unwrap();

        fs::write(
            policies_dir.join("vpn_access.sql"),
            "CREATE NETWORK POLICY vpn_access (RULES (vpn (action = 'allow', direction = 'ingress', address = '10.0.0.0/8')));",
        )
        .unwrap();

        let result = load_network_policies(dir.path(), "default", &BTreeMap::new()).unwrap();
        assert_eq!(result.len(), 2);
        // Sorted by filename
        assert_eq!(result[0].name, "office_access");
        assert_eq!(result[1].name, "vpn_access");
    }
}
