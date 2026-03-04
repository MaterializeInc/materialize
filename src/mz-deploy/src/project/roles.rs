//! Role definition loading and validation.
//!
//! Loads role definitions from `<root>/roles/` directory. Each `.sql` file
//! defines a single role with a required `CREATE ROLE` statement and optional
//! `ALTER ROLE`, `GRANT ROLE`, and `COMMENT` statements.

use crate::project::error::{
    LoadError, ProjectError, ValidationError, ValidationErrorKind, ValidationErrors,
};
use crate::project::parser::{parse_statements_with_context, statement_type_name};
use mz_sql_parser::ast::{
    AlterRoleStatement, CommentObjectType, CommentStatement, CreateRoleStatement,
    GrantRoleStatement, Raw, Statement,
};
use std::path::{Path, PathBuf};

/// A parsed role definition from a `.sql` file in the `roles/` directory.
pub struct RoleDefinition {
    /// Role name (derived from filename and validated against CREATE statement).
    pub name: String,
    /// Path to the source `.sql` file.
    pub path: PathBuf,
    /// The CREATE ROLE statement.
    pub create_stmt: CreateRoleStatement,
    /// Optional ALTER ROLE statements for this role.
    pub alter_stmts: Vec<AlterRoleStatement<Raw>>,
    /// Optional GRANT ROLE statements granting this role to members.
    pub grants: Vec<GrantRoleStatement<Raw>>,
    /// Optional COMMENT statements targeting this role.
    pub comments: Vec<CommentStatement<Raw>>,
}

/// Load all role definitions from `<root>/roles/`.
///
/// Returns an empty vec if `roles/` doesn't exist (the directory is optional).
pub fn load_roles(root: &Path) -> Result<Vec<RoleDefinition>, ProjectError> {
    let roles_dir = root.join("roles");

    if !roles_dir.exists() {
        return Ok(vec![]);
    }

    if !roles_dir.is_dir() {
        return Err(LoadError::RootNotDirectory { path: roles_dir }.into());
    }

    let mut entries: Vec<_> = std::fs::read_dir(&roles_dir)
        .map_err(|e| LoadError::DirectoryReadFailed {
            path: roles_dir.clone(),
            source: e,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| LoadError::EntryReadFailed {
            directory: roles_dir.clone(),
            source: e,
        })?;

    // Sort for deterministic order
    entries.sort_by_key(|e| e.file_name());

    let mut definitions = Vec::new();
    let mut errors = Vec::new();

    for entry in entries {
        let path = entry.path();

        // Skip non-.sql files
        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }

        // Extract expected role name from filename
        let expected_name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| LoadError::InvalidFileName { path: path.clone() })?
            .to_string();

        // Read file
        let sql = std::fs::read_to_string(&path).map_err(|e| LoadError::FileReadFailed {
            path: path.clone(),
            source: e,
        })?;

        // Parse SQL statements
        let statements = parse_statements_with_context(&sql, path.clone())?;

        // Classify statements
        match classify_role_statements(&expected_name, &path, statements) {
            Ok(def) => definitions.push(def),
            Err(mut errs) => errors.append(&mut errs),
        }
    }

    if !errors.is_empty() {
        return Err(ValidationErrors::new(errors).into());
    }

    Ok(definitions)
}

/// Classify parsed statements into a `RoleDefinition`, returning validation errors.
fn classify_role_statements(
    expected_name: &str,
    path: &Path,
    statements: Vec<Statement<Raw>>,
) -> Result<RoleDefinition, Vec<ValidationError>> {
    let mut create_stmts: Vec<CreateRoleStatement> = Vec::new();
    let mut alter_stmts: Vec<AlterRoleStatement<Raw>> = Vec::new();
    let mut grants: Vec<GrantRoleStatement<Raw>> = Vec::new();
    let mut comments: Vec<CommentStatement<Raw>> = Vec::new();
    let mut errors = Vec::new();

    for stmt in statements {
        match stmt {
            Statement::CreateRole(s) => {
                create_stmts.push(s);
            }
            Statement::AlterRole(s) => {
                // Validate that the ALTER targets this role
                let target_name = s.name.to_string();
                if target_name.to_lowercase() != expected_name.to_lowercase() {
                    errors.push(ValidationError::with_file_and_sql(
                        ValidationErrorKind::RoleAlterTargetMismatch {
                            target: target_name,
                            role_name: expected_name.to_string(),
                        },
                        path.to_path_buf(),
                        s.to_string(),
                    ));
                } else {
                    alter_stmts.push(s);
                }
            }
            Statement::GrantRole(s) => {
                // Validate that this role is among the roles being granted
                let has_match = s
                    .role_names
                    .iter()
                    .any(|r| r.to_string().to_lowercase() == expected_name.to_lowercase());
                if !has_match {
                    let target_names: Vec<String> =
                        s.role_names.iter().map(|r| r.to_string()).collect();
                    errors.push(ValidationError::with_file_and_sql(
                        ValidationErrorKind::RoleGrantTargetMismatch {
                            target: target_names.join(", "),
                            role_name: expected_name.to_string(),
                        },
                        path.to_path_buf(),
                        s.to_string(),
                    ));
                } else {
                    grants.push(s);
                }
            }
            Statement::Comment(s) => {
                // Validate that the comment targets this role
                match &s.object {
                    CommentObjectType::Role { name } => {
                        let target_name = name.to_string();
                        if target_name.to_lowercase() != expected_name.to_lowercase() {
                            errors.push(ValidationError::with_file_and_sql(
                                ValidationErrorKind::RoleCommentTargetMismatch {
                                    target: target_name,
                                    role_name: expected_name.to_string(),
                                },
                                path.to_path_buf(),
                                s.to_string(),
                            ));
                        }
                        comments.push(s);
                    }
                    _ => {
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::InvalidRoleStatement {
                                statement_type: "COMMENT (not targeting a role)".to_string(),
                                role_name: expected_name.to_string(),
                            },
                            path.to_path_buf(),
                            s.to_string(),
                        ));
                    }
                }
            }
            other => {
                errors.push(ValidationError::with_file_and_sql(
                    ValidationErrorKind::InvalidRoleStatement {
                        statement_type: statement_type_name(&other).to_string(),
                        role_name: expected_name.to_string(),
                    },
                    path.to_path_buf(),
                    other.to_string(),
                ));
            }
        }
    }

    // Validate exactly one CREATE ROLE
    if create_stmts.is_empty() {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::RoleMissingCreateStatement {
                role_name: expected_name.to_string(),
            },
            path.to_path_buf(),
        ));
    } else if create_stmts.len() > 1 {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::RoleMultipleCreateStatements {
                role_name: expected_name.to_string(),
            },
            path.to_path_buf(),
        ));
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    let create_stmt = create_stmts.into_iter().next().unwrap();

    // Validate role name matches filename
    let declared_name = create_stmt.name.to_string();
    if declared_name.to_lowercase() != expected_name.to_lowercase() {
        return Err(vec![ValidationError::with_file(
            ValidationErrorKind::RoleNameMismatch {
                declared: declared_name,
                expected: expected_name.to_string(),
            },
            path.to_path_buf(),
        )]);
    }

    Ok(RoleDefinition {
        name: expected_name.to_string(),
        path: path.to_path_buf(),
        create_stmt,
        alter_stmts,
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
    fn test_load_roles_no_directory() {
        let dir = create_test_dir();
        let result = load_roles(dir.path()).unwrap();
        assert!(
            result.is_empty(),
            "should return empty vec when roles/ doesn't exist"
        );
    }

    #[test]
    fn test_load_roles_basic() {
        let dir = create_test_dir();
        let roles_dir = dir.path().join("roles");
        fs::create_dir(&roles_dir).unwrap();

        fs::write(
            roles_dir.join("analyst.sql"),
            "CREATE ROLE analyst INHERIT;\n\
             ALTER ROLE analyst SET cluster TO 'analytics';\n\
             GRANT analyst TO joe, jane;\n\
             COMMENT ON ROLE analyst IS 'Read-only analytics access';",
        )
        .unwrap();

        let result = load_roles(dir.path()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "analyst");
        assert_eq!(result[0].alter_stmts.len(), 1);
        assert_eq!(result[0].grants.len(), 1);
        assert_eq!(result[0].comments.len(), 1);
    }

    #[test]
    fn test_load_roles_create_only() {
        let dir = create_test_dir();
        let roles_dir = dir.path().join("roles");
        fs::create_dir(&roles_dir).unwrap();

        fs::write(roles_dir.join("reader.sql"), "CREATE ROLE reader;").unwrap();

        let result = load_roles(dir.path()).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "reader");
        assert!(result[0].alter_stmts.is_empty());
        assert!(result[0].grants.is_empty());
        assert!(result[0].comments.is_empty());
    }

    #[test]
    fn test_load_roles_name_mismatch() {
        let dir = create_test_dir();
        let roles_dir = dir.path().join("roles");
        fs::create_dir(&roles_dir).unwrap();

        fs::write(roles_dir.join("analyst.sql"), "CREATE ROLE wrong_name;").unwrap();

        let result = load_roles(dir.path());
        assert!(
            result.is_err(),
            "should error when role name doesn't match filename"
        );
    }

    #[test]
    fn test_load_roles_missing_create() {
        let dir = create_test_dir();
        let roles_dir = dir.path().join("roles");
        fs::create_dir(&roles_dir).unwrap();

        fs::write(roles_dir.join("analyst.sql"), "GRANT analyst TO joe;").unwrap();

        let result = load_roles(dir.path());
        assert!(
            result.is_err(),
            "should error when no CREATE ROLE statement"
        );
    }

    #[test]
    fn test_load_roles_unsupported_statement() {
        let dir = create_test_dir();
        let roles_dir = dir.path().join("roles");
        fs::create_dir(&roles_dir).unwrap();

        fs::write(
            roles_dir.join("analyst.sql"),
            "CREATE ROLE analyst;\n\
             CREATE TABLE foo (id INT);",
        )
        .unwrap();

        let result = load_roles(dir.path());
        assert!(
            result.is_err(),
            "should error on unsupported statement type"
        );
    }

    #[test]
    fn test_load_roles_alter_target_mismatch() {
        let dir = create_test_dir();
        let roles_dir = dir.path().join("roles");
        fs::create_dir(&roles_dir).unwrap();

        fs::write(
            roles_dir.join("analyst.sql"),
            "CREATE ROLE analyst;\n\
             ALTER ROLE other_role SET cluster TO 'analytics';",
        )
        .unwrap();

        let result = load_roles(dir.path());
        assert!(
            result.is_err(),
            "should error when ALTER targets wrong role"
        );
    }

    #[test]
    fn test_load_roles_grant_target_mismatch() {
        let dir = create_test_dir();
        let roles_dir = dir.path().join("roles");
        fs::create_dir(&roles_dir).unwrap();

        fs::write(
            roles_dir.join("analyst.sql"),
            "CREATE ROLE analyst;\n\
             GRANT other_role TO joe;",
        )
        .unwrap();

        let result = load_roles(dir.path());
        assert!(
            result.is_err(),
            "should error when grant targets wrong role"
        );
    }

    #[test]
    fn test_load_roles_comment_target_mismatch() {
        let dir = create_test_dir();
        let roles_dir = dir.path().join("roles");
        fs::create_dir(&roles_dir).unwrap();

        fs::write(
            roles_dir.join("analyst.sql"),
            "CREATE ROLE analyst;\n\
             COMMENT ON ROLE other_role IS 'wrong target';",
        )
        .unwrap();

        let result = load_roles(dir.path());
        assert!(
            result.is_err(),
            "should error when comment targets wrong role"
        );
    }

    #[test]
    fn test_load_roles_multiple_files() {
        let dir = create_test_dir();
        let roles_dir = dir.path().join("roles");
        fs::create_dir(&roles_dir).unwrap();

        fs::write(roles_dir.join("analyst.sql"), "CREATE ROLE analyst;").unwrap();

        fs::write(roles_dir.join("writer.sql"), "CREATE ROLE writer;").unwrap();

        let result = load_roles(dir.path()).unwrap();
        assert_eq!(result.len(), 2);
        // Sorted by filename
        assert_eq!(result[0].name, "analyst");
        assert_eq!(result[1].name, "writer");
    }
}
