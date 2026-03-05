//! Identifier validation for database objects.
//!
//! Validates that identifiers (database, schema, object, and cluster names)
//! follow Materialize's naming rules: lowercase letters, digits, underscores,
//! and dollar signs, starting with a letter or underscore.

use super::super::types::FullyQualifiedName;
use crate::project::error::{ValidationError, ValidationErrorKind};
use std::path::PathBuf;

/// The type of identifier being validated (for error messages).
#[derive(Debug, Clone, Copy)]
pub enum IdentifierKind {
    Database,
    Schema,
    Object,
    Cluster,
}

impl std::fmt::Display for IdentifierKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database => write!(f, "database"),
            Self::Schema => write!(f, "schema"),
            Self::Object => write!(f, "object"),
            Self::Cluster => write!(f, "cluster"),
        }
    }
}

/// Validates an identifier follows naming rules.
///
/// # Rules
///
/// - **Start Character**: Must begin with a lowercase letter (a-z, including letters with
///   diacritical marks and non-Latin letters) or an underscore (_).
/// - **Subsequent Characters**: Can include lowercase letters, digits (0-9), underscores (_),
///   or dollar signs ($).
/// - **Case**: All characters must be lowercase.
///
/// # Arguments
///
/// * `name` - The identifier name to validate
/// * `kind` - The type of identifier (for error messages)
///
/// # Returns
///
/// * `Ok(())` if the identifier is valid
/// * `Err(String)` with a descriptive error message if invalid
///
/// # Examples
///
/// ```text
/// Valid identifiers:
///   users, _temp, my_table, café, 日本語, user123, price$
///
/// Invalid identifiers:
///   Users (uppercase)
///   123table (starts with digit)
///   my-table (contains hyphen)
///   MY_TABLE (uppercase)
/// ```
pub fn validate_identifier_format(name: &str, kind: IdentifierKind) -> Result<(), String> {
    if name.is_empty() {
        return Err(format!("{} name cannot be empty", kind));
    }

    let mut chars = name.chars().peekable();

    // Check first character
    if let Some(first) = chars.next() {
        if first.is_uppercase() {
            return Err(format!(
                "{} name '{}' contains uppercase character '{}' at position 1. \
                 Identifiers must be lowercase.",
                kind, name, first
            ));
        }

        if first.is_ascii_digit() {
            return Err(format!(
                "{} name '{}' starts with digit '{}'. \
                 Identifiers must start with a letter or underscore.",
                kind, name, first
            ));
        }

        // First char must be a letter (including unicode letters) or underscore
        if !first.is_alphabetic() && first != '_' {
            return Err(format!(
                "{} name '{}' starts with invalid character '{}'. \
                 Identifiers must start with a letter or underscore.",
                kind, name, first
            ));
        }
    }

    // Check subsequent characters
    for (pos, ch) in chars.enumerate() {
        let position = pos + 2; // +2 because we already consumed first char and positions are 1-indexed

        if ch.is_uppercase() {
            return Err(format!(
                "{} name '{}' contains uppercase character '{}' at position {}. \
                 Identifiers must be lowercase.",
                kind, name, ch, position
            ));
        }

        // Valid subsequent chars: letters (lowercase), digits, underscore, dollar sign
        let is_valid = ch.is_alphabetic() || ch.is_ascii_digit() || ch == '_' || ch == '$';

        if !is_valid {
            return Err(format!(
                "{} name '{}' contains invalid character '{}' at position {}. \
                 Identifiers can only contain letters, digits, underscores, and dollar signs.",
                kind, name, ch, position
            ));
        }
    }

    Ok(())
}

/// Validates all identifiers in a FullyQualifiedName (database, schema, object).
///
/// # Arguments
///
/// * `fqn` - The fully qualified name to validate
/// * `errors` - Vector to collect validation errors
pub fn validate_fqn_identifiers(fqn: &FullyQualifiedName, errors: &mut Vec<ValidationError>) {
    // Validate database name
    if let Err(reason) = validate_identifier_format(fqn.database(), IdentifierKind::Database) {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::InvalidIdentifier {
                name: fqn.database().to_string(),
                reason,
            },
            fqn.path.clone(),
        ));
    }

    // Validate schema name
    if let Err(reason) = validate_identifier_format(fqn.schema(), IdentifierKind::Schema) {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::InvalidIdentifier {
                name: fqn.schema().to_string(),
                reason,
            },
            fqn.path.clone(),
        ));
    }

    // Validate object name
    if let Err(reason) = validate_identifier_format(fqn.object(), IdentifierKind::Object) {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::InvalidIdentifier {
                name: fqn.object().to_string(),
                reason,
            },
            fqn.path.clone(),
        ));
    }
}

/// Validates a cluster name follows naming rules.
///
/// # Arguments
///
/// * `cluster_name` - The cluster name to validate
/// * `path` - The file path (for error reporting)
///
/// # Returns
///
/// * `Ok(())` if valid
/// * `Err(ValidationError)` if invalid
pub fn validate_cluster_name(cluster_name: &str, path: &PathBuf) -> Result<(), ValidationError> {
    validate_identifier_format(cluster_name, IdentifierKind::Cluster).map_err(|reason| {
        ValidationError::with_file(
            ValidationErrorKind::InvalidIdentifier {
                name: cluster_name.to_string(),
                reason,
            },
            path.clone(),
        )
    })
}

/// Validates that the statement's identifier matches the expected file path structure.
///
/// Ensures that the object name in the CREATE statement matches the file name, and
/// that any schema/database qualifiers match the directory structure.
///
/// # Validation Rules
///
/// - The object name must match the file name (without `.sql` extension)
/// - If the statement includes a schema qualifier, it must match the parent directory name
/// - If the statement includes a database qualifier, it must match the grandparent directory name
///
/// # Examples
///
/// Valid mappings:
/// ```text
/// materialize/public/users.sql  ->  CREATE TABLE users (...)
/// materialize/public/users.sql  ->  CREATE TABLE public.users (...)
/// materialize/public/users.sql  ->  CREATE TABLE materialize.public.users (...)
/// ```
///
/// Invalid mappings:
/// ```text
/// materialize/public/users.sql  ->  CREATE TABLE customers (...)  X name mismatch
/// materialize/public/users.sql  ->  CREATE TABLE private.users (...)  X schema mismatch
/// materialize/public/users.sql  ->  CREATE TABLE other.public.users (...)  X database mismatch
/// ```
pub(in super::super) fn validate_ident(
    stmt: &super::super::super::ast::Statement,
    fqn: &FullyQualifiedName,
    errors: &mut Vec<ValidationError>,
) {
    let ident = stmt.ident();

    // The object name in the statement must match the file name
    if ident.object != fqn.object() {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::ObjectNameMismatch {
                declared: ident.object.clone(),
                expected: fqn.object().to_string(),
            },
            fqn.path.clone(),
        ));
    }

    // If the statement includes a schema qualifier, validate it matches the path-derived schema
    if let Some(ref stmt_schema) = ident.schema
        && stmt_schema != fqn.schema()
    {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::SchemaMismatch {
                declared: stmt_schema.clone(),
                expected: fqn.schema().to_string(),
            },
            fqn.path.clone(),
        ));
    }

    // If the statement includes a database qualifier, validate it matches the path-derived database
    if let Some(ref stmt_database) = ident.database
        && stmt_database != fqn.database()
    {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::DatabaseMismatch {
                declared: stmt_database.clone(),
                expected: fqn.database().to_string(),
            },
            fqn.path.clone(),
        ));
    }
}
