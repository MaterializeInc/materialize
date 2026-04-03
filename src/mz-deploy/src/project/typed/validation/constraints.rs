//! Constraint enforcement validation.
//!
//! Validates that constraint enforcement rules are respected based on
//! the object type they are defined on, and that FK targets and columns
//! are valid.

use super::super::types::FullyQualifiedName;
use crate::project::error::{ValidationError, ValidationErrorKind};
use crate::project::object_id::ObjectId;
use crate::project::planned;
use crate::types::{ObjectKind, Types};
use mz_sql_parser::ast::*;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

/// Validates constraint enforcement rules based on the parent object type.
///
/// Enforcement rules:
/// - **Constraints NOT allowed on:** `Table` (includes table-from-source)
/// - **Enforced is NOT allowed on:** `View` (error if `enforced: true` on a view)
/// - **Allowed:** `MaterializedView` (enforced or not), `View` (not-enforced only)
pub(in super::super) fn validate_constraint_enforcement(
    fqn: &FullyQualifiedName,
    constraints: &[CreateConstraintStatement<Raw>],
    offsets: &[usize],
    obj_type: ObjectType,
    errors: &mut Vec<ValidationError>,
) {
    for (i, constraint) in constraints.iter().enumerate() {
        let offset = offsets[i];
        let constraint_sql = format!("{};", constraint);
        let constraint_name = constraint
            .name
            .as_ref()
            .map(|n| n.to_string())
            .unwrap_or_else(|| "<unnamed>".to_string());

        // Constraints are not allowed on tables (includes table-from-source)
        if matches!(obj_type, ObjectType::Table) {
            errors.push(ValidationError::with_file_sql_and_offset(
                ValidationErrorKind::ConstraintNotAllowedOnTable {
                    constraint_name,
                    object_type: "table".to_string(),
                },
                fqn.path.clone(),
                constraint_sql,
                offset,
            ));
            continue;
        }

        // Enforced constraints are not allowed on views
        if constraint.enforced && matches!(obj_type, ObjectType::View) {
            errors.push(ValidationError::with_file_sql_and_offset(
                ValidationErrorKind::EnforcedConstraintNotAllowed {
                    constraint_name,
                    object_type: "view".to_string(),
                },
                fqn.path.clone(),
                constraint_sql,
                offset,
            ));
        }
    }
}

/// Validates FK reference target types.
///
/// Rules:
/// - **Enforced FK:** target must be `Table` or `MaterializedView`
/// - **Not-enforced FK:** additionally allows `View`
/// - **External objects not in types.lock:** assumed to be table (OK)
pub fn validate_constraint_fk_targets(
    project: &planned::Project,
    types: &Types,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();

    for obj in project.iter_objects() {
        for constraint in &obj.typed_object.constraints {
            if constraint.kind != ConstraintKind::ForeignKey {
                continue;
            }

            let Some(refs) = &constraint.references else {
                continue;
            };

            let constraint_name = constraint
                .name
                .as_ref()
                .map(|n| n.to_string())
                .unwrap_or_else(|| "<unnamed>".to_string());
            let constraint_sql = format!("{};", constraint);

            let ref_name = refs.object.name();
            if ref_name.0.len() != 3 {
                continue;
            }

            let ref_id = ObjectId::new(
                ref_name.0[0].to_string(),
                ref_name.0[1].to_string(),
                ref_name.0[2].to_string(),
            );

            // Determine the kind of the referenced object
            let ref_kind = if let Some(ref_obj) = project.find_object(&ref_id) {
                ref_obj.typed_object.stmt.kind()
            } else {
                // External object: look up in types.lock, default to Table
                types.get_kind(&ref_id.to_string())
            };

            let is_valid = match ref_kind {
                ObjectKind::Table | ObjectKind::MaterializedView => true,
                ObjectKind::View => !constraint.enforced,
                _ => false,
            };

            if !is_valid {
                errors.push(ValidationError::with_file_and_sql(
                    ValidationErrorKind::FkInvalidTargetType {
                        constraint_name,
                        ref_object: ref_id.to_string(),
                        ref_kind: ref_kind.to_string(),
                        enforced: constraint.enforced,
                    },
                    PathBuf::from(format!(
                        "{}/{}/{}.sql",
                        obj.id.database, obj.id.schema, obj.id.object
                    )),
                    constraint_sql,
                ));
            }
        }
    }

    errors
}

/// Validates that constraint columns exist on their parent and referenced objects.
///
/// Objects not present in the column map are silently skipped (their columns
/// are not yet known, e.g. views before type checking).
pub fn validate_constraint_columns(
    project: &planned::Project,
    column_map: &BTreeMap<String, BTreeSet<String>>,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();

    for obj in project.iter_objects() {
        for constraint in &obj.typed_object.constraints {
            let parent_fqn = obj.id.to_string().to_lowercase();
            let constraint_name = constraint
                .name
                .as_ref()
                .map(|n| n.to_string())
                .unwrap_or_else(|| "<unnamed>".to_string());
            let constraint_sql = format!("{};", constraint);

            // Validate constraint columns exist on parent object
            if let Some(parent_cols) = column_map.get(&parent_fqn) {
                for col in &constraint.columns {
                    let col_lower = col.as_str().to_lowercase();
                    if !parent_cols.contains(&col_lower) {
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::ConstraintColumnNotFound {
                                constraint_name: constraint_name.clone(),
                                column: col.to_string(),
                                object: obj.id.to_string(),
                                available_columns: parent_cols.iter().cloned().collect(),
                            },
                            PathBuf::from(format!(
                                "{}/{}/{}.sql",
                                obj.id.database, obj.id.schema, obj.id.object
                            )),
                            constraint_sql.clone(),
                        ));
                    }
                }
            }

            // Validate FK reference columns
            if let Some(refs) = &constraint.references {
                let ref_fqn = refs.object.name().to_string().to_lowercase();

                if let Some(ref_cols) = column_map.get(&ref_fqn) {
                    for col in &refs.columns {
                        let col_lower = col.as_str().to_lowercase();
                        if !ref_cols.contains(&col_lower) {
                            errors.push(ValidationError::with_file_and_sql(
                                ValidationErrorKind::FkRefColumnNotFound {
                                    constraint_name: constraint_name.clone(),
                                    column: col.to_string(),
                                    ref_object: refs.object.name().to_string(),
                                    available_columns: ref_cols.iter().cloned().collect(),
                                },
                                PathBuf::from(format!(
                                    "{}/{}/{}.sql",
                                    obj.id.database, obj.id.schema, obj.id.object
                                )),
                                constraint_sql.clone(),
                            ));
                        }
                    }
                }
            }
        }
    }

    errors
}
