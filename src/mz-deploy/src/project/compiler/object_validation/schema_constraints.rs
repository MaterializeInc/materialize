// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Schema-level constraint validation.
//!
//! Validates structural constraints across objects within a schema, such as
//! ensuring storage and computation objects are not mixed in the same schema.

use crate::project::ast::Statement;
use crate::project::error::{ValidationError, ValidationErrorKind};
use crate::project::ir::compiled::DatabaseObject;
use std::path::PathBuf;

/// Validates that a schema doesn't mix storage objects with computation objects.
///
/// This validation prevents accidentally recreating tables or sinks when recreating views,
/// which would cause data loss. Storage and computation objects should be in separate schemas.
///
/// # Object Groups
///
/// - **Storage objects**: Tables, Sinks (can coexist in same schema)
/// - **Computation objects**: Views, Materialized Views (can coexist in same schema)
/// - These two groups CANNOT mix in the same schema
///
/// # Validation Rules
///
/// Valid combinations within a schema:
/// - Tables only
/// - Tables + Sinks
/// - Sinks only
/// - Views only
/// - Views + Materialized Views
/// - Materialized Views only
///
/// Invalid combinations:
/// - Tables + Views
/// - Tables + Materialized Views
/// - Sinks + Views
/// - Sinks + Materialized Views
/// - Tables + Sinks + Views
/// - (any mix of storage and computation)
///
/// # Arguments
///
/// * `schema_name` - The name of the schema being validated
/// * `objects` - All database objects in the schema
/// * `errors` - Vector to collect validation errors
pub(super) fn validate_no_storage_and_computation_in_schema(
    schema_name: &str,
    objects: &[DatabaseObject],
    errors: &mut Vec<ValidationError>,
) {
    let mut has_storage = false;
    let mut has_computation = false;
    let mut storage_names = Vec::new();
    let mut computation_names = Vec::new();

    for obj in objects {
        match &obj.stmt {
            Statement::CreateTable(_)
            | Statement::CreateTableFromSource(_)
            | Statement::CreateSource(_)
            | Statement::CreateSink(_)
            | Statement::CreateSecret(_)
            | Statement::CreateConnection(_) => {
                has_storage = true;
                let ident = obj.stmt.ident();
                storage_names.push(ident.object.clone());
            }
            Statement::CreateView(_) | Statement::CreateMaterializedView(_) => {
                has_computation = true;
                let ident = obj.stmt.ident();
                computation_names.push(ident.object.clone());
            }
        }
    }

    if has_storage && has_computation {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::StorageAndComputationObjectsInSameSchema {
                schema_name: schema_name.to_string(),
                storage_objects: storage_names,
                computation_objects: computation_names,
            },
            PathBuf::from(schema_name),
        ));
    }
}
