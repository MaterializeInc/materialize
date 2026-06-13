// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Builds the shared base catalog forked by per-task typechecks.

use super::catalog::CatalogRuntime;
use super::convert::{create_catalog_item_sql, relation_desc_to_columns};
use super::{ObjectTypeCheckError, TypeCheckError};
use crate::project::ast::Statement;
use crate::project::ir::compiled::FullyQualifiedName;
use crate::project::ir::graph::Project;
use crate::project::ir::object_id::ObjectId;
use crate::types::{ColumnType, Types};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Build the shared catalog used by every per-task typecheck.
///
/// `restrict` filters which non-view project objects and external types are
/// registered. `Some(set)` registers only objects whose `ObjectId` is in the
/// set (used by incremental typechecking to bootstrap just the deps a dirty
/// closure transitively needs). `None` registers everything.
///
/// Errors from registering non-typechecked objects are accumulated; if any are
/// present after this phase, the caller should abort before running phase 2.
pub(super) fn bootstrap_catalog(
    project: &Project,
    external_types: &Types,
    restrict: Option<&BTreeSet<ObjectId>>,
) -> Result<
    (
        Arc<CatalogRuntime>,
        BTreeMap<ObjectId, BTreeMap<String, ColumnType>>,
    ),
    TypeCheckError,
> {
    let mut runtime = CatalogRuntime::open()?;
    runtime.bootstrap_namespaces(project, external_types);

    let included = |id: &ObjectId| restrict.is_none_or(|set| set.contains(id));

    let mut base_columns: BTreeMap<ObjectId, BTreeMap<String, ColumnType>> = BTreeMap::new();
    let mut errors: Vec<ObjectTypeCheckError> = Vec::new();
    let mut registered_from_create: BTreeSet<ObjectId> = BTreeSet::new();
    for db_obj in project.iter_objects() {
        if matches!(
            db_obj.typed_object.stmt,
            Statement::CreateView(_) | Statement::CreateMaterializedView(_)
        ) {
            continue;
        }
        let object_id = db_obj.id.clone();
        if !included(&object_id) {
            continue;
        }
        let fqn: FullyQualifiedName = object_id.clone().into();
        let Some(sql) = create_catalog_item_sql(&db_obj.typed_object.stmt, &fqn) else {
            continue;
        };
        match runtime.create_item(&object_id, &sql) {
            Ok(desc) => {
                base_columns.insert(object_id.clone(), relation_desc_to_columns(&desc));
                registered_from_create.insert(object_id);
            }
            Err(err) => errors.push(err),
        }
    }

    for (id, columns) in &external_types.tables {
        if registered_from_create.contains(id) || !included(id) {
            continue;
        }
        // System catalogs are already in the base catalog; nothing to seed.
        if id.database().is_none() {
            continue;
        }
        runtime.create_stub_table(id, columns)?;
    }

    if !errors.is_empty() {
        return Err(TypeCheckError::Multiple(errors));
    }

    Ok((Arc::new(runtime), base_columns))
}
