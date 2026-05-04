// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordinator-side support machinery for (frontend) read-then write.

use mz_catalog::memory::objects::CatalogItem;
use mz_repr::CatalogItemId;
use mz_sql::catalog::CatalogItemType;

use crate::catalog::Catalog;
use crate::error::AdapterError;

/// Validates that all dependencies are valid for read-then-write operations.
///
/// Ensures all objects the selection depends on are valid for `ReadThenWrite` operations:
///
/// - They do not refer to any objects whose notion of time moves differently than that of
///   user tables. This limitation is meant to ensure no writes occur between this read and the
///   subsequent write.
/// - They do not use mz_now(), whose time produced during read will differ from the write
///   timestamp.
pub(crate) fn validate_read_then_write_dependencies(
    catalog: &Catalog,
    id: &CatalogItemId,
) -> Result<(), AdapterError> {
    use CatalogItemType::*;
    use mz_catalog::memory::objects;
    let mut ids_to_check = Vec::new();
    let valid = match catalog.try_get_entry(id) {
        Some(entry) => {
            if let CatalogItem::View(objects::View {
                locally_optimized_expr: optimized_expr,
                ..
            })
            | CatalogItem::MaterializedView(objects::MaterializedView {
                locally_optimized_expr: optimized_expr,
                ..
            }) = entry.item()
            {
                if optimized_expr.contains_temporal() {
                    return Err(AdapterError::Unsupported(
                        "calls to mz_now in write statements",
                    ));
                }
            }
            match entry.item().typ() {
                typ @ (Func | View | MaterializedView) => {
                    ids_to_check.extend(entry.uses());
                    let valid_id = id.is_user() || matches!(typ, Func);
                    valid_id
                }
                Source | Secret | Connection => false,
                // Cannot select from sinks or indexes.
                Sink | Index => unreachable!(),
                Table => {
                    if !id.is_user() {
                        // We can't read from non-user tables
                        false
                    } else {
                        // We can't read from tables that are source-exports
                        entry.source_export_details().is_none()
                    }
                }
                Type => true,
            }
        }
        None => false,
    };
    if !valid {
        let (object_name, object_type) = match catalog.try_get_entry(id) {
            Some(entry) => {
                let object_name = catalog.resolve_full_name(entry.name(), None).to_string();
                let object_type = match entry.item().typ() {
                    // We only need the disallowed types here; the allowed types are handled above.
                    Source => "source",
                    Secret => "secret",
                    Connection => "connection",
                    Table => {
                        if !id.is_user() {
                            "system table"
                        } else {
                            "source-export table"
                        }
                    }
                    View => "system view",
                    MaterializedView => "system materialized view",
                    _ => "invalid dependency",
                };
                (object_name, object_type.to_string())
            }
            None => (id.to_string(), "unknown".to_string()),
        };
        return Err(AdapterError::InvalidTableMutationSelection {
            object_name,
            object_type,
        });
    }
    for id in ids_to_check {
        validate_read_then_write_dependencies(catalog, &id)?;
    }
    Ok(())
}
