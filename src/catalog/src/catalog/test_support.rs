// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cross-crate catalog fixtures, excluded from production builds.

use super::Catalog;
use crate::memory::objects::CatalogItem;
use mz_repr::CatalogItemId;
use std::collections::BTreeSet;

/// Reconstructs a fixture item through the native catalog parser.
pub fn parse_item(
    state: &mut super::CatalogState,
    id: mz_repr::GlobalId,
    sql: &str,
    versions: &std::collections::BTreeMap<mz_repr::RelationVersion, mz_repr::GlobalId>,
) -> Result<CatalogItem, crate::memory::error::ItemError> {
    state.with_enable_for_item_parsing(|state| {
        state
            .parse_item_inner(id, sql, versions, None, false, None, None, None)
            .map(|(item, _)| item)
            .map_err(|(error, _)| error)
    })
}

/// Inserts a synthetic chain of `depth + 1` user views into `catalog` where
/// view `base + i` reads from `base + i + 1`. Ids start well above any id
/// the debug catalog assigns so they do not collide with real entries.
///
/// Clones a builtin view as a template rather than constructing a `View` by
/// hand. Read-then-write validation only reads the item type, `uses()`, and
/// the optimized expression's temporal-ness, all of which a builtin view
/// satisfies (user id + non-temporal).
pub fn insert_synthetic_view_chain(catalog: &mut Catalog, base: u64, depth: usize) {
    use mz_ore::cast::CastFrom;
    use mz_sql::names::{DependencyIds, ResolvedIds};

    let template = catalog
        .state
        .entry_by_id
        .values()
        .find(|entry| matches!(entry.item(), CatalogItem::View(_)))
        .expect("debug catalog has builtin views")
        .clone();

    // `uses()` for a view unions `resolved_ids` and `dependencies`, so clear
    // both and point only at the next link.
    for i in 0..=depth {
        let id = CatalogItemId::User(base + u64::cast_from(i));
        let mut entry = template.clone();
        entry.id = id;
        entry.referenced_by = Vec::new();
        entry.used_by = Vec::new();
        let mut resolved_ids = ResolvedIds::empty();
        if i < depth {
            resolved_ids.add_item(CatalogItemId::User(base + u64::cast_from(i + 1)));
        }
        match &mut entry.item {
            CatalogItem::View(view) => {
                view.resolved_ids = resolved_ids;
                view.dependencies = DependencyIds(BTreeSet::new());
            }
            _ => unreachable!("template is a view"),
        }
        catalog.state.entry_by_id.insert(id, entry);
    }
}
