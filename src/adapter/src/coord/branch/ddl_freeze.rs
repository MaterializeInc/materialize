// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Source-side DDL freeze for items referenced by a live branch.
//!
//! While a branch holds references to a source object's persist blobs, the
//! source object cannot be altered, dropped, or renamed without risking
//! invariant violation. This module surfaces a single helper,
//! [`source_ddl_blocked_by_branch`], that the planner consults from
//! `ALTER TABLE`, `DROP TABLE`, `RENAME`, and `DROP SCHEMA` paths.

use mz_repr::CatalogItemId;

/// Information about which branches are blocking source-side DDL on a given
/// catalog item. The error path uses these names to render a clear message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockingBranches {
    pub blocking_branches: Vec<String>,
}

/// A trait the catalog implements to answer "are there live `BranchDescriptor`
/// rows that reference `source_id`?". Pulled out as a trait so the helper is
/// testable without a full catalog.
pub trait BranchReferenceLookup {
    fn branches_referencing(&self, source_id: CatalogItemId) -> Vec<String>;
}

/// Returns `Some(blocking_branches)` if any live branch's descriptor names
/// `source_id` as its `source_catalog_id`. Returns `None` if no branch
/// references it.
///
/// Planners for `ALTER TABLE`, `DROP TABLE`, `RENAME`, and `DROP SCHEMA`
/// call this on every source item they intend to mutate and surface the
/// branch names back to the user.
pub fn source_ddl_blocked_by_branch<L: BranchReferenceLookup>(
    catalog: &L,
    source_id: CatalogItemId,
) -> Option<BlockingBranches> {
    let names = catalog.branches_referencing(source_id);
    if names.is_empty() {
        None
    } else {
        Some(BlockingBranches {
            blocking_branches: names,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    struct StubCatalog {
        index: BTreeMap<CatalogItemId, Vec<String>>,
    }

    impl BranchReferenceLookup for StubCatalog {
        fn branches_referencing(&self, source_id: CatalogItemId) -> Vec<String> {
            self.index.get(&source_id).cloned().unwrap_or_default()
        }
    }

    fn catalog(rows: &[(u64, &[&str])]) -> StubCatalog {
        let index = rows
            .iter()
            .map(|(id, names)| {
                (
                    CatalogItemId::User(*id),
                    names.iter().map(|s| s.to_string()).collect(),
                )
            })
            .collect();
        StubCatalog { index }
    }

    #[mz_ore::test]
    fn no_branches_allows_ddl() {
        let cat = catalog(&[]);
        assert!(source_ddl_blocked_by_branch(&cat, CatalogItemId::User(1)).is_none());
    }

    #[mz_ore::test]
    fn single_branch_blocks_and_names_itself() {
        let cat = catalog(&[(1, &["nightly"])]);
        let blocking = source_ddl_blocked_by_branch(&cat, CatalogItemId::User(1))
            .expect("blocked");
        assert_eq!(
            blocking.blocking_branches,
            vec!["nightly".to_owned()]
        );
    }

    #[mz_ore::test]
    fn multiple_branches_all_reported() {
        let cat = catalog(&[(1, &["nightly", "experiment_a", "experiment_b"])]);
        let blocking = source_ddl_blocked_by_branch(&cat, CatalogItemId::User(1))
            .expect("blocked");
        assert_eq!(blocking.blocking_branches.len(), 3);
    }

    #[mz_ore::test]
    fn unrelated_source_not_blocked() {
        let cat = catalog(&[(1, &["nightly"])]);
        // A different source has no branches; the freeze is scoped per item.
        assert!(source_ddl_blocked_by_branch(&cat, CatalogItemId::User(2)).is_none());
    }
}
