// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Helper function for dataflow expiration checks.

use crate::catalog::Catalog;
use mz_compute_types::dataflows::{RefreshDep, RefreshDepIndex};
use mz_repr::GlobalId;

impl Catalog {
    /// Recursive function.
    pub(crate) fn get_refresh_dependencies(
        &self,
        deps: impl Iterator<Item = GlobalId>,
        deps_tree: &mut Vec<RefreshDep>,
    ) -> Option<RefreshDepIndex> {
        let start = deps_tree.len();
        for dep in deps {
            let entry = self.get_entry(&dep);
            let refresh_dep_index =
                self.get_refresh_dependencies(entry.uses().into_iter(), deps_tree);
            let refresh_schedule = entry
                .materialized_view()
                .and_then(|mv| mv.refresh_schedule.clone());
            if refresh_dep_index.is_some() || refresh_schedule.is_some() {
                deps_tree.push(RefreshDep {
                    refresh_dep_index,
                    refresh_schedule,
                });
            }
        }
        let end = deps_tree.len();
        (end > start).then_some(RefreshDepIndex { start, end })
    }
}
