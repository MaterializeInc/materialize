// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::GlobalId;
use mz_storage_types::freshness::{FreshnessLeaf, FreshnessTree};
use mz_storage_types::time_dependence::TimeDependence;

use super::merge_freshness_trees;

fn ingestion(id: u64) -> Option<FreshnessTree> {
    Some(FreshnessTree::Leaf(FreshnessLeaf::Ingestion(
        GlobalId::User(id),
    )))
}

#[mz_ore::test]
fn freshness_tree_of_join_is_min_of_ingestions() {
    // The inputs of a dataflow joining two ingestions, with an input without time dependence
    // that does not contribute, like `determine_time_dependence`.
    let inputs = vec![ingestion(2), None, ingestion(1)];
    let tree = merge_freshness_trees(inputs, None);
    assert_eq!(
        tree,
        Some(FreshnessTree::Min(vec![
            ingestion(1).unwrap(),
            ingestion(2).unwrap()
        ]))
    );

    // The time dependence of the same dataflow is unchanged.
    let dependence = TimeDependence::merge(
        vec![
            Some(TimeDependence::default()),
            None,
            Some(TimeDependence::default()),
        ],
        None,
    );
    assert_eq!(dependence, Some(TimeDependence::default()));
}

#[mz_ore::test]
fn freshness_tree_without_dependence_is_none() {
    assert_eq!(merge_freshness_trees(vec![None, None], None), None);
    assert_eq!(merge_freshness_trees(vec![], None), None);
}
