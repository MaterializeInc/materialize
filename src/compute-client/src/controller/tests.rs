// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use mz_repr::refresh_schedule::{RefreshEvery, RefreshSchedule};
use mz_repr::{GlobalId, Timestamp};
use mz_storage_types::errors::CollectionMissing;
use mz_storage_types::freshness::{FreshnessLeaf, FreshnessTree, Ticker};
use mz_storage_types::time_dependence::{TimeDependence, TimeDependenceError};

use super::{merge_freshness_trees, walk_freshness_trees};

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

fn gid(id: u64) -> GlobalId {
    GlobalId::User(id)
}

fn fixed(interval_ms: u64) -> FreshnessTree {
    FreshnessTree::Leaf(FreshnessLeaf::Fixed(Ticker::Aligned { interval_ms }))
}

/// The tree of the walk over `index_ids` and `source_ids`, where source 10 is an ingestion in
/// storage, source 11 an ingestion in storage, source 12 a compute object, and index 20 a
/// compute introspection collection. Source 13 and index 22 have no tree.
fn walk_with_inputs(
    index_ids: &[u64],
    source_ids: &[u64],
    used: &[u64],
    schedule: Option<&RefreshSchedule>,
) -> Result<(Option<FreshnessTree>, BTreeMap<GlobalId, FreshnessTree>), TimeDependenceError> {
    let used: BTreeSet<_> = used.iter().copied().map(gid).collect();
    walk_freshness_trees(
        index_ids.iter().copied().map(gid),
        source_ids.iter().copied().map(gid),
        &used,
        schedule,
        |id| match id {
            GlobalId::User(20) => Ok(Some(fixed(1000))),
            GlobalId::User(22) => Ok(None),
            id => Err(CollectionMissing(id)),
        },
        |id| match id {
            GlobalId::User(12) => Some(Some(fixed(5))),
            _ => None,
        },
        // The storage lookup would also answer for the compute object, to show it is not used.
        |id| match id {
            GlobalId::User(10 | 11 | 12) => ingestion(match id {
                GlobalId::User(n) => n,
                _ => unreachable!(),
            }),
            _ => None,
        },
    )
}

/// The tree of [`walk_with_inputs`].
fn walk(
    index_ids: &[u64],
    source_ids: &[u64],
    used: &[u64],
    schedule: Option<&RefreshSchedule>,
) -> Result<Option<FreshnessTree>, TimeDependenceError> {
    walk_with_inputs(index_ids, source_ids, used, schedule).map(|(tree, _)| tree)
}

#[mz_ore::test]
fn freshness_walk_inputs_have_a_tree() {
    let (_, inputs) =
        walk_with_inputs(&[20, 22], &[10, 11, 12, 13], &[10, 12, 13, 20, 22], None).unwrap();
    assert_eq!(
        inputs,
        BTreeMap::from([
            (gid(10), ingestion(10).unwrap()),
            (gid(12), fixed(5)),
            (gid(20), fixed(1000)),
        ])
    );
}

#[mz_ore::test]
fn freshness_walk_joins_two_ingestions() {
    let tree = walk(&[], &[10, 11], &[10, 11], None).unwrap();
    assert_eq!(
        tree,
        Some(FreshnessTree::Min(vec![
            ingestion(10).unwrap(),
            ingestion(11).unwrap()
        ]))
    );

    // The time dependence derived from the same inputs is unchanged by the walk.
    let dependence = TimeDependence::merge(
        vec![
            Some(TimeDependence::default()),
            Some(TimeDependence::default()),
        ],
        None,
    );
    assert_eq!(dependence, Some(TimeDependence::default()));
}

#[mz_ore::test]
fn freshness_walk_ignores_unused_imports() {
    let tree = walk(&[20], &[10, 11], &[10], None).unwrap();
    assert_eq!(tree, ingestion(10));
}

#[mz_ore::test]
fn freshness_walk_prefers_compute_tree_over_storage() {
    let tree = walk(&[], &[12], &[12], None).unwrap();
    assert_eq!(tree, Some(fixed(5)));
}

#[mz_ore::test]
fn freshness_walk_includes_introspection_index() {
    let tree = walk(&[20], &[10], &[10, 20], None).unwrap();
    assert_eq!(
        tree,
        Some(FreshnessTree::Min(vec![
            ingestion(10).unwrap(),
            fixed(1000)
        ]))
    );
}

#[mz_ore::test]
fn freshness_walk_wraps_refresh_schedule() {
    let schedule = RefreshSchedule {
        everies: vec![RefreshEvery {
            interval: Duration::from_secs(60),
            aligned_to: Timestamp::MIN,
        }],
        ats: vec![],
    };
    let tree = walk(&[], &[10], &[10], Some(&schedule)).unwrap();
    assert_eq!(
        tree,
        Some(FreshnessTree::Refresh {
            schedule,
            input: Box::new(ingestion(10).unwrap()),
        })
    );
}

#[mz_ore::test]
fn freshness_walk_reports_missing_used_index() {
    let err = walk(&[21], &[], &[21], None).unwrap_err();
    assert!(matches!(err, TimeDependenceError::CollectionMissing(id) if id == gid(21)));
}
