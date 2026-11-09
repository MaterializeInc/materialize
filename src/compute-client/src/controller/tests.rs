// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use mz_compute_types::dataflows::{LirDataflowDescription, SourceImport};
use mz_compute_types::sources::{SourceInstanceArguments, SourceInstanceDesc};
use mz_repr::{GlobalId, SqlRelationType};
use mz_storage_types::time_dependence::{TimeDependence, TimeDependenceError};
use timely::progress::Antichain;

use super::merge_import_dependences;

/// A dataflow maintaining an index on a view that reads the source `source`.
fn index_over_source(source: GlobalId) -> LirDataflowDescription {
    let mut dataflow = LirDataflowDescription::new("index".into());
    dataflow.source_imports.insert(
        source,
        SourceImport {
            desc: SourceInstanceDesc {
                storage_metadata: (),
                arguments: SourceInstanceArguments { operators: None },
                typ: SqlRelationType::empty(),
            },
            monotonic: false,
            with_snapshot: true,
            upper: Antichain::new(),
        },
    );
    dataflow
}

fn lookup(
    dependences: &BTreeMap<GlobalId, Option<TimeDependence>>,
    id: GlobalId,
) -> Result<Option<TimeDependence>, TimeDependenceError> {
    dependences
        .get(&id)
        .cloned()
        .ok_or(TimeDependenceError::CollectionMissing(id))
}

#[mz_ore::test]
fn freshness_dependence_of_index_over_load_generator() {
    let source = GlobalId::User(1);
    let dataflow = index_over_source(source);
    let used_imports = BTreeSet::from([source]);

    // A load generator: no time dependence, but a wall-clock freshness dependence.
    let time = BTreeMap::from([(source, None)]);
    let freshness = BTreeMap::from([(source, Some(TimeDependence::default()))]);

    let merge = |dependences: &BTreeMap<_, _>| {
        merge_import_dependences(
            &dataflow,
            &used_imports,
            |id| lookup(dependences, id),
            |id| lookup(dependences, id),
        )
        .expect("all imports known")
    };
    assert_eq!(merge(&time), None);
    assert_eq!(merge(&freshness), Some(TimeDependence::default()));
}

#[mz_ore::test]
fn freshness_dependence_ignores_unused_imports() {
    let source = GlobalId::User(1);
    let dataflow = index_over_source(source);
    let freshness = BTreeMap::from([(source, Some(TimeDependence::default()))]);

    let merged = merge_import_dependences(
        &dataflow,
        &BTreeSet::new(),
        |id| lookup(&freshness, id),
        |id| lookup(&freshness, id),
    )
    .expect("all imports known");
    assert_eq!(merged, None);
}
