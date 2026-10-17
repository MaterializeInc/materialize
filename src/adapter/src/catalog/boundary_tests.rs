// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use mz_catalog::memory::objects::CatalogItem;
use mz_repr::CatalogItemId;
use mz_sql::catalog::SessionCatalog;
use mz_sql::names::{ObjectId, ResolvedDatabaseSpecifier, SchemaSpecifier};

use super::{Catalog, CatalogReadRecorder, CatalogReadSet, CatalogState};
use crate::AdapterError;

const ROOT: CatalogItemId = CatalogItemId::User(1 << 40);
const CHILD: CatalogItemId = CatalogItemId::User((1 << 40) + 1);

fn fixture(state: &CatalogState) -> CatalogState {
    let mut state = state.clone();
    // Logs have no progress collection, so the fixture controls the full closure.
    let template = state
        .entry_by_id
        .values()
        .find(|entry| matches!(entry.item(), CatalogItem::Log(_)))
        .expect("debug catalog has log sources")
        .clone();
    for id in [ROOT, CHILD] {
        let mut entry = template.clone();
        entry.id = id;
        entry.used_by.clear();
        entry.referenced_by.clear();
        state.entry_by_id.insert(id, entry);
    }
    state
}

fn comment(state: &mut CatalogState, column: Option<usize>, value: Option<&str>) {
    let id = state.get_entry(&ROOT).comment_object_id();
    Arc::make_mut(&mut state.comments).update_comment(id, column, value.map(str::to_owned));
}

fn read_comments(state: &CatalogState) -> CatalogReadSet {
    let recorder = CatalogReadRecorder::default();
    let catalog = state
        .for_system_session()
        .with_read_recorder(recorder.clone());
    assert_eq!(
        catalog.get_item_comments(&ROOT),
        state
            .comments
            .get_object_comments(state.get_entry(&ROOT).comment_object_id()),
    );
    recorder.snapshot()
}

fn assert_conflict(reads: &CatalogReadSet, state: &CatalogState) {
    assert!(matches!(
        reads.check(state),
        Err(AdapterError::DDLTransactionRace)
    ));
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // Catalog bootstrap uses unsupported foreign functions.
async fn absent_comments_are_observed_and_missing_items_are_safe() {
    Catalog::with_debug(|catalog| async move {
        let mut state = fixture(catalog.state());
        let reads = read_comments(&state);
        assert!(reads.check(&state).is_ok());
        comment(&mut state, None, Some("object documentation"));
        assert_conflict(&reads, &state);
        comment(&mut state, None, None);
        assert!(reads.check(&state).is_ok());
        state.entry_by_id.remove(&ROOT);
        assert_conflict(&reads, &state);
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // Catalog bootstrap uses unsupported foreign functions.
async fn present_object_comments_detect_edits_and_removal() {
    Catalog::with_debug(|catalog| async move {
        let mut state = fixture(catalog.state());
        comment(&mut state, None, Some("object documentation"));
        let reads = read_comments(&state);
        comment(&mut state, None, Some("changed documentation"));
        assert_conflict(&reads, &state);
        comment(&mut state, None, None);
        assert_conflict(&reads, &state);
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // Catalog bootstrap uses unsupported foreign functions.
async fn column_comments_are_observed_independently_of_object_comments() {
    Catalog::with_debug(|catalog| async move {
        let mut state = fixture(catalog.state());
        comment(&mut state, None, Some("object documentation"));
        let absent = read_comments(&state);
        comment(&mut state, Some(0), Some("column documentation"));
        assert_conflict(&absent, &state);
        let present = read_comments(&state);
        comment(&mut state, Some(0), Some("changed column documentation"));
        assert_conflict(&present, &state);
        comment(&mut state, Some(0), None);
        assert_conflict(&present, &state);
        assert!(absent.check(&state).is_ok());
        catalog.expire().await;
    })
    .await;
}

fn read_dependents(state: &CatalogState) -> [CatalogReadSet; 2] {
    let objects = CatalogReadRecorder::default();
    let items = CatalogReadRecorder::default();
    let object_answer = state
        .for_system_session()
        .with_read_recorder(objects.clone())
        .object_dependents(&vec![ObjectId::Item(ROOT)]);
    let item_answer = state
        .for_system_session()
        .with_read_recorder(items.clone())
        .item_dependents(ROOT);
    assert_eq!(object_answer, item_answer);
    assert_eq!(item_answer.last(), Some(&ObjectId::Item(ROOT)));
    [objects.snapshot(), items.snapshot()]
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // Catalog bootstrap uses unsupported foreign functions.
async fn dependency_closure_growth_conflicts_only_when_read() {
    Catalog::with_debug(|catalog| async move {
        let mut state = fixture(catalog.state());
        let reads = read_dependents(&state);
        let item_reads = CatalogReadRecorder::default();
        {
            let catalog = state
                .for_system_session()
                .with_read_recorder(item_reads.clone());
            let item = catalog.get_item(&ROOT);
            let _ = (item.id(), item.name(), item.item_type());
        }
        for reads in &reads {
            assert!(reads.check(&state).is_ok());
        }
        state
            .entry_by_id
            .get_mut(&ROOT)
            .expect("root item")
            .used_by
            .push(CHILD);
        state
            .entry_by_id
            .get_mut(&ROOT)
            .expect("root item")
            .referenced_by
            .push(CHILD);
        for reads in &reads {
            assert_conflict(reads, &state);
        }
        assert!(item_reads.snapshot().check(&state).is_ok());
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // Catalog bootstrap uses unsupported foreign functions.
async fn dependency_closure_removal_and_missing_roots_conflict() {
    Catalog::with_debug(|catalog| async move {
        let mut state = fixture(catalog.state());
        state
            .entry_by_id
            .get_mut(&ROOT)
            .expect("root item")
            .used_by
            .push(CHILD);
        let reads = read_dependents(&state);
        state
            .entry_by_id
            .get_mut(&ROOT)
            .expect("root item")
            .used_by
            .clear();
        state.entry_by_id.remove(&CHILD);
        for reads in &reads {
            assert_conflict(reads, &state);
        }
        let reads = read_dependents(&state);
        state.entry_by_id.remove(&ROOT);
        for reads in &reads {
            assert_conflict(reads, &state);
        }
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // Catalog bootstrap uses unsupported foreign functions.
async fn object_dependency_replay_tolerates_a_missing_database() {
    Catalog::with_debug(|catalog| async move {
        let mut state = catalog.state().clone();
        let database = state.database_by_id.values().next().expect("database");
        let database_id = database.id;
        let schema_id = *database.schemas_by_id.keys().next().expect("schema");
        let reads = [
            ObjectId::Database(database_id),
            ObjectId::Schema((
                ResolvedDatabaseSpecifier::Id(database_id),
                SchemaSpecifier::Id(schema_id),
            )),
        ]
        .map(|root| {
            let recorder = CatalogReadRecorder::default();
            let _ = state
                .for_system_session()
                .with_read_recorder(recorder.clone())
                .object_dependents(&vec![root]);
            recorder.snapshot()
        });
        for reads in &reads {
            assert!(reads.check(&state).is_ok());
        }
        state.database_by_id.remove(&database_id);
        for reads in &reads {
            assert_conflict(reads, &state);
        }
        catalog.expire().await;
    })
    .await;
}
