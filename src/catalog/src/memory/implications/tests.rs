// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use crate::memory::objects::TableDataSource;
use mz_repr::{GlobalId, RelationDesc, RelationVersion, VersionedRelationDesc};
use mz_sql::names::ResolvedIds;
use std::collections::BTreeMap;

fn create_test_table(name: &str) -> Table {
    Table {
        desc: VersionedRelationDesc::new(
            RelationDesc::builder()
                .with_column(name, mz_repr::SqlScalarType::String.nullable(false))
                .finish(),
        ),
        create_sql: None,
        collections: BTreeMap::from([(RelationVersion::root(), GlobalId::System(1))]),
        conn_id: None,
        resolved_ids: ResolvedIds::empty(),
        custom_logical_compaction_window: None,
        is_retained_metrics_object: false,
        data_source: TableDataSource::TableWrites { defaults: vec![] },
    }
}

#[mz_ore::test]
fn test_item_state_transitions() {
    // Test None -> Added
    let mut state = CatalogImplicationKind::None;
    assert!(
        state
            .transition("item1".to_string(), None, StateDiff::Addition)
            .is_ok()
    );
    assert!(matches!(state, CatalogImplicationKind::Added(_)));

    // Test Added -> Altered (via retraction)
    let mut state = CatalogImplicationKind::Added("new_item".to_string());
    assert!(
        state
            .transition("old_item".to_string(), None, StateDiff::Retraction)
            .is_ok()
    );
    match &state {
        CatalogImplicationKind::Altered { prev, new } => {
            // The retracted item is the OLD state
            assert_eq!(prev, "old_item");
            // The existing Added item is the NEW state
            assert_eq!(new, "new_item");
        }
        _ => panic!("Expected Altered state"),
    }

    // Test None -> Dropped
    let mut state = CatalogImplicationKind::None;
    assert!(
        state
            .transition(
                "item1".to_string(),
                Some("test_name".to_string()),
                StateDiff::Retraction
            )
            .is_ok()
    );
    assert!(matches!(state, CatalogImplicationKind::Dropped(_, _)));

    // Test Dropped -> Altered (via addition)
    let mut state = CatalogImplicationKind::Dropped("old_item".to_string(), "name".to_string());
    assert!(
        state
            .transition("new_item".to_string(), None, StateDiff::Addition)
            .is_ok()
    );
    match &state {
        CatalogImplicationKind::Altered { prev, new } => {
            // The existing Dropped item is the OLD state
            assert_eq!(prev, "old_item");
            // The added item is the NEW state
            assert_eq!(new, "new_item");
        }
        _ => panic!("Expected Altered state"),
    }

    // Test invalid transitions
    let mut state = CatalogImplicationKind::Added("item".to_string());
    assert!(
        state
            .transition("item2".to_string(), None, StateDiff::Addition)
            .is_err()
    );

    let mut state = CatalogImplicationKind::Dropped("item".to_string(), "name".to_string());
    assert!(
        state
            .transition("item2".to_string(), None, StateDiff::Retraction)
            .is_err()
    );
}

#[mz_ore::test]
fn test_table_absorb_state_machine() {
    let table1 = create_test_table("table1");
    let table2 = create_test_table("table2");

    // Test None -> AddTable
    let mut cmd = CatalogImplication::None;
    cmd.absorb_table(
        table1.clone(),
        Some("schema.table1".to_string()),
        StateDiff::Addition,
    );
    // Check that we have an Added state
    match &cmd {
        CatalogImplication::Table(state) => match state {
            CatalogImplicationKind::Added(t) => {
                assert_eq!(t.desc.latest().arity(), table1.desc.latest().arity())
            }
            _ => panic!("Expected Added state"),
        },
        _ => panic!("Expected Table command"),
    }

    // Test AddTable -> AlterTable (via retraction)
    // This tests the bug fix: when we have AddTable(table1) and receive Retraction(table2),
    // table2 is the old state being removed, table1 is the new state
    cmd.absorb_table(
        table2.clone(),
        Some("schema.table2".to_string()),
        StateDiff::Retraction,
    );
    match &cmd {
        CatalogImplication::Table(state) => match state {
            CatalogImplicationKind::Altered { prev, new } => {
                // Verify the fix: prev should be the retracted table, new should be the added table
                assert_eq!(prev.desc.latest().arity(), table2.desc.latest().arity());
                assert_eq!(new.desc.latest().arity(), table1.desc.latest().arity());
            }
            _ => panic!("Expected Altered state"),
        },
        _ => panic!("Expected Table command"),
    }

    // Test None -> DropTable
    let mut cmd = CatalogImplication::None;
    cmd.absorb_table(
        table1.clone(),
        Some("schema.table1".to_string()),
        StateDiff::Retraction,
    );
    match &cmd {
        CatalogImplication::Table(state) => match state {
            CatalogImplicationKind::Dropped(t, name) => {
                assert_eq!(t.desc.latest().arity(), table1.desc.latest().arity());
                assert_eq!(name, "schema.table1");
            }
            _ => panic!("Expected Dropped state"),
        },
        _ => panic!("Expected Table command"),
    }

    // Test DropTable -> AlterTable (via addition)
    cmd.absorb_table(
        table2.clone(),
        Some("schema.table2".to_string()),
        StateDiff::Addition,
    );
    match &cmd {
        CatalogImplication::Table(state) => match state {
            CatalogImplicationKind::Altered { prev, new } => {
                // prev should be the dropped table, new should be the added table
                assert_eq!(prev.desc.latest().arity(), table1.desc.latest().arity());
                assert_eq!(new.desc.latest().arity(), table2.desc.latest().arity());
            }
            _ => panic!("Expected Altered state"),
        },
        _ => panic!("Expected Table command"),
    }
}

#[mz_ore::test]
#[should_panic(expected = "Cannot add an already added object")]
fn test_invalid_double_add() {
    let table = create_test_table("table");
    let mut cmd = CatalogImplication::None;

    // First addition
    cmd.absorb_table(
        table.clone(),
        Some("schema.table".to_string()),
        StateDiff::Addition,
    );

    // Second addition should panic
    cmd.absorb_table(
        table.clone(),
        Some("schema.table".to_string()),
        StateDiff::Addition,
    );
}

#[mz_ore::test]
#[should_panic(expected = "Cannot drop an already dropped object")]
fn test_invalid_double_drop() {
    let table = create_test_table("table");
    let mut cmd = CatalogImplication::None;

    // First drop
    cmd.absorb_table(
        table.clone(),
        Some("schema.table".to_string()),
        StateDiff::Retraction,
    );

    // Second drop should panic
    cmd.absorb_table(
        table.clone(),
        Some("schema.table".to_string()),
        StateDiff::Retraction,
    );
}

#[mz_ore::test]
fn selection_changes_include_unselects_and_filter_the_build() {
    use crate::durable::objects::WrittenPlan;

    let selected = GlobalId::User(1);
    let removed = GlobalId::User(2);
    let foreign = GlobalId::User(3);
    let updates = [
        (selected, "local", StateDiff::Retraction),
        (selected, "local", StateDiff::Addition),
        (removed, "local", StateDiff::Retraction),
        (foreign, "foreign", StateDiff::Addition),
    ]
    .into_iter()
    .map(|(id, build, diff)| ParsedStateUpdate {
        kind: ParsedStateUpdateKind::WrittenPlan(WrittenPlan {
            id,
            build_version: build.into(),
            revision: uuid::Uuid::new_v4(),
        }),
        ts: Timestamp::MIN,
        diff,
    })
    .collect();
    let effects = CatalogImplications::from_updates(updates, "local");
    assert_eq!(effects.written_plans, BTreeSet::from([selected, removed]));
}

#[mz_ore::test]
fn permission_retractions_do_not_drop_collections() {
    use crate::durable::objects::CollectionCompactionBound;

    let surviving = GlobalId::User(1);
    let retracted = GlobalId::User(2);
    let mut updates: Vec<_> = [
        (surviving, 7, StateDiff::Retraction),
        (surviving, 8, StateDiff::Addition),
        (retracted, 9, StateDiff::Retraction),
    ]
    .into_iter()
    .map(|(id, time, diff)| ParsedStateUpdate {
        kind: ParsedStateUpdateKind::CollectionCompactionBound(CollectionCompactionBound {
            id,
            frontier: Some(Timestamp::new(time)),
        }),
        ts: Timestamp::MIN,
        diff,
    })
    .collect();
    updates.push(ParsedStateUpdate {
        kind: ParsedStateUpdateKind::StorageCollectionMetadata { id: retracted },
        ts: Timestamp::MIN,
        diff: StateDiff::Retraction,
    });
    let effects = CatalogImplications::from_updates(updates, "local");
    assert_eq!(
        effects.compaction_bounds,
        BTreeMap::from([(surviving, Antichain::from_elem(Timestamp::new(8)))])
    );
    assert_eq!(
        effects.retired_storage_metadata,
        BTreeSet::from([retracted])
    );
    assert!(effects.items.is_empty());
}
