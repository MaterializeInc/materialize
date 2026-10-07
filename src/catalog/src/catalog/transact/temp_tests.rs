// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_repr::{CatalogItemId, GlobalId};
use mz_sql::catalog::CatalogError as SqlCatalogError;
use mz_sql::names::{
    ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier, SchemaSpecifier,
};
use mz_sql::session::user::{MZ_SYSTEM_ROLE_ID, SYSTEM_USER};
use uuid::Uuid;

use super::{DropObjectInfo, Op};
use crate::SYSTEM_CONN_ID;
use crate::catalog::test_support::parse_item;
use crate::catalog::transaction_context::TransactionContext;
use crate::catalog::{Catalog, CatalogError, CatalogState};
use crate::memory::error::{Error, ErrorKind};
use crate::memory::objects::CatalogItem;

fn create_view(state: &mut CatalogState, id: u64, name: &str, input: &str) -> Op {
    let sql = format!("CREATE TEMPORARY VIEW mz_temp.{name} AS SELECT * FROM mz_catalog.{input}");
    let CatalogItem::View(mut view) =
        parse_item(state, GlobalId::User(id), &sql, &BTreeMap::new()).expect("parse fixture view")
    else {
        panic!("fixture SQL must produce a view");
    };
    // Reconstruction parses as the system session and leaves ownership unset.
    // The transaction supplies the registered session's durable identity.
    view.conn_id = Some(SYSTEM_CONN_ID.clone());
    Op::CreateItem {
        id: CatalogItemId::User(id),
        name: QualifiedItemName {
            qualifiers: ItemQualifiers {
                database_spec: ResolvedDatabaseSpecifier::Ambient,
                schema_spec: SchemaSpecifier::Temporary,
            },
            item: name.into(),
        },
        item: CatalogItem::View(view),
        owner_id: MZ_SYSTEM_ROLE_ID,
    }
}

fn namespace(state: &CatalogState) -> BTreeMap<String, CatalogItemId> {
    state
        .get_schema(
            &ResolvedDatabaseSpecifier::Ambient,
            &SchemaSpecifier::Temporary,
            &SYSTEM_CONN_ID,
        )
        .items
        .clone()
}

fn assert_replacement(state: &CatalogState, original: &Op, replacement: &Op) {
    let Op::CreateItem {
        id: old_id,
        item: old_item,
        ..
    } = original
    else {
        unreachable!();
    };
    let Op::CreateItem { id, name, item, .. } = replacement else {
        unreachable!();
    };
    assert_eq!(namespace(state).get(&name.item), Some(id));
    assert!(state.try_get_entry(old_id).is_none());
    let entry = state.get_entry(id);
    assert_eq!(entry.conn_id(), Some(&SYSTEM_CONN_ID));
    assert_eq!(entry.uses(), item.uses());
    assert_eq!(entry.references(), item.references());
    for dependency in old_item.uses().union(&item.uses()) {
        let input = state.get_entry(dependency);
        assert!(!input.used_by().contains(old_id));
        assert!(!input.referenced_by().contains(old_id));
        assert_eq!(
            input.used_by().contains(id),
            item.uses().contains(dependency)
        );
        assert_eq!(
            input.referenced_by().contains(id),
            item.references().items().any(|id| id == dependency)
        );
    }
}

#[mz_ore::test(tokio::test)]
async fn temporary_view_replacement_dry_run() {
    Catalog::with_debug(|mut catalog| async move {
        let session = TransactionContext {
            user: &SYSTEM_USER,
            conn_id: &SYSTEM_CONN_ID,
            uuid: Uuid::new_v4(),
            authenticated_role_id: &MZ_SYSTEM_ROLE_ID,
        };
        catalog.register_temporary_namespace(session.conn_id, session.uuid);

        // Both cases use the same operations and assertions. Only the location
        // of the original differs: committed catalog or accumulated dry run.
        for (offset, committed, name) in
            [(0, true, "committed_temp"), (10, false, "provisional_temp")]
        {
            let old_id = CatalogItemId::User(1_000_000 + offset);
            let new_id = CatalogItemId::User(1_000_001 + offset);
            let mut fixture = catalog.state().clone();
            let original = create_view(&mut fixture, 1_000_000 + offset, name, "mz_tables");
            let replacement = create_view(&mut fixture, 1_000_001 + offset, name, "mz_columns");
            let (base, snapshot) = if committed {
                catalog
                    .transact(None, 1.into(), Some(&session), vec![original.clone()])
                    .await
                    .expect("commit original temporary view");
                (catalog.state().clone(), None)
            } else {
                let (base, snapshot) = catalog
                    .transact_incremental_dry_run(
                        catalog.state(),
                        vec![original.clone()],
                        Some(&session),
                        None,
                        1.into(),
                    )
                    .await
                    .expect("create provisional temporary view");
                assert!(catalog.state().try_get_entry(&old_id).is_none());
                assert!(!namespace(catalog.state()).contains_key(name));
                (base, Some(snapshot))
            };
            let committed_namespace = namespace(catalog.state());

            let ops = vec![
                Op::DropObjects(vec![DropObjectInfo::Item(old_id)]),
                replacement.clone(),
            ];
            let (candidate, _) = catalog
                .transact_incremental_dry_run(
                    &base,
                    ops.clone(),
                    Some(&session),
                    snapshot.clone(),
                    1.into(),
                )
                .await
                .expect("drop-plus-create must replace the temporary view in a dry run");
            assert_replacement(&candidate, &original, &replacement);
            assert_eq!(namespace(&base).get(name), Some(&old_id));
            assert_eq!(namespace(catalog.state()), committed_namespace);
            assert!(catalog.state().try_get_entry(&new_id).is_none());
            if committed {
                assert_eq!(
                    catalog.state().get_entry(&old_id).uses(),
                    base.get_entry(&old_id).uses()
                );
            }

            // Omitting the drop is a collision even when the original exists
            // only in the accumulated state, not in the committed catalog.
            let duplicate = catalog
                .transact_incremental_dry_run(
                    &base,
                    vec![replacement.clone()],
                    Some(&session),
                    snapshot,
                    1.into(),
                )
                .await;
            assert!(matches!(
                duplicate,
                Err(CatalogError::Catalog(Error {
                    kind: ErrorKind::Sql(SqlCatalogError::ItemAlreadyExists(id, ref item_name)),
                })) if id == new_id && item_name == name
            ));

            // Replay the same statement boundaries through the commit path.
            if !committed {
                catalog
                    .transact(None, 1.into(), Some(&session), vec![original.clone()])
                    .await
                    .expect("commit provisional creation");
            }
            catalog
                .transact(None, 1.into(), Some(&session), ops)
                .await
                .expect("commit temporary replacement");
            assert_eq!(namespace(catalog.state()), namespace(&candidate));
            assert_replacement(catalog.state(), &original, &replacement);
        }
    })
    .await;
}
