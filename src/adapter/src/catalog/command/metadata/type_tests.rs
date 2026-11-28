// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use crate::coord::catalog_implications::parsed_state_updates::ParsedStateUpdateKind;
use mz_catalog::memory::objects::StateDiff;

use super::*;
use crate::catalog::Catalog;
use crate::catalog::command::{CatalogCommand, drop::DropStatement};
use crate::session::Session;

fn statement(sql: &str) -> Arc<Statement<Raw>> {
    Arc::new(
        mz_sql_parser::parser::parse_statements(sql)
            .expect("parse type fixture")
            .remove(0)
            .ast,
    )
}

async fn create(
    catalog: &Catalog,
    session: &Session,
    sql: &str,
) -> (MetadataStatement, (CatalogItemId, GlobalId)) {
    let ts = catalog.current_upper().await;
    let ids = catalog
        .allocate_user_ids(1, ts)
        .await
        .expect("allocate type IDs")
        .remove(0);
    let mut command = MetadataStatement::new(statement(sql), Params::empty(), session.meta());
    command.allocate_type(ids);
    (command, ids)
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn type_creation_resolves_candidate_dependencies_and_rolls_back() {
    let persist = mz_persist_client::PersistClient::new_for_tests().await;
    let organization_id = uuid::Uuid::new_v4();
    let bootstrap_args = mz_catalog::durable::test_bootstrap_args();
    let mut catalog =
        Catalog::open_debug_catalog(persist.clone(), organization_id, &bootstrap_args)
            .await
            .expect("open type catalog");
    let mut session = Session::dummy();
    session.initialize_role_metadata(mz_sql::session::user::MZ_SYSTEM_ROLE_ID);
    session
        .start_transaction(mz_ore::now::to_datetime(1000), None, None)
        .expect("start transaction");
    let (original, (original_id, _)) = create(
        &catalog,
        &session,
        "CREATE TYPE element_type AS LIST (ELEMENT TYPE = int4)",
    )
    .await;
    let ts = catalog.current_upper().await;
    catalog
        .transact_commands(
            None,
            None,
            ts,
            None,
            &mut [CatalogCommand::Metadata(Arc::new(original))],
            &mut BTreeMap::new(),
        )
        .await
        .expect("create original type");

    let (mut pending, (outer_id, outer_global_id)) = create(
        &catalog,
        &session,
        "CREATE TYPE outer_type AS (a element_type)",
    )
    .await;
    pending
        .compile(catalog.state())
        .expect("initial dependency resolves");
    let (replacement, (replacement_id, _)) = create(
        &catalog,
        &session,
        "CREATE TYPE element_type AS LIST (ELEMENT TYPE = text)",
    )
    .await;
    let (leaf, (leaf_id, _)) = create(
        &catalog,
        &session,
        "CREATE TYPE leaf_type AS MAP (KEY TYPE = text, VALUE TYPE = outer_type)",
    )
    .await;
    // The dependency chain crosses ID order in both directions.
    assert!(outer_id < replacement_id && replacement_id < leaf_id);
    let mut commands = [
        CatalogCommand::DropObjects(Arc::new(DropStatement::new(
            statement("DROP TYPE element_type"),
            Params::empty(),
            session.meta(),
        ))),
        CatalogCommand::Metadata(Arc::new(replacement)),
        CatalogCommand::Metadata(Arc::new(pending)),
        CatalogCommand::Metadata(Arc::new(leaf)),
    ];
    let ts = catalog.current_upper().await;
    catalog
        .transact_commands(None, None, ts, None, &mut commands, &mut BTreeMap::new())
        .await
        .expect("resolve replacement type from command prefix");
    assert!(catalog.try_get_entry(&original_id).is_none());
    assert_ne!(original_id, replacement_id);
    let outer = catalog.get_entry(&outer_id);
    assert!(outer.uses().contains(&replacement_id));
    assert!(!outer.uses().contains(&original_id));
    assert_eq!(outer.latest_global_id(), outer_global_id);
    assert!(catalog.get_entry(&leaf_id).uses().contains(&outer_id));

    catalog.expire().await;
    catalog = Catalog::open_debug_catalog(persist, organization_id, &bootstrap_args)
        .await
        .expect("reopen types independent of ID order");
    assert!(
        catalog
            .get_entry(&outer_id)
            .uses()
            .contains(&replacement_id)
    );
    assert_eq!(
        catalog.get_entry(&outer_id).latest_global_id(),
        outer_global_id
    );
    assert!(catalog.get_entry(&leaf_id).uses().contains(&outer_id));

    let (aborted, (aborted_id, _)) = create(
        &catalog,
        &session,
        "CREATE TYPE aborted_type AS LIST (ELEMENT TYPE = int4)",
    )
    .await;
    let (missing, _) = create(
        &catalog,
        &session,
        "CREATE TYPE missing_dependency AS LIST (ELEMENT TYPE = absent_type)",
    )
    .await;
    let revision = catalog.transient_revision();
    let ts = catalog.current_upper().await;
    let mut commands = [
        CatalogCommand::Metadata(Arc::new(aborted)),
        CatalogCommand::Metadata(Arc::new(missing)),
    ];
    assert!(
        catalog
            .transact_commands(None, None, ts, None, &mut commands, &mut BTreeMap::new())
            .await
            .is_err()
    );
    assert!(catalog.try_get_entry(&aborted_id).is_none());
    assert_eq!(catalog.transient_revision(), revision);

    let ts = catalog.current_upper().await;
    let drop = DropStatement::new(
        statement("DROP TYPE element_type CASCADE"),
        Params::empty(),
        session.meta(),
    );
    let result = catalog
        .transact_commands(
            None,
            None,
            ts,
            None,
            &mut [CatalogCommand::DropObjects(Arc::new(drop))],
            &mut BTreeMap::new(),
        )
        .await
        .expect("retract types in reverse dependency order");
    let retractions: Vec<_> = result
        .catalog_updates
        .iter()
        .filter_map(|update| match &update.kind {
            ParsedStateUpdateKind::Item { durable_item, .. }
                if update.diff == StateDiff::Retraction
                    && [replacement_id, outer_id, leaf_id].contains(&durable_item.id) =>
            {
                Some(durable_item.id)
            }
            _ => None,
        })
        .collect();
    assert_eq!(retractions, vec![leaf_id, outer_id, replacement_id]);
    for id in [replacement_id, outer_id, leaf_id] {
        assert!(catalog.try_get_entry(&id).is_none());
    }
    catalog.expire().await;
}
