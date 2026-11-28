// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

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
    Catalog::with_debug(|mut catalog| async move {
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
        let mut commands = [
            CatalogCommand::DropObjects(Arc::new(DropStatement::new(
                statement("DROP TYPE element_type"),
                Params::empty(),
                session.meta(),
            ))),
            CatalogCommand::Metadata(Arc::new(replacement)),
            CatalogCommand::Metadata(Arc::new(pending)),
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

        catalog.expire().await;
    })
    .await;
}
