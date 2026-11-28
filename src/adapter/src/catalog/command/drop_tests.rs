// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use mz_repr::CatalogItemId;
use mz_sql::ast::{Raw, Statement};
use mz_sql::names::ObjectId;
use mz_sql::plan::Params;

use super::CatalogCommand;
use super::drop::DropStatement;
use super::table::TableStatement;
use crate::catalog::{Catalog, CommandExecutionError, Op};
use crate::session::Session;

fn session() -> Session {
    let mut session = Session::dummy();
    session.initialize_role_metadata(mz_sql::session::user::MZ_SYSTEM_ROLE_ID);
    session
        .start_transaction(mz_ore::now::to_datetime(1000), None, None)
        .expect("start invoking transaction");
    session
}

fn statement(sql: &str) -> Arc<Statement<Raw>> {
    Arc::new(
        mz_sql_parser::parser::parse_statements(sql)
            .expect("parse test statement")
            .remove(0)
            .ast,
    )
}

fn drop_statement(session: &Session, name: &str) -> DropStatement {
    DropStatement::new(
        statement(&format!("DROP TABLE materialize.public.{name}")),
        Params::empty(),
        session.meta(),
    )
}

async fn table_command(
    catalog: &mut Catalog,
    session: &Session,
    name: &str,
) -> (CatalogCommand, CatalogItemId) {
    let ts = catalog.current_upper().await;
    let ids = catalog
        .allocate_user_ids(1, ts)
        .await
        .expect("allocate table IDs")
        .remove(0);
    let mut command = TableStatement::new(
        statement(&format!("CREATE TABLE materialize.public.{name} (a int)")),
        Params::empty(),
        session.meta(),
    );
    command.allocate_create(ids);
    (CatalogCommand::CreateTable(Arc::new(command)), ids.0)
}

async fn commit(catalog: &mut Catalog, commands: &mut [CatalogCommand]) {
    let ts = catalog.current_upper().await;
    catalog
        .transact_commands(None, None, ts, None, commands, &mut BTreeMap::new())
        .await
        .expect("commit test commands");
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn drop_sees_table_created_in_command_prefix() {
    Catalog::with_debug(|mut catalog| async move {
        let session = session();
        let (create, id) = table_command(&mut catalog, &session, "prefix_table").await;
        let mut drop = drop_statement(&session, "prefix_table");
        assert!(
            drop.plan(catalog.state()).is_err(),
            "the table must not exist in the initial catalog"
        );

        commit(
            &mut catalog,
            &mut [create, CatalogCommand::DropRelation(Arc::new(drop))],
        )
        .await;

        assert!(catalog.try_get_entry(&id).is_none());
        assert!(
            drop_statement(&session, "prefix_table")
                .plan(catalog.state())
                .is_err()
        );
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn failing_drop_rolls_back_prior_creates_and_drops() {
    Catalog::with_debug(|mut catalog| async move {
        let session = session();
        let (create, original_id) = table_command(&mut catalog, &session, "original_table").await;
        commit(&mut catalog, &mut [create]).await;
        let (create, new_id) = table_command(&mut catalog, &session, "new_table").await;
        let revision = catalog.transient_revision();
        let mut commands = [
            create,
            CatalogCommand::DropRelation(Arc::new(drop_statement(&session, "original_table"))),
            CatalogCommand::DropRelation(Arc::new(drop_statement(&session, "missing_table"))),
        ];
        let ts = catalog.current_upper().await;
        let result = catalog
            .transact_commands(None, None, ts, None, &mut commands, &mut BTreeMap::new())
            .await;
        let Err(CommandExecutionError::Error(error)) = result else {
            panic!("dropping a missing table must fail without requesting preparation");
        };
        assert!(
            error.to_string().contains("missing_table"),
            "the final DROP must cause the failure, got: {error}"
        );
        assert_eq!(catalog.transient_revision(), revision);
        assert!(catalog.try_get_entry(&new_id).is_none());
        assert!(catalog.try_get_entry(&original_id).is_some());
        let plan = drop_statement(&session, "original_table")
            .plan(catalog.state())
            .expect("rolled-back drop preserves original binding");
        assert_eq!(plan.drop_ids, vec![ObjectId::Item(original_id)]);
        assert!(
            drop_statement(&session, "new_table")
                .plan(catalog.state())
                .is_err()
        );
        catalog.expire().await;
    })
    .await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn drop_replans_reused_name_and_preserves_renamed_original() {
    Catalog::with_debug(|mut catalog| async move {
        let session = session();
        let (create, original_id) = table_command(&mut catalog, &session, "reused_table").await;
        commit(&mut catalog, &mut [create]).await;
        let mut drop = drop_statement(&session, "reused_table");
        let plan = drop.plan(catalog.state()).expect("plan original drop");
        assert_eq!(plan.drop_ids, vec![ObjectId::Item(original_id)]);

        let current_full_name =
            catalog.resolve_full_name(catalog.get_entry(&original_id).name(), None);
        let ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                ts,
                None,
                vec![Op::RenameItem {
                    id: original_id,
                    current_full_name,
                    to_name: "renamed_table".into(),
                }],
            )
            .await
            .expect("rename original table");
        let (replacement, replacement_id) =
            table_command(&mut catalog, &session, "reused_table").await;
        commit(&mut catalog, &mut [replacement]).await;
        assert_ne!(original_id, replacement_id);

        commit(
            &mut catalog,
            &mut [CatalogCommand::DropRelation(Arc::new(drop))],
        )
        .await;

        assert!(catalog.try_get_entry(&replacement_id).is_none());
        assert_eq!(catalog.get_entry(&original_id).name().item, "renamed_table");
        let plan = drop_statement(&session, "renamed_table")
            .plan(catalog.state())
            .expect("renamed original remains addressable");
        assert_eq!(plan.drop_ids, vec![ObjectId::Item(original_id)]);
        assert!(
            drop_statement(&session, "reused_table")
                .plan(catalog.state())
                .is_err()
        );
        catalog.expire().await;
    })
    .await;
}
