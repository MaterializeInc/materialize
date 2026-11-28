// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persistent table definitions are resolved against the commit candidate.

use std::sync::Arc;

use mz_catalog::memory::objects::{CatalogItem, Table};
use mz_repr::{CatalogItemId, GlobalId};
use mz_sql::ast::{Raw, Statement};
use mz_sql::names::ResolvedIds;
use mz_sql::plan::{CreateTablePlan, Params, Plan, PlanNotice};
use mz_sql::session::metadata::SessionMetadata;

use crate::AdapterError;
use crate::catalog::{CatalogState, Op};
use crate::session::SessionMeta;

use super::CommandExecution;

#[derive(Clone, Debug)]
pub struct TableStatement {
    statement: Arc<Statement<Raw>>,
    params: Params,
    session: Arc<SessionMeta>,
    create_id: Option<(CatalogItemId, GlobalId)>,
    notices: Vec<PlanNotice>,
    skipped_name: Option<String>,
}

pub(crate) struct TableCompilation {
    pub plan: CreateTablePlan,
    resolved_ids: ResolvedIds,
    notices: Vec<PlanNotice>,
}

impl TableStatement {
    pub(crate) fn new(
        statement: Arc<Statement<Raw>>,
        params: Params,
        session: SessionMeta,
    ) -> Self {
        Self {
            statement,
            params,
            session: Arc::new(session),
            create_id: None,
            notices: Vec::new(),
            skipped_name: None,
        }
    }

    pub(crate) fn allocate_create(&mut self, id: (CatalogItemId, GlobalId)) {
        assert!(
            self.create_id.is_none(),
            "table IDs must not change across retries"
        );
        self.create_id = Some(id);
    }

    pub(crate) fn compile_create(
        &self,
        state: &CatalogState,
    ) -> Result<TableCompilation, AdapterError> {
        let (plan, resolved_ids, notices) =
            super::plan_command(state, &self.statement, &self.params, &self.session)?;
        let Plan::CreateTable(plan) = plan else {
            return Err(AdapterError::Unsupported("non-CREATE TABLE intent"));
        };
        if plan.table.temporary {
            return Err(AdapterError::Unsupported("temporary table command"));
        }
        Ok(TableCompilation {
            plan,
            resolved_ids,
            notices,
        })
    }

    pub(super) fn compile(
        &mut self,
        state: &CatalogState,
    ) -> Result<CommandExecution<'_>, AdapterError> {
        let compiled = self.compile_create(state)?;
        let CreateTablePlan {
            name,
            table,
            if_not_exists,
        } = compiled.plan;
        let (id, global_id) = self.create_id.expect("table IDs allocated at admission");
        let table = Table::from_plan(
            table,
            global_id,
            self.session.conn_id().clone(),
            compiled.resolved_ids,
        );
        let ops = vec![Op::CreateItem {
            id,
            name: name.clone(),
            item: CatalogItem::Table(table),
            owner_id: *self.session.current_role_id(),
        }];
        state.validate_resource_limits(&ops, self.session.conn_id())?;
        self.notices = compiled.notices;
        Ok(CommandExecution::create_item(
            ops,
            id,
            name.item,
            if_not_exists,
            &mut self.skipped_name,
        ))
    }

    pub(crate) fn notices(&self) -> &[PlanNotice] {
        &self.notices
    }

    pub(crate) fn skipped_name(&self) -> Option<&str> {
        self.skipped_name.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::catalog::Catalog;
    use crate::catalog::command::CatalogCommand;
    use crate::session::Session;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn table_command_resolves_current_schema_and_collision() {
        Catalog::with_debug(|mut catalog| async move {
            let mut session = Session::dummy();
            session.initialize_role_metadata(mz_sql::session::user::MZ_SYSTEM_ROLE_ID);
            session
                .start_transaction(mz_ore::now::to_datetime(1000), None, None)
                .unwrap();
            let raw = mz_sql_parser::parser::parse_statements(
                "CREATE TABLE IF NOT EXISTS materialize.public.replayed_table (a int DEFAULT 7)",
            )
            .unwrap()
            .remove(0)
            .ast;
            let mut command = TableStatement::new(Arc::new(raw), Params::empty(), session.meta());
            let original = command.compile_create(catalog.state()).unwrap().plan.name;
            let ts = catalog.current_upper().await;
            let mut allocated = catalog.allocate_user_ids(2, ts).await.unwrap();
            let contender_ids = allocated.pop().unwrap();
            let ids = allocated.pop().unwrap();
            let mut contender = command.clone();
            contender.allocate_create(contender_ids);
            command.allocate_create(ids);

            let ts = catalog.current_upper().await;
            catalog
                .transact(
                    None,
                    ts,
                    None,
                    vec![
                        Op::RenameSchema {
                            database_spec: original.qualifiers.database_spec,
                            schema_spec: original.qualifiers.schema_spec,
                            new_name: "prior_public".into(),
                            check_reserved_names: true,
                        },
                        Op::CreateSchema {
                            database_id: original.qualifiers.database_spec,
                            schema_name: "public".into(),
                            owner_id: *session.current_role_id(),
                        },
                    ],
                )
                .await
                .unwrap();

            let mut commands = [CatalogCommand::CreateTable(Arc::new(command))];
            let ts = catalog.current_upper().await;
            let created = catalog
                .transact_commands(None, ts, None, &mut commands, &mut BTreeMap::new())
                .await
                .unwrap();
            assert!(!created.catalog_updates.is_empty());
            let entry = catalog.get_entry(&ids.0);
            assert_ne!(
                entry.name().qualifiers.schema_spec,
                original.qualifiers.schema_spec
            );
            assert_eq!(
                catalog.resolve_full_name(entry.name(), None).schema,
                "public"
            );
            let mut limited = catalog.state().clone();
            limited
                .system_config_mut()
                .set("max_tables", mz_sql::session::vars::VarInput::Flat("0"))
                .unwrap();
            assert!(
                matches!(
                    contender.compile(&limited),
                    Err(AdapterError::ResourceExhaustion { .. })
                ),
                "resource checks must precede IF NOT EXISTS collision handling"
            );
            let ts = catalog.current_upper().await;
            let mut commands = [CatalogCommand::CreateTable(Arc::new(contender))];
            let skipped = catalog
                .transact_commands(None, ts, None, &mut commands, &mut BTreeMap::new())
                .await
                .unwrap();
            assert!(
                skipped.catalog_updates.is_empty(),
                "IF NOT EXISTS must not publish a new object"
            );
            assert!(catalog.try_get_entry(&contender_ids.0).is_none());
            let [CatalogCommand::CreateTable(command)] = commands else {
                unreachable!("table command")
            };
            assert_eq!(command.skipped_name(), Some("replayed_table"));
            catalog.expire().await;
        })
        .await;
    }
}
