// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Database and schema creation resolved against the commit candidate.

use std::sync::Arc;

use mz_sql::ast::{Raw, Statement};
use mz_sql::plan::{Params, Plan, PlanNotice};
use mz_sql::session::metadata::SessionMetadata;

use crate::catalog::{CatalogState, Op};
use crate::session::SessionMeta;
use crate::{AdapterError, AdapterNotice};

use super::{CommandExecution, ExecutionOutcome};

#[derive(Clone, Copy, Debug)]
pub(super) enum NamespaceKind {
    Database,
    Schema,
}

#[derive(Clone, Debug)]
pub struct NamespaceStatement {
    statement: Arc<Statement<Raw>>,
    params: Params,
    session: Arc<SessionMeta>,
    notices: Vec<PlanNotice>,
    skipped_notice: Option<AdapterNotice>,
}

impl NamespaceStatement {
    pub(crate) fn new(
        statement: Arc<Statement<Raw>>,
        params: Params,
        session: SessionMeta,
    ) -> Self {
        Self {
            statement,
            params,
            session: Arc::new(session),
            notices: Vec::new(),
            skipped_notice: None,
        }
    }

    pub(crate) fn plan(
        &self,
        state: &CatalogState,
    ) -> Result<(Plan, Vec<PlanNotice>), AdapterError> {
        let (plan, _, notices) =
            super::plan_command(state, &self.statement, &self.params, &self.session)?;
        match plan {
            Plan::CreateDatabase(_) | Plan::CreateSchema(_) => Ok((plan, notices)),
            _ => Err(AdapterError::Unsupported("non-namespace creation intent")),
        }
    }

    pub(super) fn compile(
        &mut self,
        state: &CatalogState,
    ) -> Result<CommandExecution<'_>, AdapterError> {
        let (plan, notices) = self.plan(state)?;
        let owner_id = *self.session.current_role_id();
        let (op, kind, name, if_not_exists) = match plan {
            Plan::CreateDatabase(plan) => (
                Op::CreateDatabase {
                    name: plan.name.clone(),
                    owner_id,
                },
                NamespaceKind::Database,
                plan.name,
                plan.if_not_exists,
            ),
            Plan::CreateSchema(plan) => (
                Op::CreateSchema {
                    database_id: plan.database_spec,
                    schema_name: plan.schema_name.clone(),
                    owner_id,
                },
                NamespaceKind::Schema,
                plan.schema_name,
                plan.if_not_exists,
            ),
            _ => unreachable!("namespace plan validated"),
        };
        let ops = vec![op];
        state.validate_resource_limits(&ops, self.session.conn_id())?;
        self.notices = notices;
        self.skipped_notice = None;
        Ok(CommandExecution {
            ops,
            outcome: if_not_exists.then_some(ExecutionOutcome::CreateNamespace {
                kind,
                name,
                skipped_notice: &mut self.skipped_notice,
            }),
        })
    }

    pub(crate) fn notices(&self) -> &[PlanNotice] {
        &self.notices
    }

    pub(crate) fn skipped_notice(&self) -> Option<AdapterNotice> {
        self.skipped_notice.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mz_sql::session::vars::VarInput;

    use super::*;
    use crate::catalog::Catalog;
    use crate::catalog::command::CatalogCommand;
    use crate::session::Session;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn namespace_commands_see_prefix_and_preserve_atomicity() {
        Catalog::with_debug(|mut catalog| async move {
            let mut session = Session::dummy();
            session.initialize_role_metadata(mz_sql::session::user::MZ_SYSTEM_ROLE_ID);
            session.start_transaction(mz_ore::now::to_datetime(1000), None, None).expect("start transaction");
            let command = |sql: &str| {
                let statement = mz_sql_parser::parser::parse_statements(sql).expect("parse namespace statement").remove(0).ast;
                let command = NamespaceStatement::new(
                    Arc::new(statement), Params::empty(), session.meta(),
                );
                CatalogCommand::CreateNamespace(Arc::new(command))
            };
            let mut commands = [
                command("CREATE DATABASE replay_database"),
                command("CREATE SCHEMA replay_database.replay_schema"),
                command("CREATE SCHEMA IF NOT EXISTS replay_database.replay_schema"),
                command("CREATE DATABASE IF NOT EXISTS replay_database"),
            ];
            let ts = catalog.current_upper().await;
            let result = catalog.transact_commands(None, ts, None, &mut commands, &mut BTreeMap::new()).await.expect("schema must resolve the database created by the prefix");
            assert!(!result.catalog_updates.is_empty());
            let CatalogCommand::CreateNamespace(schema) = &commands[2] else { unreachable!("namespace command") };
            assert!(matches!(schema.skipped_notice(), Some(AdapterNotice::SchemaAlreadyExists { name }) if name == "replay_schema"));
            let CatalogCommand::CreateNamespace(database) = &commands[3] else { unreachable!("namespace command") };
            assert!(matches!(database.skipped_notice(), Some(AdapterNotice::DatabaseAlreadyExists { name }) if name == "replay_database"));

            let mut limited = catalog.state().clone();
            limited.system_config_mut().set("max_schemas_per_database", VarInput::Flat("0")).expect("set schema quota");
            let mut schema = (**schema).clone();
            assert!(matches!(schema.compile(&limited), Err(AdapterError::ResourceExhaustion { .. })), "quota must precede collision suppression");

            let mut commands = [
                command("CREATE DATABASE aborted_database"),
                command("CREATE SCHEMA replay_database.replay_schema"),
            ];
            let ts = catalog.current_upper().await;
            let result = catalog
                .transact_commands(None, ts, None, &mut commands, &mut BTreeMap::new())
                .await;
            assert!(result.is_err());
            assert!(catalog.state().resolve_database("aborted_database").is_err(), "failed schema creation must roll back the prefix");
            catalog.expire().await;
        }).await;
    }
}
