// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Catalog-only statements that need no external preparation or retained physical plan.

use std::sync::Arc;

use mz_adapter_types::dyncfgs::ENABLE_PASSWORD_AUTH;
use mz_catalog::memory::objects::Type;
use mz_repr::{CatalogItemId, GlobalId};
use mz_sql::ast::{Raw, Statement};
use mz_sql::catalog::{CatalogTypeDetails, RoleAttributesRaw};
use mz_sql::names::ResolvedIds;
use mz_sql::plan::{NetworkPolicyRule, Params, Plan, PlanNotice};
use mz_sql::session::metadata::SessionMetadata;

use crate::AdapterError;
use crate::catalog::{CatalogItem, CatalogState, Op};
use crate::session::SessionMeta;

use super::CommandExecution;

#[derive(Clone, Debug)]
pub struct MetadataStatement {
    statement: Arc<Statement<Raw>>,
    params: Params,
    session: Arc<SessionMeta>,
    notices: Vec<PlanNotice>,
    type_ids: Option<(CatalogItemId, GlobalId)>,
}

impl MetadataStatement {
    pub(crate) fn supports_statement(statement: &Statement<Raw>) -> bool {
        matches!(
            statement,
            Statement::CreateRole(_)
                | Statement::Comment(_)
                | Statement::CreateType(_)
                | Statement::CreateQueryPolicy(_)
                | Statement::AlterQueryPolicy(_)
                | Statement::CreateNetworkPolicy(_)
                | Statement::AlterNetworkPolicy(_)
        )
    }

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
            type_ids: None,
        }
    }

    pub(crate) fn plan(
        &self,
        state: &CatalogState,
    ) -> Result<(Plan, ResolvedIds, Vec<PlanNotice>), AdapterError> {
        let (plan, resolved_ids, notices) =
            super::plan_command(state, &self.statement, &self.params, &self.session)?;
        match plan {
            Plan::CreateRole(_)
            | Plan::Comment(_)
            | Plan::CreateType(_)
            | Plan::CreateQueryPolicy(_)
            | Plan::AlterQueryPolicy(_)
            | Plan::CreateNetworkPolicy(_)
            | Plan::AlterNetworkPolicy(_) => Ok((plan, resolved_ids, notices)),
            _ => Err(AdapterError::Unsupported("non-metadata statement intent")),
        }
    }

    pub(super) fn compile(
        &mut self,
        state: &CatalogState,
    ) -> Result<CommandExecution<'_>, AdapterError> {
        let (plan, resolved_ids, notices) = self.plan(state)?;
        let op = match plan {
            Plan::CreateRole(plan) => {
                validate_role_attributes(state, &plan.attributes)?;
                Op::CreateRole {
                    name: plan.name,
                    attributes: plan.attributes,
                }
            }
            Plan::CreateType(plan) => {
                let (id, global_id) = self.type_ids.expect("type IDs allocated before commit");
                let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
                let catalog = state.for_session_metadata(&*self.session, tx);
                plan.typ.inner.desc(&catalog)?;
                Op::CreateItem {
                    id,
                    name: plan.name,
                    item: CatalogItem::Type(Type {
                        create_sql: Some(plan.typ.create_sql),
                        global_id,
                        details: CatalogTypeDetails {
                            array_id: None,
                            typ: plan.typ.inner,
                            pg_metadata: None,
                        },
                        resolved_ids,
                    }),
                    owner_id: *self.session.current_role_id(),
                }
            }
            Plan::Comment(plan) => Op::Comment {
                object_id: plan.object_id,
                sub_component: plan.sub_component,
                comment: plan.comment,
            },
            Plan::CreateQueryPolicy(plan) => Op::CreateQueryPolicy {
                name: plan.name,
                mode: plan.mode,
                rules: plan.rules,
                owner_id: *self.session.current_role_id(),
            },
            Plan::AlterQueryPolicy(plan) => Op::AlterQueryPolicy {
                id: plan.id,
                mode: plan.mode,
                rules: plan.rules,
            },
            Plan::CreateNetworkPolicy(plan) => Op::CreateNetworkPolicy {
                name: plan.name,
                rules: plan.rules,
                owner_id: *self.session.current_role_id(),
            },
            Plan::AlterNetworkPolicy(plan) => {
                if state.system_config().default_network_policy_name() == plan.name {
                    validate_network_policy_change(&self.session, &plan.rules)?;
                }
                Op::AlterNetworkPolicy {
                    id: plan.id,
                    name: plan.name,
                    rules: plan.rules,
                    owner_id: *self.session.current_role_id(),
                }
            }
            _ => unreachable!("metadata plan validated"),
        };
        let ops = vec![op];
        state.validate_resource_limits(&ops, self.session.conn_id())?;
        self.notices = notices;
        Ok(CommandExecution::raw(ops))
    }

    pub(crate) fn allocate_type(&mut self, ids: (CatalogItemId, GlobalId)) {
        assert!(matches!(&*self.statement, Statement::CreateType(_)));
        let previous = self.type_ids.replace(ids);
        assert!(
            previous.is_none(),
            "type IDs must remain stable across replay"
        );
    }

    pub(crate) fn creates_role(&self) -> bool {
        matches!(&*self.statement, Statement::CreateRole(_))
    }

    pub(crate) fn notices(&self) -> &[PlanNotice] {
        &self.notices
    }
}

pub(crate) fn validate_role_attributes(
    state: &CatalogState,
    attributes: &RoleAttributesRaw,
) -> Result<(), AdapterError> {
    if !ENABLE_PASSWORD_AUTH.get(state.system_config().dyncfgs()) {
        if attributes.superuser.is_some() || attributes.password.is_some() {
            return Err(AdapterError::UnavailableFeature {
                feature: "SUPERUSER and PASSWORD attributes".to_string(),
                docs: Some("https://materialize.com/docs/sql/create-role/#details".to_string()),
            });
        }
    }
    Ok(())
}

/// Rejects rules that would lock out the invoking non-internal session.
pub(crate) fn validate_network_policy_change(
    session: &SessionMeta,
    rules: &Vec<NetworkPolicyRule>,
) -> Result<(), AdapterError> {
    if session.user().is_internal() {
        return Ok(());
    }
    let ip = session
        .client_ip()
        .ok_or(AdapterError::NetworkPolicyDenied(
            crate::coord::NetworkPolicyError::MissingIp,
        ))?;
    crate::coord::validate_ip_with_policy_rules(ip, rules)
        .map_err(|_| AdapterError::PlanError(mz_sql::plan::PlanError::NetworkPolicyLockoutError))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mz_sql::names::CommentObjectId;
    use mz_sql::session::vars::VarInput;

    use super::*;
    use crate::catalog::Catalog;
    use crate::catalog::command::{CatalogCommand, drop::DropStatement};
    use crate::session::Session;

    fn statement(sql: &str) -> Arc<Statement<Raw>> {
        Arc::new(
            mz_sql_parser::parser::parse_statements(sql)
                .expect("parse fixture")
                .remove(0)
                .ast,
        )
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn metadata_commands_resolve_prefix_and_reused_names() {
        Catalog::with_debug(|mut catalog| async move {
            let mut session = Session::dummy();
            session.initialize_role_metadata(mz_sql::session::user::MZ_SYSTEM_ROLE_ID);
            session
                .start_transaction(mz_ore::now::to_datetime(1000), None, None)
                .expect("start transaction");
            let command = |sql: &str| {
                CatalogCommand::Metadata(Arc::new(MetadataStatement::new(
                    statement(sql),
                    Params::empty(),
                    session.meta(),
                )))
            };
            let mut commands = [
                command("CREATE ROLE metadata_role"),
                command("COMMENT ON ROLE metadata_role IS 'first'"),
            ];
            let ts = catalog.current_upper().await;
            catalog
                .transact_commands(None, None, ts, None, &mut commands, &mut BTreeMap::new())
                .await
                .expect("comment sees role created in prefix");
            let original_id = catalog
                .try_get_role_by_name("metadata_role")
                .expect("original role")
                .id;
            assert_eq!(
                catalog
                    .state()
                    .comments
                    .get_object_comments(CommentObjectId::Role(original_id))
                    .expect("role comment")
                    .get(&None)
                    .map(String::as_str),
                Some("first")
            );

            let mut comment = MetadataStatement::new(
                statement("COMMENT ON ROLE metadata_role IS 'replacement'"),
                Params::empty(),
                session.meta(),
            );
            comment
                .compile(catalog.state())
                .expect("initial comment target exists");
            let mut commands = [
                CatalogCommand::DropObjects(Arc::new(DropStatement::new(
                    statement("DROP ROLE metadata_role"),
                    Params::empty(),
                    session.meta(),
                ))),
                command("CREATE ROLE metadata_role"),
                CatalogCommand::Metadata(Arc::new(comment)),
            ];
            let ts = catalog.current_upper().await;
            catalog
                .transact_commands(None, None, ts, None, &mut commands, &mut BTreeMap::new())
                .await
                .expect("replan comment against replacement role");
            let replacement_id = catalog
                .try_get_role_by_name("metadata_role")
                .expect("replacement role")
                .id;
            assert_ne!(original_id, replacement_id);
            assert!(
                catalog
                    .state()
                    .comments
                    .get_object_comments(CommentObjectId::Role(original_id))
                    .is_none()
            );
            assert_eq!(
                catalog
                    .state()
                    .comments
                    .get_object_comments(CommentObjectId::Role(replacement_id))
                    .expect("replacement comment")
                    .get(&None)
                    .map(String::as_str),
                Some("replacement")
            );

            let mut limited = catalog.state().clone();
            limited
                .system_config_mut()
                .set("max_roles", VarInput::Flat("0"))
                .expect("set role quota");
            let mut create = MetadataStatement::new(
                statement("CREATE ROLE over_quota"),
                Params::empty(),
                session.meta(),
            );
            assert!(matches!(
                create.compile(&limited),
                Err(AdapterError::ResourceExhaustion { .. })
            ));

            let revision = catalog.transient_revision();
            let mut commands = [
                command("CREATE ROLE aborted_role"),
                command("COMMENT ON ROLE missing_role IS 'invalid'"),
            ];
            let ts = catalog.current_upper().await;
            assert!(
                catalog
                    .transact_commands(None, None, ts, None, &mut commands, &mut BTreeMap::new())
                    .await
                    .is_err()
            );
            assert!(catalog.try_get_role_by_name("aborted_role").is_none());
            assert_eq!(catalog.transient_revision(), revision);
            catalog.expire().await;
        })
        .await;
    }
}

#[cfg(test)]
mod policy_tests;

#[cfg(test)]
mod type_tests;
