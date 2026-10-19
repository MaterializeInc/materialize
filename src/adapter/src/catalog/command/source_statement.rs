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

use itertools::Itertools;
use mz_catalog::memory::objects::{CatalogItem, Source, Table};
use mz_repr::{CatalogItemId, GlobalId};
use mz_sql::ast::{Raw, Statement};
use mz_sql::names::{Aug, QualifiedItemName, ResolvedIds};
use mz_sql::plan::{Params, Plan, PlanNotice};
use mz_sql::pure::{
    PurifiedStatement, SourcePreparation, SourcePreparationRequest, SourcePreparationResponse,
    SourcePreparationStatus,
};
use mz_sql::session::metadata::SessionMetadata;
use tokio::sync::mpsc;

use super::source::{SourceObject, SourcePlanning, plan_create_source};
use crate::catalog::{CatalogState, ConnCatalog, Op};
use crate::session::SessionMeta;
use crate::{AdapterError, AdapterNotice};

#[derive(Clone, Debug)]
pub(crate) struct SourceStatement {
    statement: Arc<Statement<Raw>>,
    params: Params,
    session: Arc<SessionMeta>,
    preparation: SourcePreparation,
    ids: BTreeMap<SourceObject, (CatalogItemId, GlobalId)>,
}

#[derive(Debug)]
pub(crate) enum SourceResolution {
    Prepare(SourcePreparationRequest),
    Allocate(Vec<SourceObject>),
    Ready(Box<SourceCompilation>),
}

#[derive(Debug)]
pub(crate) struct SourceCompilation {
    pub plan: Plan,
    pub(in crate::catalog) ops: Vec<Op>,
    pub(in crate::catalog) if_not_exists: BTreeMap<CatalogItemId, QualifiedItemName>,
    pub(in crate::catalog) notices: Vec<PlanNotice>,
}

impl SourceStatement {
    pub(crate) fn new(
        statement: Arc<Statement<Raw>>,
        params: Params,
        session: SessionMeta,
        now: u64,
    ) -> Self {
        Self {
            statement,
            params,
            session: Arc::new(session),
            preparation: SourcePreparation::new(now),
            ids: BTreeMap::new(),
        }
    }

    pub(in crate::catalog) fn connection_id(&self) -> &mz_adapter_types::connection::ConnectionId {
        self.session.conn_id()
    }

    pub(crate) fn record_ids(
        &mut self,
        keys: Vec<SourceObject>,
        ids: Vec<(CatalogItemId, GlobalId)>,
    ) {
        for (key, id) in keys.into_iter().zip_eq(ids) {
            self.ids.entry(key).or_insert(id);
        }
    }

    pub(in crate::catalog) fn resolve(
        &self,
        state: &CatalogState,
    ) -> Result<SourceResolution, AdapterError> {
        let (tx, rx) = mpsc::unbounded_channel();
        let (catalog, statement) = self.input(state, tx)?;
        let status = self.preparation.resolve(&catalog, statement)?;
        self.compile(&catalog, status, rx)
    }

    pub(in crate::catalog) fn finish(
        &mut self,
        state: &CatalogState,
        response: SourcePreparationResponse,
    ) -> Result<SourceResolution, AdapterError> {
        let (tx, rx) = mpsc::unbounded_channel();
        let (catalog, statement) = self.input(state, tx)?;
        let status = self.preparation.finish(&catalog, statement, response)?;
        self.compile(&catalog, status, rx)
    }

    fn input<'a>(
        &self,
        state: &'a CatalogState,
        notices: mpsc::UnboundedSender<AdapterNotice>,
    ) -> Result<(ConnCatalog<'a>, Statement<Aug>), AdapterError> {
        let catalog = state.for_session_metadata(self.session.as_ref(), notices);
        let (statement, mut resolved_ids) =
            mz_sql::names::resolve(&catalog, (*self.statement).clone())?;
        if matches!(&statement, Statement::AlterSource(stmt)
            if matches!(stmt.action, mz_sql::ast::AlterSourceAction::AddSubsources { .. }))
        {
            if let Some(item) = mz_sql::pure::statement_source(&catalog, &statement) {
                if let Some(source) = state.get_entry(&item.id()).source() {
                    resolved_ids.extend_from(&source.resolved_ids);
                }
            }
        }
        mz_sql::rbac::check_purification(
            &catalog,
            self.session.as_ref(),
            mz_sql::pure::statement_source(&catalog, &statement),
            &resolved_ids,
        )?;
        Ok((catalog, statement))
    }

    fn compile(
        &self,
        catalog: &ConnCatalog<'_>,
        status: SourcePreparationStatus,
        mut notices_rx: mpsc::UnboundedReceiver<AdapterNotice>,
    ) -> Result<SourceResolution, AdapterError> {
        let purified = match status {
            SourcePreparationStatus::Pending(request) => {
                return Ok(SourceResolution::Prepare(request));
            }
            SourcePreparationStatus::Ready(statement) => *statement,
        };
        let (plan, mut resolved_ids, sql_impl_ids) = match purified {
            source @ PurifiedStatement::PurifiedCreateSource { .. } => {
                match plan_create_source(
                    catalog,
                    self.session.pcx(),
                    &self.params,
                    source,
                    &self.ids,
                )? {
                    SourcePlanning::NeedsIds(keys) => {
                        return Ok(SourceResolution::Allocate(keys));
                    }
                    SourcePlanning::Ready(plans) => (
                        Plan::CreateSources(plans),
                        ResolvedIds::empty(),
                        ResolvedIds::empty(),
                    ),
                }
            }
            PurifiedStatement::PurifiedCreateTableFromSource { stmt } => {
                let statement = Statement::CreateTableFromSource(stmt);
                let resolved_ids = mz_sql::names::visit_dependencies(catalog, &statement);
                let (plan, sql_impl_ids) = mz_sql::plan::plan(
                    Some(self.session.pcx()),
                    catalog,
                    statement,
                    &self.params,
                    &resolved_ids,
                )?;
                (plan, resolved_ids, sql_impl_ids)
            }
            source @ PurifiedStatement::PurifiedAlterSourceAddSubsources { .. } => {
                match super::source::plan_alter_source_add_subsources(
                    catalog,
                    self.session.pcx(),
                    &self.params,
                    source,
                    &self.ids,
                )? {
                    SourcePlanning::NeedsIds(keys) => return Ok(SourceResolution::Allocate(keys)),
                    SourcePlanning::Ready(plan) => {
                        (plan, ResolvedIds::empty(), ResolvedIds::empty())
                    }
                }
            }
            PurifiedStatement::PurifiedAlterSourceRefreshReferences {
                source_name,
                available_source_references,
            } => (
                super::source::plan_alter_source_refresh_references(
                    catalog,
                    source_name,
                    available_source_references,
                )?,
                ResolvedIds::empty(),
                ResolvedIds::empty(),
            ),
            PurifiedStatement::PurifiedAlterSource { alter_source_stmt } => {
                let statement = Statement::AlterSource(alter_source_stmt);
                let resolved_ids = mz_sql::names::visit_dependencies(catalog, &statement);
                let (plan, sql_impl_ids) = mz_sql::plan::plan(
                    Some(self.session.pcx()),
                    catalog,
                    statement,
                    &self.params,
                    &resolved_ids,
                )?;
                (plan, resolved_ids, sql_impl_ids)
            }
            _ => return Err(AdapterError::Unsupported("unsupported source command")),
        };
        let mut altered_source = match &plan {
            Plan::AlterSource(mz_sql::plan::AlterSourcePlan {
                item_id,
                action:
                    mz_sql::plan::AlterSourceAction::AddSubsourceExports {
                        options,
                        subsources,
                    },
                ..
            }) => {
                let source =
                    super::source::alter::rebuild_source(catalog, *item_id, options.clone())?;
                resolved_ids.extend_from(&source.resolved_ids);
                for subsource in subsources {
                    resolved_ids.extend_from(&subsource.resolved_ids);
                }
                Some(source)
            }
            _ => None,
        };
        mz_sql::rbac::check_plan(
            catalog,
            None,
            self.session.as_ref(),
            &plan,
            None,
            &resolved_ids,
            &sql_impl_ids,
        )?;
        let owner_id = self.session.role_metadata().current_role;
        let mut ops = Vec::new();
        let mut if_not_exists = BTreeMap::new();
        let creations: &[mz_sql::plan::CreateSourcePlanBundle] = match &plan {
            Plan::CreateSources(plans) => plans,
            Plan::AlterSource(mz_sql::plan::AlterSourcePlan {
                item_id, action, ..
            }) => match action {
                mz_sql::plan::AlterSourceAction::AddSubsourceExports { subsources, .. } => {
                    ops.push(Op::UpdateItem {
                        id: *item_id,
                        name: catalog.state().get_entry(item_id).name().clone(),
                        to_item: CatalogItem::Source(
                            altered_source.take().expect("rebuilt source"),
                        ),
                    });
                    subsources
                }
                mz_sql::plan::AlterSourceAction::RefreshReferences { references } => {
                    ops.push(Op::UpdateSourceReferences {
                        source_id: *item_id,
                        references: references.clone().into(),
                    });
                    &[]
                }
            },
            Plan::AlterNoop(_) => &[],

            Plan::CreateTable(plan) => {
                if plan.table.temporary {
                    return Err(AdapterError::Unsupported("temporary source table command"));
                }
                let Some(&(id, global_id)) = self.ids.get(&SourceObject::Primary) else {
                    return Ok(SourceResolution::Allocate(vec![SourceObject::Primary]));
                };
                if plan.if_not_exists {
                    if_not_exists.insert(id, plan.name.clone());
                }
                ops.push(Op::CreateItem {
                    id,
                    name: plan.name.clone(),
                    item: CatalogItem::Table(Table::from_plan(
                        plan.table.clone(),
                        global_id,
                        self.session.conn_id().clone(),
                        resolved_ids,
                    )),
                    owner_id,
                });
                &[]
            }
            _ => return Err(AdapterError::Unsupported("unsupported source plan")),
        };
        for bundle in creations {
            let mz_sql::plan::CreateSourcePlanBundle {
                item_id,
                global_id,
                plan,
                resolved_ids,
                available_source_references,
            } = bundle;
            if matches!(
                &plan.source.data_source,
                mz_sql::plan::DataSourceDesc::Webhook { .. }
            ) {
                return Err(AdapterError::Unsupported("webhook source command"));
            }
            if plan.if_not_exists {
                if_not_exists.insert(*item_id, plan.name.clone());
            }
            ops.push(Op::CreateItem {
                id: *item_id,
                name: plan.name.clone(),
                item: CatalogItem::Source(Source::new(
                    plan.clone(),
                    *global_id,
                    resolved_ids.clone(),
                    None,
                    false,
                )),
                owner_id,
            });
            if let Some(references) = available_source_references {
                ops.push(Op::UpdateSourceReferences {
                    source_id: *item_id,
                    references: references.clone().into(),
                });
            }
        }
        let mut notices = Vec::new();
        while let Ok(notice) = notices_rx.try_recv() {
            match notice {
                AdapterNotice::PlanNotice(notice) => notices.push(notice),
                _ => unreachable!("planning only emits plan notices"),
            }
        }
        Ok(SourceResolution::Ready(Box::new(SourceCompilation {
            plan,
            ops,
            if_not_exists,
            notices,
        })))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SourceOutcome {
    skipped_name: Option<String>,
    notices: Vec<PlanNotice>,
}

#[derive(Clone, Debug)]
pub struct SourceCommand {
    pub(in crate::catalog) statement: SourceStatement,
    pub(in crate::catalog) outcome: SourceOutcome,
    pub(in crate::catalog) observed: bool,
}

impl SourceCompilation {
    pub(in crate::catalog) fn created(&self) -> SourceOutcome {
        SourceOutcome {
            skipped_name: None,
            notices: self.notices.clone(),
        }
    }

    pub(in crate::catalog) fn skipped(&self, error: &AdapterError) -> Option<SourceOutcome> {
        let AdapterError::Catalog(mz_catalog::memory::error::Error {
            kind:
                mz_catalog::memory::error::ErrorKind::Sql(
                    mz_sql::catalog::CatalogError::ItemAlreadyExists(id, _),
                ),
        }) = error
        else {
            return None;
        };
        self.if_not_exists.get(id).map(|name| SourceOutcome {
            skipped_name: Some(name.item.clone()),
            notices: self.notices.clone(),
        })
    }
}

impl SourceCommand {
    /// Allows changes to an outcome that has not been exposed to the client.
    pub(crate) fn unobserved(mut self) -> Self {
        self.observed = false;
        self
    }

    pub(crate) fn item_ids(&self) -> impl Iterator<Item = CatalogItemId> + '_ {
        self.statement.ids.values().map(|(id, _)| *id)
    }

    pub(crate) fn skipped_notice(&self) -> Option<AdapterNotice> {
        self.skipped_name()
            .map(|name| AdapterNotice::ObjectAlreadyExists {
                name: name.to_owned(),
                ty: match &*self.statement.statement {
                    Statement::CreateSource(_) => "source",
                    Statement::CreateTableFromSource(_) => "table",
                    _ => unreachable!("source command statement"),
                },
            })
    }

    pub(crate) fn record_ids(
        &mut self,
        keys: Vec<SourceObject>,
        ids: Vec<(CatalogItemId, GlobalId)>,
    ) {
        self.statement.record_ids(keys, ids);
    }

    pub(crate) fn notices(&self) -> &[PlanNotice] {
        &self.outcome.notices
    }

    pub(crate) fn skipped_name(&self) -> Option<&str> {
        self.outcome.skipped_name.as_deref()
    }

    pub(in crate::catalog) fn check_outcome(
        &mut self,
        outcome: SourceOutcome,
    ) -> Result<(), AdapterError> {
        if self.observed && self.outcome != outcome {
            return Err(AdapterError::DDLTransactionOutcomeChanged);
        }
        self.outcome = outcome;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum SourceStatementPreparation {
    Prepare(SourcePreparationRequest),
    Allocate(Vec<SourceObject>),
    Ready(Box<PreparedSourceStatement>),
}

#[derive(Debug)]
pub(crate) struct PreparedSourceStatement {
    pub command: SourceCommand,
    pub state: CatalogState,
    pub snapshot: mz_catalog::durable::Snapshot,
    pub plan: Plan,
}
