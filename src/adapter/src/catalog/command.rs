// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Catalog commands retain intent, not the writes derived from a speculative catalog.
//!
//! Final execution resolves and authorizes each command against the candidate produced
//! by its predecessors.

use std::sync::Arc;

use mz_sql::ast::{Raw, Statement};
use mz_sql::catalog::ObjectType;
use mz_sql::plan::{Params, Plan, PlanNotice};
use mz_sql::session::metadata::SessionMetadata;
use tokio::sync::mpsc;

use crate::catalog::{CatalogState, Op};
use crate::session::SessionMeta;
use crate::{AdapterError, AdapterNotice};

/// A rename or swap whose client-visible outcome must survive final execution.
#[derive(Debug)]
pub struct RenameCommand {
    statement: Arc<Statement<Raw>>,
    params: Params,
    session: SessionMeta,
    outcome: RenameOutcome,
}

#[derive(Debug, PartialEq)]
struct RenameOutcome {
    object_type: ObjectType,
    changed: bool,
    notices: Vec<PlanNotice>,
}

impl RenameCommand {
    pub(crate) fn prepare(
        state: &CatalogState,
        statement: Arc<Statement<Raw>>,
        params: Params,
        session: SessionMeta,
    ) -> Result<(Self, Plan), AdapterError> {
        let (plan, notices) = plan_command(state, &statement, &params, &session)?;
        let (ops, object_type) = rename_ops(&plan);
        Ok((
            Self {
                statement,
                params,
                session,
                outcome: RenameOutcome {
                    object_type,
                    changed: !ops.is_empty(),
                    notices,
                },
            },
            plan,
        ))
    }

    pub(crate) fn notices(&self) -> &[PlanNotice] {
        &self.outcome.notices
    }

    pub(crate) fn object_type(&self) -> ObjectType {
        self.outcome.object_type
    }

    pub(super) fn execute(&self, state: &CatalogState) -> Result<Vec<Op>, AdapterError> {
        let (plan, notices) = plan_command(state, &self.statement, &self.params, &self.session)?;
        let (ops, object_type) = rename_ops(&plan);
        let outcome = RenameOutcome {
            object_type,
            changed: !ops.is_empty(),
            notices,
        };
        if outcome != self.outcome {
            return Err(AdapterError::DDLTransactionOutcomeChanged);
        }
        Ok(ops)
    }
}

/// A deterministic catalog result already returned at this transaction prefix.
#[derive(Debug)]
pub struct CatalogResultCommand {
    statement: Arc<Statement<Raw>>,
    params: Params,
    session: SessionMeta,
    row: mz_repr::Row,
    notices: Vec<PlanNotice>,
}

impl CatalogResultCommand {
    pub(crate) fn notices(&self) -> &[PlanNotice] {
        &self.notices
    }

    pub(crate) fn supports_statement(statement: &Statement<Raw>) -> bool {
        use mz_sql::ast::ShowStatement;
        let Statement::Show(show) = statement else {
            return false;
        };
        match show {
            ShowStatement::ShowCreateView(_)
            | ShowStatement::ShowCreateMaterializedView(_)
            | ShowStatement::ShowCreateSource(_)
            | ShowStatement::ShowCreateTable(_)
            | ShowStatement::ShowCreateSink(_)
            | ShowStatement::ShowCreateMetricSink(_)
            | ShowStatement::ShowCreateIndex(_)
            | ShowStatement::ShowCreateConnection(_)
            | ShowStatement::ShowCreateCluster(_)
            | ShowStatement::ShowCreateType(_) => true,
            ShowStatement::ShowObjects(_)
            | ShowStatement::ShowColumns(_)
            | ShowStatement::ShowVariable(_)
            | ShowStatement::InspectShard(_) => false,
        }
    }

    pub(crate) fn prepare(
        state: &CatalogState,
        statement: Arc<Statement<Raw>>,
        params: Params,
        session: SessionMeta,
    ) -> Result<(Self, Plan), AdapterError> {
        let (plan, notices) = plan_command(state, &statement, &params, &session)?;
        let row = Self::result(&plan)?;
        let command = Self {
            statement,
            params,
            session,
            row,
            notices,
        };
        Ok((command, plan))
    }

    pub(in crate::catalog) fn execute(&self, state: &CatalogState) -> Result<(), AdapterError> {
        let (plan, notices) = plan_command(state, &self.statement, &self.params, &self.session)?;
        if Self::result(&plan)? == self.row && notices == self.notices {
            Ok(())
        } else {
            Err(AdapterError::DDLTransactionOutcomeChanged)
        }
    }

    pub(crate) fn result(plan: &Plan) -> Result<mz_repr::Row, AdapterError> {
        match plan {
            Plan::ShowCreate(show) => Ok(show.row.clone()),
            _ => Err(AdapterError::Unsupported(
                "non-deterministic catalog result",
            )),
        }
    }
}

/// A statement schema exposed or stored for the client at this transaction prefix.
#[derive(Debug)]
pub struct DescriptionCommand {
    statement: Statement<Raw>,
    param_types: Vec<Option<mz_repr::SqlScalarType>>,
    session: SessionMeta,
    expected: mz_sql::plan::StatementDesc,
}

impl DescriptionCommand {
    pub(crate) fn new(
        statement: Statement<Raw>,
        param_types: Vec<Option<mz_repr::SqlScalarType>>,
        session: SessionMeta,
        expected: mz_sql::plan::StatementDesc,
    ) -> Self {
        Self {
            statement,
            param_types,
            session,
            expected,
        }
    }

    pub(in crate::catalog) fn execute(&self, state: &CatalogState) -> Result<(), AdapterError> {
        let (tx, _rx) = mpsc::unbounded_channel();
        let catalog = state.for_session_metadata(&self.session, tx);
        let (statement, _) = mz_sql::names::resolve(&catalog, self.statement.clone())?;
        let current =
            mz_sql::plan::describe(self.session.pcx(), &catalog, statement, &self.param_types)?;
        if current == self.expected {
            Ok(())
        } else {
            Err(AdapterError::DDLTransactionOutcomeChanged)
        }
    }
}

fn plan_command(
    state: &CatalogState,
    statement: &Statement<Raw>,
    params: &Params,
    session: &SessionMeta,
) -> Result<(Plan, Vec<PlanNotice>), AdapterError> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let catalog = state.for_session_metadata(session, tx);
    let (statement, resolved_ids) = mz_sql::names::resolve(&catalog, statement.clone())?;
    let (plan, sql_impl_ids) = mz_sql::plan::plan(
        Some(session.pcx()),
        &catalog,
        statement,
        params,
        &resolved_ids,
    )?;
    mz_sql::rbac::check_plan(
        &catalog,
        None,
        session,
        &plan,
        None,
        &resolved_ids,
        &sql_impl_ids,
    )?;
    let mut notices = Vec::new();
    while let Ok(notice) = rx.try_recv() {
        match notice {
            AdapterNotice::PlanNotice(notice) => notices.push(notice),
            _ => unreachable!("planning only emits plan notices"),
        }
    }
    Ok((plan, notices))
}

fn rename_ops(plan: &Plan) -> (Vec<Op>, ObjectType) {
    match plan {
        Plan::AlterNoop(plan) => (vec![], plan.object_type),
        Plan::AlterItemRename(plan) => (
            vec![Op::RenameItem {
                id: plan.id,
                current_full_name: plan.current_full_name.clone(),
                to_name: plan.to_name.clone(),
            }],
            plan.object_type,
        ),
        Plan::AlterSchemaRename(plan) => (
            vec![Op::RenameSchema {
                database_spec: plan.cur_schema_spec.0,
                schema_spec: plan.cur_schema_spec.1,
                new_name: plan.new_schema_name.clone(),
                check_reserved_names: true,
            }],
            ObjectType::Schema,
        ),
        Plan::AlterSchemaSwap(plan) => (
            vec![
                Op::RenameSchema {
                    database_spec: plan.schema_a_spec.0,
                    schema_spec: plan.schema_a_spec.1,
                    new_name: plan.name_temp.clone(),
                    check_reserved_names: false,
                },
                Op::RenameSchema {
                    database_spec: plan.schema_b_spec.0,
                    schema_spec: plan.schema_b_spec.1,
                    new_name: plan.schema_a_name.clone(),
                    check_reserved_names: false,
                },
                Op::RenameSchema {
                    database_spec: plan.schema_a_spec.0,
                    schema_spec: plan.schema_a_spec.1,
                    new_name: plan.schema_b_name.clone(),
                    check_reserved_names: false,
                },
            ],
            ObjectType::Schema,
        ),
        Plan::AlterClusterRename(plan) => (
            vec![Op::RenameCluster {
                id: plan.id,
                name: plan.name.clone(),
                to_name: plan.to_name.clone(),
                check_reserved_names: true,
            }],
            ObjectType::Cluster,
        ),
        Plan::AlterClusterSwap(plan) => (
            vec![
                Op::RenameCluster {
                    id: plan.id_a,
                    name: plan.name_a.clone(),
                    to_name: plan.name_temp.clone(),
                    check_reserved_names: false,
                },
                Op::RenameCluster {
                    id: plan.id_b,
                    name: plan.name_b.clone(),
                    to_name: plan.name_a.clone(),
                    check_reserved_names: false,
                },
                Op::RenameCluster {
                    id: plan.id_a,
                    name: plan.name_temp.clone(),
                    to_name: plan.name_b.clone(),
                    check_reserved_names: false,
                },
            ],
            ObjectType::Cluster,
        ),
        Plan::AlterClusterReplicaRename(plan) => (
            vec![Op::RenameClusterReplica {
                cluster_id: plan.cluster_id,
                replica_id: plan.replica_id,
                name: plan.name.clone(),
                to_name: plan.to_name.clone(),
            }],
            ObjectType::ClusterReplica,
        ),
        _ => unreachable!("rename/swap planning must produce a rename or no-op plan"),
    }
}
