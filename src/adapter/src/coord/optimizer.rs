// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic for optimizing [`Plan`]s.

use mz_sql::plan::{CreateViewPlan, Optimized, Plan, Unoptimized, View};

use crate::coord::Coordinator;
use crate::optimize;
use crate::optimize::{Optimize, OptimizerError};

/// Type alias for function that returns an optimized plan.
type ExpensiveFn = Box<dyn FnOnce() -> Result<Plan<Optimized>, OptimizerError> + Send>;

/// Represents the work that needs to be executed to optimize a plan.
pub(crate) enum PlanOptimization {
    /// No optimization is needed and an optimized plan can be returned immediately.
    NoOp(Plan<Optimized>),
    /// A potentially expensive amount of work is needed to optimize the plan.
    Expensive(ExpensiveFn),
}

impl PlanOptimization {
    pub(crate) fn execute(self) -> Result<Plan<Optimized>, OptimizerError> {
        match self {
            PlanOptimization::NoOp(plan) => Ok(plan),
            PlanOptimization::Expensive(f) => f(),
        }
    }
}

impl Coordinator {
    pub(crate) fn optimize_plan(&mut self, plan: Plan<Unoptimized>) -> PlanOptimization {
        match plan {
            Plan::CreateView(plan) => PlanOptimization::Expensive(self.optimize_create_view(plan)),
            Plan::CreateConnection(plan) => PlanOptimization::NoOp(Plan::CreateConnection(plan)),
            Plan::CreateDatabase(plan) => PlanOptimization::NoOp(Plan::CreateDatabase(plan)),
            Plan::CreateSchema(plan) => PlanOptimization::NoOp(Plan::CreateSchema(plan)),
            Plan::CreateRole(plan) => PlanOptimization::NoOp(Plan::CreateRole(plan)),
            Plan::CreateCluster(plan) => PlanOptimization::NoOp(Plan::CreateCluster(plan)),
            Plan::CreateClusterReplica(plan) => {
                PlanOptimization::NoOp(Plan::CreateClusterReplica(plan))
            }
            Plan::CreateSource(plan) => PlanOptimization::NoOp(Plan::CreateSource(plan)),
            Plan::CreateSources(plan) => PlanOptimization::NoOp(Plan::CreateSources(plan)),
            Plan::CreateSecret(plan) => PlanOptimization::NoOp(Plan::CreateSecret(plan)),
            Plan::CreateSink(plan) => PlanOptimization::NoOp(Plan::CreateSink(plan)),
            Plan::CreateTable(plan) => PlanOptimization::NoOp(Plan::CreateTable(plan)),
            Plan::CreateMaterializedView(plan) => {
                PlanOptimization::NoOp(Plan::CreateMaterializedView(plan))
            }
            Plan::CreateIndex(plan) => PlanOptimization::NoOp(Plan::CreateIndex(plan)),
            Plan::CreateType(plan) => PlanOptimization::NoOp(Plan::CreateType(plan)),
            Plan::Comment(plan) => PlanOptimization::NoOp(Plan::Comment(plan)),
            Plan::DiscardTemp => PlanOptimization::NoOp(Plan::DiscardTemp),
            Plan::DiscardAll => PlanOptimization::NoOp(Plan::DiscardAll),
            Plan::DropObjects(plan) => PlanOptimization::NoOp(Plan::DropObjects(plan)),
            Plan::DropOwned(plan) => PlanOptimization::NoOp(Plan::DropOwned(plan)),
            Plan::EmptyQuery => PlanOptimization::NoOp(Plan::EmptyQuery),
            Plan::ShowAllVariables => PlanOptimization::NoOp(Plan::ShowAllVariables),
            Plan::ShowCreate(plan) => PlanOptimization::NoOp(Plan::ShowCreate(plan)),
            Plan::ShowColumns(plan) => PlanOptimization::NoOp(Plan::ShowColumns(plan)),
            Plan::ShowVariable(plan) => PlanOptimization::NoOp(Plan::ShowVariable(plan)),
            Plan::InspectShard(plan) => PlanOptimization::NoOp(Plan::InspectShard(plan)),
            Plan::SetVariable(plan) => PlanOptimization::NoOp(Plan::SetVariable(plan)),
            Plan::ResetVariable(plan) => PlanOptimization::NoOp(Plan::ResetVariable(plan)),
            Plan::SetTransaction(plan) => PlanOptimization::NoOp(Plan::SetTransaction(plan)),
            Plan::StartTransaction(plan) => PlanOptimization::NoOp(Plan::StartTransaction(plan)),
            Plan::CommitTransaction(plan) => PlanOptimization::NoOp(Plan::CommitTransaction(plan)),
            Plan::AbortTransaction(plan) => PlanOptimization::NoOp(Plan::AbortTransaction(plan)),
            Plan::Select(plan) => PlanOptimization::NoOp(Plan::Select(plan)),
            Plan::Subscribe(plan) => PlanOptimization::NoOp(Plan::Subscribe(plan)),
            Plan::CopyFrom(plan) => PlanOptimization::NoOp(Plan::CopyFrom(plan)),
            Plan::ExplainPlan(plan) => PlanOptimization::NoOp(Plan::ExplainPlan(plan)),
            Plan::ExplainTimestamp(plan) => PlanOptimization::NoOp(Plan::ExplainTimestamp(plan)),
            Plan::ExplainSinkSchema(plan) => PlanOptimization::NoOp(Plan::ExplainSinkSchema(plan)),
            Plan::Insert(plan) => PlanOptimization::NoOp(Plan::Insert(plan)),
            Plan::AlterCluster(plan) => PlanOptimization::NoOp(Plan::AlterCluster(plan)),
            Plan::AlterClusterSwap(plan) => PlanOptimization::NoOp(Plan::AlterClusterSwap(plan)),
            Plan::AlterNoop(plan) => PlanOptimization::NoOp(Plan::AlterNoop(plan)),
            Plan::AlterIndexSetOptions(plan) => {
                PlanOptimization::NoOp(Plan::AlterIndexSetOptions(plan))
            }
            Plan::AlterIndexResetOptions(plan) => {
                PlanOptimization::NoOp(Plan::AlterIndexResetOptions(plan))
            }
            Plan::AlterSetCluster(plan) => PlanOptimization::NoOp(Plan::AlterSetCluster(plan)),
            Plan::AlterSink(plan) => PlanOptimization::NoOp(Plan::AlterSink(plan)),
            Plan::AlterConnection(plan) => PlanOptimization::NoOp(Plan::AlterConnection(plan)),
            Plan::AlterSource(plan) => PlanOptimization::NoOp(Plan::AlterSource(plan)),
            Plan::PurifiedAlterSource {
                alter_source,
                subsources,
            } => PlanOptimization::NoOp(Plan::PurifiedAlterSource {
                alter_source,
                subsources,
            }),
            Plan::AlterClusterRename(plan) => {
                PlanOptimization::NoOp(Plan::AlterClusterRename(plan))
            }
            Plan::AlterClusterReplicaRename(plan) => {
                PlanOptimization::NoOp(Plan::AlterClusterReplicaRename(plan))
            }
            Plan::AlterItemRename(plan) => PlanOptimization::NoOp(Plan::AlterItemRename(plan)),
            Plan::AlterItemSwap(plan) => PlanOptimization::NoOp(Plan::AlterItemSwap(plan)),
            Plan::AlterSchemaRename(plan) => PlanOptimization::NoOp(Plan::AlterSchemaRename(plan)),
            Plan::AlterSchemaSwap(plan) => PlanOptimization::NoOp(Plan::AlterSchemaSwap(plan)),
            Plan::AlterSecret(plan) => PlanOptimization::NoOp(Plan::AlterSecret(plan)),
            Plan::AlterSystemSet(plan) => PlanOptimization::NoOp(Plan::AlterSystemSet(plan)),
            Plan::AlterSystemReset(plan) => PlanOptimization::NoOp(Plan::AlterSystemReset(plan)),
            Plan::AlterSystemResetAll(plan) => {
                PlanOptimization::NoOp(Plan::AlterSystemResetAll(plan))
            }
            Plan::AlterRole(plan) => PlanOptimization::NoOp(Plan::AlterRole(plan)),
            Plan::AlterOwner(plan) => PlanOptimization::NoOp(Plan::AlterOwner(plan)),
            Plan::Declare(plan) => PlanOptimization::NoOp(Plan::Declare(plan)),
            Plan::Fetch(plan) => PlanOptimization::NoOp(Plan::Fetch(plan)),
            Plan::Close(plan) => PlanOptimization::NoOp(Plan::Close(plan)),
            Plan::ReadThenWrite(plan) => PlanOptimization::NoOp(Plan::ReadThenWrite(plan)),
            Plan::Prepare(plan) => PlanOptimization::NoOp(Plan::Prepare(plan)),
            Plan::Execute(plan) => PlanOptimization::NoOp(Plan::Execute(plan)),
            Plan::Deallocate(plan) => PlanOptimization::NoOp(Plan::Deallocate(plan)),
            Plan::Raise(plan) => PlanOptimization::NoOp(Plan::Raise(plan)),
            Plan::GrantRole(plan) => PlanOptimization::NoOp(Plan::GrantRole(plan)),
            Plan::RevokeRole(plan) => PlanOptimization::NoOp(Plan::RevokeRole(plan)),
            Plan::GrantPrivileges(plan) => PlanOptimization::NoOp(Plan::GrantPrivileges(plan)),
            Plan::RevokePrivileges(plan) => PlanOptimization::NoOp(Plan::RevokePrivileges(plan)),
            Plan::AlterDefaultPrivileges(plan) => {
                PlanOptimization::NoOp(Plan::AlterDefaultPrivileges(plan))
            }
            Plan::ReassignOwned(plan) => PlanOptimization::NoOp(Plan::ReassignOwned(plan)),
            Plan::SideEffectingFunc(plan) => PlanOptimization::NoOp(Plan::SideEffectingFunc(plan)),
            Plan::ValidateConnection(plan) => {
                PlanOptimization::NoOp(Plan::ValidateConnection(plan))
            }
        }
    }

    fn optimize_create_view(&mut self, plan: CreateViewPlan<Unoptimized>) -> ExpensiveFn {
        // Collect optimizer parameters.
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

        // Build an optimizer for this VIEW.
        let mut optimizer = optimize::view::Optimizer::new(optimizer_config);

        Box::new(move || {
            // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local)
            let raw_expr = plan.view.expr.clone();
            let optimized_expr = optimizer.optimize(raw_expr)?;

            let view = View {
                create_sql: plan.view.create_sql,
                expr: (plan.view.expr, optimized_expr),
                column_names: plan.view.column_names,
                temporary: plan.view.temporary,
            };
            Ok(Plan::CreateView(CreateViewPlan {
                name: plan.name,
                view,
                replace: plan.replace,
                drop_ids: plan.drop_ids,
                if_not_exists: plan.if_not_exists,
                ambiguous_columns: plan.ambiguous_columns,
            }))
        })
    }
}
