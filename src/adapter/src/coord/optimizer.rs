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

impl Coordinator {
    pub(crate) fn optimize_plan(
        &mut self,
        plan: Plan<Unoptimized>,
    ) -> Result<Plan<Optimized>, OptimizerError> {
        Ok(match plan {
            Plan::CreateConnection(plan) => Plan::CreateConnection(plan),
            Plan::CreateDatabase(plan) => Plan::CreateDatabase(plan),
            Plan::CreateSchema(plan) => Plan::CreateSchema(plan),
            Plan::CreateRole(plan) => Plan::CreateRole(plan),
            Plan::CreateCluster(plan) => Plan::CreateCluster(plan),
            Plan::CreateClusterReplica(plan) => Plan::CreateClusterReplica(plan),
            Plan::CreateSource(plan) => Plan::CreateSource(plan),
            Plan::CreateSources(plan) => Plan::CreateSources(plan),
            Plan::CreateSecret(plan) => Plan::CreateSecret(plan),
            Plan::CreateSink(plan) => Plan::CreateSink(plan),
            Plan::CreateTable(plan) => Plan::CreateTable(plan),
            Plan::CreateView(plan) => Plan::CreateView(self.optimize_create_view(plan)?),
            Plan::CreateMaterializedView(plan) => Plan::CreateMaterializedView(plan),
            Plan::CreateIndex(plan) => Plan::CreateIndex(plan),
            Plan::CreateType(plan) => Plan::CreateType(plan),
            Plan::Comment(plan) => Plan::Comment(plan),
            Plan::DiscardTemp => Plan::DiscardTemp,
            Plan::DiscardAll => Plan::DiscardAll,
            Plan::DropObjects(plan) => Plan::DropObjects(plan),
            Plan::DropOwned(plan) => Plan::DropOwned(plan),
            Plan::EmptyQuery => Plan::EmptyQuery,
            Plan::ShowAllVariables => Plan::ShowAllVariables,
            Plan::ShowCreate(plan) => Plan::ShowCreate(plan),
            Plan::ShowColumns(plan) => Plan::ShowColumns(plan),
            Plan::ShowVariable(plan) => Plan::ShowVariable(plan),
            Plan::InspectShard(plan) => Plan::InspectShard(plan),
            Plan::SetVariable(plan) => Plan::SetVariable(plan),
            Plan::ResetVariable(plan) => Plan::ResetVariable(plan),
            Plan::SetTransaction(plan) => Plan::SetTransaction(plan),
            Plan::StartTransaction(plan) => Plan::StartTransaction(plan),
            Plan::CommitTransaction(plan) => Plan::CommitTransaction(plan),
            Plan::AbortTransaction(plan) => Plan::AbortTransaction(plan),
            Plan::Select(plan) => Plan::Select(plan),
            Plan::Subscribe(plan) => Plan::Subscribe(plan),
            Plan::CopyFrom(plan) => Plan::CopyFrom(plan),
            Plan::ExplainPlan(plan) => Plan::ExplainPlan(plan),
            Plan::ExplainTimestamp(plan) => Plan::ExplainTimestamp(plan),
            Plan::ExplainSinkSchema(plan) => Plan::ExplainSinkSchema(plan),
            Plan::Insert(plan) => Plan::Insert(plan),
            Plan::AlterCluster(plan) => Plan::AlterCluster(plan),
            Plan::AlterClusterSwap(plan) => Plan::AlterClusterSwap(plan),
            Plan::AlterNoop(plan) => Plan::AlterNoop(plan),
            Plan::AlterIndexSetOptions(plan) => Plan::AlterIndexSetOptions(plan),
            Plan::AlterIndexResetOptions(plan) => Plan::AlterIndexResetOptions(plan),
            Plan::AlterSetCluster(plan) => Plan::AlterSetCluster(plan),
            Plan::AlterSink(plan) => Plan::AlterSink(plan),
            Plan::AlterConnection(plan) => Plan::AlterConnection(plan),
            Plan::AlterSource(plan) => Plan::AlterSource(plan),
            Plan::PurifiedAlterSource {
                alter_source,
                subsources,
            } => Plan::PurifiedAlterSource {
                alter_source,
                subsources,
            },
            Plan::AlterClusterRename(plan) => Plan::AlterClusterRename(plan),
            Plan::AlterClusterReplicaRename(plan) => Plan::AlterClusterReplicaRename(plan),
            Plan::AlterItemRename(plan) => Plan::AlterItemRename(plan),
            Plan::AlterItemSwap(plan) => Plan::AlterItemSwap(plan),
            Plan::AlterSchemaRename(plan) => Plan::AlterSchemaRename(plan),
            Plan::AlterSchemaSwap(plan) => Plan::AlterSchemaSwap(plan),
            Plan::AlterSecret(plan) => Plan::AlterSecret(plan),
            Plan::AlterSystemSet(plan) => Plan::AlterSystemSet(plan),
            Plan::AlterSystemReset(plan) => Plan::AlterSystemReset(plan),
            Plan::AlterSystemResetAll(plan) => Plan::AlterSystemResetAll(plan),
            Plan::AlterRole(plan) => Plan::AlterRole(plan),
            Plan::AlterOwner(plan) => Plan::AlterOwner(plan),
            Plan::Declare(plan) => Plan::Declare(plan),
            Plan::Fetch(plan) => Plan::Fetch(plan),
            Plan::Close(plan) => Plan::Close(plan),
            Plan::ReadThenWrite(plan) => Plan::ReadThenWrite(plan),
            Plan::Prepare(plan) => Plan::Prepare(plan),
            Plan::Execute(plan) => Plan::Execute(plan),
            Plan::Deallocate(plan) => Plan::Deallocate(plan),
            Plan::Raise(plan) => Plan::Raise(plan),
            Plan::GrantRole(plan) => Plan::GrantRole(plan),
            Plan::RevokeRole(plan) => Plan::RevokeRole(plan),
            Plan::GrantPrivileges(plan) => Plan::GrantPrivileges(plan),
            Plan::RevokePrivileges(plan) => Plan::RevokePrivileges(plan),
            Plan::AlterDefaultPrivileges(plan) => Plan::AlterDefaultPrivileges(plan),
            Plan::ReassignOwned(plan) => Plan::ReassignOwned(plan),
            Plan::SideEffectingFunc(plan) => Plan::SideEffectingFunc(plan),
            Plan::ValidateConnection(plan) => Plan::ValidateConnection(plan),
        })
    }

    fn optimize_create_view(
        &mut self,
        plan: CreateViewPlan<Unoptimized>,
    ) -> Result<CreateViewPlan<Optimized>, OptimizerError> {
        // Collect optimizer parameters.
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

        // Build an optimizer for this VIEW.
        let mut optimizer = optimize::view::Optimizer::new(optimizer_config);

        // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local)
        let raw_expr = plan.view.expr.clone();
        let optimized_expr = optimizer.optimize(raw_expr)?;

        let view = View {
            create_sql: plan.view.create_sql,
            expr: (plan.view.expr, optimized_expr),
            column_names: plan.view.column_names,
            temporary: plan.view.temporary,
        };
        Ok(CreateViewPlan {
            name: plan.name,
            view,
            replace: plan.replace,
            drop_ids: plan.drop_ids,
            if_not_exists: plan.if_not_exists,
            ambiguous_columns: plan.ambiguous_columns,
        })
    }
}
