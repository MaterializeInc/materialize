// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Special cases related to the "introspection" of Materialize
//!
//! Every Materialize deployment has a pre-installed [`mz_introspection`] cluster, which
//! has several indexes to speed up common introspection queries. We also have a special
//! `mz_support` role, which can be used by support teams to diagnose a deployment.
//! For each of these use cases, we have some special restrictions we want to apply. The
//! logic around these restrictions is defined here.
//!
//!
//! [`mz_introspection`]: https://materialize.com/docs/sql/show-clusters/#mz_introspection-system-cluster

use mz_expr::CollectionPlan;
use mz_repr::GlobalId;
use mz_sql::catalog::SessionCatalog;
use mz_sql::plan::{
    ExplainPlanPlan, ExplainTimestampPlan, Explainee, ExplaineeStatement, Plan, SubscribeFrom,
};
use smallvec::SmallVec;

use crate::catalog::ConnCatalog;
use crate::coord::TargetCluster;
use crate::notice::AdapterNotice;
use crate::session::Session;
use crate::AdapterError;
use mz_catalog::builtin::MZ_INTROSPECTION_CLUSTER;

/// Checks whether or not we should automatically run a query on the `mz_introspection`
/// cluster, as opposed to whatever the current default cluster is.
pub fn auto_run_on_introspection<'a, 's, 'p>(
    catalog: &'a ConnCatalog<'a>,
    session: &'s Session,
    plan: &'p Plan,
) -> TargetCluster {
    let (depends_on, could_run_expensive_function) = match plan {
        Plan::Select(plan) => (
            plan.source.depends_on(),
            plan.source.could_run_expensive_function(),
        ),
        Plan::ShowColumns(plan) => (
            plan.select_plan.source.depends_on(),
            plan.select_plan.source.could_run_expensive_function(),
        ),
        Plan::Subscribe(plan) => (
            plan.from.depends_on(),
            match &plan.from {
                SubscribeFrom::Id(_) => false,
                SubscribeFrom::Query { expr, desc: _ } => expr.could_run_expensive_function(),
            },
        ),
        Plan::ExplainPlan(ExplainPlanPlan {
            explainee: Explainee::Statement(ExplaineeStatement::Select { plan, .. }),
            ..
        }) => (
            plan.source.depends_on(),
            plan.source.could_run_expensive_function(),
        ),
        Plan::ExplainTimestamp(ExplainTimestampPlan { raw_plan, .. }) => (
            raw_plan.depends_on(),
            raw_plan.could_run_expensive_function(),
        ),
        Plan::CreateConnection(_)
        | Plan::CreateDatabase(_)
        | Plan::CreateSchema(_)
        | Plan::CreateRole(_)
        | Plan::CreateCluster(_)
        | Plan::CreateClusterReplica(_)
        | Plan::CreateSource(_)
        | Plan::CreateSources(_)
        | Plan::CreateSecret(_)
        | Plan::CreateSink(_)
        | Plan::CreateTable(_)
        | Plan::CreateView(_)
        | Plan::CreateMaterializedView(_)
        | Plan::CreateIndex(_)
        | Plan::CreateType(_)
        | Plan::Comment(_)
        | Plan::DiscardTemp
        | Plan::DiscardAll
        | Plan::DropObjects(_)
        | Plan::DropOwned(_)
        | Plan::EmptyQuery
        | Plan::ShowAllVariables
        | Plan::ShowCreate(_)
        | Plan::ShowVariable(_)
        | Plan::InspectShard(_)
        | Plan::SetVariable(_)
        | Plan::ResetVariable(_)
        | Plan::SetTransaction(_)
        | Plan::StartTransaction(_)
        | Plan::CommitTransaction(_)
        | Plan::AbortTransaction(_)
        | Plan::CopyFrom(_)
        | Plan::CopyTo(_)
        | Plan::ExplainPlan(_)
        | Plan::ExplainPushdown(_)
        | Plan::ExplainSinkSchema(_)
        | Plan::Insert(_)
        | Plan::AlterNoop(_)
        | Plan::AlterClusterRename(_)
        | Plan::AlterClusterSwap(_)
        | Plan::AlterClusterReplicaRename(_)
        | Plan::AlterCluster(_)
        | Plan::AlterConnection(_)
        | Plan::AlterSource(_)
        | Plan::AlterSetCluster(_)
        | Plan::AlterItemRename(_)
        | Plan::AlterItemSwap(_)
        | Plan::AlterRetainHistory(_)
        | Plan::AlterSchemaRename(_)
        | Plan::AlterSchemaSwap(_)
        | Plan::AlterSecret(_)
        | Plan::AlterSystemSet(_)
        | Plan::AlterSystemReset(_)
        | Plan::AlterSystemResetAll(_)
        | Plan::AlterRole(_)
        | Plan::AlterOwner(_)
        | Plan::Declare(_)
        | Plan::Fetch(_)
        | Plan::Close(_)
        | Plan::ReadThenWrite(_)
        | Plan::Prepare(_)
        | Plan::Execute(_)
        | Plan::Deallocate(_)
        | Plan::Raise(_)
        | Plan::GrantRole(_)
        | Plan::RevokeRole(_)
        | Plan::GrantPrivileges(_)
        | Plan::RevokePrivileges(_)
        | Plan::AlterDefaultPrivileges(_)
        | Plan::ReassignOwned(_)
        | Plan::ValidateConnection(_)
        | Plan::SideEffectingFunc(_) => return TargetCluster::Active,
    };

    // Bail if the user has disabled it via the SessionVar.
    if !session.vars().auto_route_introspection_queries() {
        return TargetCluster::Active;
    }

    // We can't switch what cluster we're using, if the user has specified a replica.
    if session.vars().cluster_replica().is_some() {
        return TargetCluster::Active;
    }

    // These dependencies are just existing dataflows that are referenced in the plan.
    let mut depends_on = depends_on.into_iter().peekable();
    let has_dependencies = depends_on.peek().is_some();

    // Make sure we only depend on the system catalog, and nothing we depend on is a
    // per-replica object, that requires being run a specific replica.
    let valid_dependencies = depends_on.all(|id| {
        let entry = catalog.state().get_entry(&id);
        let schema = &entry.name().qualifiers.schema_spec;

        let system_only = catalog.state().is_system_schema_specifier(schema);
        let non_replica = catalog.state().introspection_dependencies(id).is_empty();

        system_only && non_replica
    });

    if (has_dependencies && valid_dependencies)
        || (!has_dependencies && !could_run_expensive_function)
    {
        let intros_cluster = catalog
            .state()
            .resolve_builtin_cluster(&MZ_INTROSPECTION_CLUSTER);
        tracing::debug!("Running on '{}' cluster", MZ_INTROSPECTION_CLUSTER.name);

        // If we're running on a different cluster than the active one, notify the user.
        if intros_cluster.name != session.vars().cluster() {
            session.add_notice(AdapterNotice::AutoRunOnIntrospectionCluster);
        }
        TargetCluster::Introspection
    } else {
        TargetCluster::Active
    }
}

/// Checks if we're currently running on the [`MZ_INTROSPECTION_CLUSTER`], and if so, do
/// we depend on any objects that we're not allowed to query from the cluster.
pub fn check_cluster_restrictions(
    cluster: &str,
    catalog: &impl SessionCatalog,
    plan: &Plan,
) -> Result<(), AdapterError> {
    // We only impose restrictions if the current cluster is the introspection cluster.
    if cluster != MZ_INTROSPECTION_CLUSTER.name {
        return Ok(());
    }

    // Only continue, and check restrictions, if a Plan would run some computation on the cluster.
    //
    // Note: We get the dependencies from the Plans themselves, because it's only after planning
    // that we actually know what objects we'll need to reference.
    //
    // Note: Creating other objects like Materialized Views is prevented elsewhere. We define the
    // 'mz_introspection' cluster to be "read-only", which restricts these actions.
    let depends_on: Box<dyn Iterator<Item = GlobalId>> = match plan {
        Plan::ReadThenWrite(plan) => Box::new(plan.selection.depends_on().into_iter()),
        Plan::Subscribe(plan) => match plan.from {
            SubscribeFrom::Id(id) => Box::new(std::iter::once(id)),
            SubscribeFrom::Query { ref expr, .. } => Box::new(expr.depends_on().into_iter()),
        },
        Plan::Select(plan) => Box::new(plan.source.depends_on().into_iter()),
        _ => return Ok(()),
    };

    // Collect any items that are not allowed to be run on the introspection cluster.
    let unallowed_dependents: SmallVec<[String; 2]> = depends_on
        .filter_map(|id| {
            let item = catalog.get_item(&id);
            let full_name = catalog.resolve_full_name(item.name());

            if !catalog.is_system_schema(&full_name.schema) {
                Some(full_name.to_string())
            } else {
                None
            }
        })
        .collect();

    // If the query depends on unallowed items, error out.
    if !unallowed_dependents.is_empty() {
        Err(AdapterError::UnallowedOnCluster {
            depends_on: unallowed_dependents,
            cluster: MZ_INTROSPECTION_CLUSTER.name.to_string(),
        })
    } else {
        Ok(())
    }
}
