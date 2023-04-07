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
//! `mz_introspection` role, which can be used by support teams to diagnose a deployment.
//! For each of these use cases, we have some special restrictions we want to apply. The
//! logic around these restrictions is defined here.
//!
//!
//! [`mz_introspection`]: https://materialize.com/docs/sql/show-clusters/#mz_introspection-system-cluster

use mz_repr::GlobalId;
use mz_sql::catalog::SessionCatalog;
use mz_sql::plan::Plan;

use crate::catalog::{Catalog, Cluster};
use crate::notice::AdapterNotice;
use crate::rbac;
use crate::session::Session;

use crate::{
    catalog::builtin::{MZ_INTROSPECTION_CLUSTER, MZ_INTROSPECTION_ROLE},
    AdapterError,
};

/// Checks whether or not we should automatically run a query on the `mz_introspection`
/// cluster, as opposed to whatever the current default cluster is.
pub fn auto_run_on_introspection<'a, 's>(
    catalog: &'a Catalog,
    session: &'s mut Session,
    depends_on: impl IntoIterator<Item = GlobalId>,
) -> Option<&'a Cluster> {
    // We consider the feature to be enabled if we're in unsafe mode (e.g. testing) or
    // the LaunchDarkly flag is set (e.g. in prod).
    let feature_enabled = catalog.unsafe_mode()
        || catalog
            .system_config()
            .enable_auto_route_introspection_queries();

    // Bail if the feature isn't enabled, or the user has disabled it via the SessionVar.
    if !feature_enabled || !session.vars().auto_route_introspection_queries() {
        return None;
    }

    // We can't switch what cluster we're using, if the user has specified a replica.
    if session.vars().cluster_replica().is_some() {
        return None;
    }

    // Check to make sure our iterator contains atleast one element, this prevents us
    // from always running empty queries on the mz_introspection cluster.
    let mut depends_on = depends_on.into_iter().peekable();
    let non_empty = depends_on.peek().is_some();

    // Make sure we only depend on the system catalog, and nothing we depend on is a
    // per-replica object, that requires being run a specific replica.
    let valid_dependencies = depends_on.all(|id| {
        let entry = catalog.get_entry(&id);
        let schema = &entry.name().qualifiers.schema_spec;

        let system_only = catalog.state().is_system_schema_specifier(schema);
        let non_replica = catalog.introspection_dependencies(id).is_empty();

        system_only && non_replica
    });

    if non_empty && valid_dependencies {
        let intros_cluster = catalog.resolve_builtin_cluster(&MZ_INTROSPECTION_CLUSTER);
        tracing::debug!("Running on '{}' cluster", MZ_INTROSPECTION_CLUSTER.name);

        // If we're running on a different cluster than the active one, notify the user.
        if intros_cluster.name != session.vars().cluster() {
            session.add_notice(AdapterNotice::AutoRunOnIntrospectionCluster);
        }
        Some(intros_cluster)
    } else {
        None
    }
}

/// TODO(jkosh44) This function will verify the privileges for the mz_introspection user.
///  All of the privileges are hard coded into this function. In the future if we ever add
///  a more robust privileges framework, then this function should be replaced with that
///  framework.
pub fn user_privilege_hack(
    catalog: &impl SessionCatalog,
    session: &Session,
    plan: &Plan,
    depends_on: &Vec<GlobalId>,
) -> Result<(), AdapterError> {
    if session.user().name != MZ_INTROSPECTION_ROLE.name {
        return Ok(());
    }

    match plan {
        // **Special Cases**
        //
        // Generally we want to prevent the mz_introspection user from being able to
        // access user objects. But there are a few special cases where we permit
        // limited access, which are:
        //   * SHOW CREATE ... commands, which are very useful for debugging, see
        //     <https://github.com/MaterializeInc/materialize/issues/18027> for more
        //     details.
        //
        Plan::ShowCreate(_) => {
            return Ok(());
        }

        Plan::Subscribe(_)
        | Plan::Peek(_)
        | Plan::CopyFrom(_)
        | Plan::SendRows(_)
        | Plan::Explain(_)
        | Plan::ShowAllVariables
        | Plan::ShowVariable(_)
        | Plan::SetVariable(_)
        | Plan::ResetVariable(_)
        | Plan::StartTransaction(_)
        | Plan::CommitTransaction(_)
        | Plan::AbortTransaction(_)
        | Plan::EmptyQuery
        | Plan::Declare(_)
        | Plan::Fetch(_)
        | Plan::Close(_)
        | Plan::Prepare(_)
        | Plan::Execute(_)
        | Plan::Deallocate(_) => {}

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
        | Plan::DiscardTemp
        | Plan::DiscardAll
        | Plan::DropObjects(_)
        | Plan::Insert(_)
        | Plan::AlterNoop(_)
        | Plan::AlterIndexSetOptions(_)
        | Plan::AlterIndexResetOptions(_)
        | Plan::AlterRole(_)
        | Plan::AlterSink(_)
        | Plan::AlterSource(_)
        | Plan::AlterItemRename(_)
        | Plan::AlterSecret(_)
        | Plan::AlterSystemSet(_)
        | Plan::AlterSystemReset(_)
        | Plan::AlterSystemResetAll(_)
        | Plan::AlterOwner(_)
        | Plan::ReadThenWrite(_)
        | Plan::Raise(_)
        | Plan::RotateKeys(_)
        | Plan::GrantRole(_)
        | Plan::RevokeRole(_)
        | Plan::CopyRows(_) => {
            return Err(AdapterError::Unauthorized(
                rbac::UnauthorizedError::Privilege {
                    action: plan.name().to_string(),
                    reason: None,
                },
            ))
        }
    }

    for id in depends_on {
        let item = catalog.get_item(id);
        let full_name = catalog.resolve_full_name(item.name());
        if !catalog.is_system_schema(&full_name.schema) {
            return Err(AdapterError::Unauthorized(
                rbac::UnauthorizedError::Privilege {
                    action: format!("interact with object {full_name}"),
                    reason: None,
                },
            ));
        }
    }

    Ok(())
}
