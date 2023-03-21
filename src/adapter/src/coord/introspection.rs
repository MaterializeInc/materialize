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

use mz_ore::collections::HashSet;
use mz_repr::GlobalId;
use mz_sql::catalog::SessionCatalog;
use mz_sql::plan::Plan;
use once_cell::sync::Lazy;
use smallvec::SmallVec;

use crate::catalog::builtin::BuiltinCluster;
use crate::catalog::Catalog;
use crate::rbac;
use crate::session::Session;

use crate::{
    catalog::builtin::{
        INFORMATION_SCHEMA, MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA, MZ_INTROSPECTION_CLUSTER,
        MZ_INTROSPECTION_ROLE, PG_CATALOG_SCHEMA,
    },
    AdapterError,
};

/// The schema's a user is allowed to query from the `mz_introspection` cluster
static ALLOWED_SCHEMAS: Lazy<HashSet<&str>> = Lazy::new(|| {
    HashSet::from([
        MZ_CATALOG_SCHEMA,
        PG_CATALOG_SCHEMA,
        MZ_INTERNAL_SCHEMA,
        INFORMATION_SCHEMA,
    ])
});

/// Checks whether or not we should automatically run a query on the `mz_introspection`
/// cluster, as opposed to whatever the current default cluster is.
pub fn auto_run_on_introspection(
    catalog: &Catalog,
    session: &Session,
    depends_on: impl IntoIterator<Item = GlobalId>,
) -> Option<&'static BuiltinCluster> {
    if !session.vars().force_introspection_cluster() {
        return None;
    }

    depends_on
        .into_iter()
        .all(|id| {
            let entry = catalog.get_entry(&id);
            let full_name = catalog.resolve_full_name(entry.name(), Some(session.conn_id()));
            ALLOWED_SCHEMAS.contains(full_name.schema.as_str())
        })
        .then_some(&MZ_INTROSPECTION_CLUSTER)
}

/// Checks if we're currently running on the [`MZ_INTROSPECTION_CLUSTER`], and if so, do
/// we depend on any objects that we're not allowed to query from the cluster.
pub fn check_cluster_restrictions(
    catalog: &impl SessionCatalog,
    plan: &Plan,
    depends_on: &Vec<GlobalId>,
) -> Result<(), AdapterError> {
    // We only impose restrictions if the current cluster is the introspection cluster.
    let cluster = catalog.active_cluster();
    if cluster != MZ_INTROSPECTION_CLUSTER.name {
        return Ok(());
    }

    // Allows explain queries.
    if let Plan::Explain(_) = plan {
        return Ok(());
    }

    // Collect any items that are not allowed to be run on the introspection cluster.
    let unallowed_dependents: SmallVec<[String; 2]> = depends_on
        .iter()
        .filter_map(|id| {
            let item = catalog.get_item(id);
            let full_name = catalog.resolve_full_name(item.name());

            if !ALLOWED_SCHEMAS.contains(full_name.schema.as_str()) {
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
        | Plan::DropDatabase(_)
        | Plan::DropSchema(_)
        | Plan::DropRoles(_)
        | Plan::DropClusters(_)
        | Plan::DropClusterReplicas(_)
        | Plan::DropItems(_)
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
        | Plan::ReadThenWrite(_)
        | Plan::Raise(_)
        | Plan::RotateKeys(_)
        | Plan::GrantRole(_)
        | Plan::RevokeRole(_)
        | Plan::CopyRows(_) => {
            return Err(AdapterError::Unauthorized(
                rbac::UnauthorizedError::privilege(plan.name().to_string(), None),
            ))
        }
    }

    for id in depends_on {
        let item = catalog.get_item(id);
        let full_name = catalog.resolve_full_name(item.name());
        if !ALLOWED_SCHEMAS.contains(full_name.schema.as_str()) {
            return Err(AdapterError::Unauthorized(
                rbac::UnauthorizedError::privilege(
                    format!("interact with object {full_name}"),
                    None,
                ),
            ));
        }
    }

    Ok(())
}
