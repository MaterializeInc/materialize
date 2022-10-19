// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::catalog::builtin::MZ_SYSTEM_COMPUTE_INSTANCE;
use crate::catalog::Catalog;
use mz_compute_client::controller::ComputeInstanceId;
use mz_sql::names::{ObjectQualifiers, RawDatabaseSpecifier};
use mz_sql::plan::Plan;
use mz_stash::Append;
use std::collections::{HashMap, HashSet};

/// Describes the permission of a particular user.
#[derive(Debug, Clone)]
pub enum UserPermissions {
    /// User has no restrictions.
    All,
    /// User is only allowed to do what is defined in permissions.
    AllowList(Permissions),
    /// User is blocked from doing anything defined in permissions.
    BlockList(Permissions),
}

/// A specific set of permissions.
#[derive(Debug, Clone)]
pub struct Permissions {
    pub(crate) compute_instances: HashSet<String>,
    pub(crate) schemas: HashMap<RawDatabaseSpecifier, HashSet<String>>,
    pub(crate) action: HashSet<UserAction>,
}

/// Actions that a user can take.
#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub(crate) enum UserAction {
    Read,
    Write,
    Create,
    Drop,
    Alter,
    Show,
    Set,
    AlterSystem,
}

impl Default for UserPermissions {
    // default permission is able to do anything except interact with mz_system cluster.
    fn default() -> Self {
        Self::BlockList(Permissions {
            compute_instances: HashSet::from_iter([MZ_SYSTEM_COMPUTE_INSTANCE.name.to_string()]),
            schemas: HashMap::new(),
            action: HashSet::new(),
        })
    }
}

impl UserPermissions {
    /// Returns whether a user has any restrictions.
    pub fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    /// Returns whether a user is allowed to interact with the compute instance `cluster`.
    pub fn is_allowed_compute_instance(&self, cluster: &str) -> bool {
        match self {
            Self::All => true,
            Self::AllowList(permissions) => permissions.contains_compute_instance(cluster),

            Self::BlockList(permissions) => !permissions.contains_compute_instance(cluster),
        }
    }

    /// Returns whether a user is allowed to interact with the schema `schema` in database `database`.
    pub fn is_allowed_schema(&self, database: &RawDatabaseSpecifier, schema: &str) -> bool {
        match self {
            Self::All => true,
            Self::AllowList(permissions) => permissions.contains_schema(database, schema),
            Self::BlockList(permissions) => !permissions.contains_schema(database, schema),
        }
    }

    /// Returns whether user is allowed to execute `plan`.
    pub fn is_allowed_plan(&self, plan: &Plan) -> bool {
        match UserAction::from_plan(plan) {
            Some(action) => match self {
                Self::All => true,
                Self::AllowList(permissions) => permissions.contains_action(&action),
                Self::BlockList(permission) => !permission.contains_action(&action),
            },
            None => true,
        }
    }
}

impl Permissions {
    fn contains_compute_instance(&self, cluster: &str) -> bool {
        self.compute_instances.contains(cluster)
    }

    fn contains_schema(&self, database: &RawDatabaseSpecifier, schema: &str) -> bool {
        self.schemas
            .get(database)
            .map(|schemas| schemas.contains(schema))
            .unwrap_or(false)
    }

    fn contains_action(&self, action: &UserAction) -> bool {
        self.action.contains(action)
    }
}

impl UserAction {
    fn from_plan(plan: &Plan) -> Option<Self> {
        match plan {
            Plan::Peek(_) | Plan::Explain(_) | Plan::SendRows(_) | Plan::Subscribe(_) => {
                Some(Self::Read)
            }
            Plan::CreateConnection(_)
            | Plan::CreateDatabase(_)
            | Plan::CreateSchema(_)
            | Plan::CreateRole(_)
            | Plan::CreateComputeInstance(_)
            | Plan::CreateComputeReplica(_)
            | Plan::CreateSource(_)
            | Plan::CreateSecret(_)
            | Plan::CreateSink(_)
            | Plan::CreateTable(_)
            | Plan::CreateView(_)
            | Plan::CreateMaterializedView(_)
            | Plan::CreateIndex(_)
            | Plan::CreateType(_) => Some(Self::Create),
            Plan::DiscardTemp
            | Plan::DiscardAll
            | Plan::DropDatabase(_)
            | Plan::DropSchema(_)
            | Plan::DropRoles(_)
            | Plan::DropComputeInstances(_)
            | Plan::DropComputeReplicas(_)
            | Plan::DropItems(_) => Some(Self::Drop),
            Plan::ShowAllVariables | Plan::ShowVariable(_) => Some(Self::Show),
            Plan::SetVariable(_) | Plan::ResetVariable(_) => Some(Self::Set),
            Plan::SendDiffs(_) | Plan::CopyFrom(_) | Plan::ReadThenWrite(_) | Plan::Insert(_) => {
                Some(Self::Write)
            }
            Plan::AlterNoop(_)
            | Plan::AlterIndexSetOptions(_)
            | Plan::AlterIndexResetOptions(_)
            | Plan::AlterSink(_)
            | Plan::AlterSource(_)
            | Plan::AlterItemRename(_)
            | Plan::AlterSecret(_)
            | Plan::RotateKeys(_) => Some(Self::Alter),
            Plan::AlterSystemSet(_) | Plan::AlterSystemReset(_) | Plan::AlterSystemResetAll(_) => {
                Some(Self::AlterSystem)
            }
            Plan::StartTransaction(_)
            | Plan::CommitTransaction
            | Plan::AbortTransaction
            | Plan::EmptyQuery
            | Plan::Declare(_)
            | Plan::Fetch(_)
            | Plan::Close(_)
            | Plan::Prepare(_)
            | Plan::Execute(_)
            | Plan::Deallocate(_)
            | Plan::Raise(_) => None,
        }
    }

    fn uses_compute_instance(&self) -> bool {
        match self {
            Self::Read | Self::Write | Self::Create | Self::Drop | Self::Alter => true,
            Self::Show | Self::Set | Self::AlterSystem => false,
        }
    }
}

/// Returns whether the plan might use a compute instance.
pub fn uses_compute_instance(plan: &Plan) -> bool {
    match UserAction::from_plan(plan) {
        Some(action) => action.uses_compute_instance(),
        None => false,
    }
}

/// Returns the target compute instance from a plan if one exists.
pub fn extract_target_compute_instance(plan: &Plan) -> Option<&ComputeInstanceId> {
    match plan {
        Plan::CreateMaterializedView(mv) => Some(&mv.materialized_view.compute_instance),
        Plan::CreateIndex(index) => Some(&index.index.compute_instance),
        Plan::CreateConnection(_)
        | Plan::CreateDatabase(_)
        | Plan::CreateSchema(_)
        | Plan::CreateRole(_)
        | Plan::CreateComputeInstance(_)
        | Plan::CreateComputeReplica(_)
        | Plan::CreateSource(_)
        | Plan::CreateSecret(_)
        | Plan::CreateSink(_)
        | Plan::CreateTable(_)
        | Plan::CreateView(_)
        | Plan::CreateType(_)
        | Plan::DiscardTemp
        | Plan::DiscardAll
        | Plan::DropDatabase(_)
        | Plan::DropSchema(_)
        | Plan::DropRoles(_)
        | Plan::DropComputeInstances(_)
        | Plan::DropComputeReplicas(_)
        | Plan::DropItems(_)
        | Plan::EmptyQuery
        | Plan::ShowAllVariables
        | Plan::ShowVariable(_)
        | Plan::SetVariable(_)
        | Plan::ResetVariable(_)
        | Plan::StartTransaction(_)
        | Plan::CommitTransaction
        | Plan::AbortTransaction
        | Plan::Peek(_)
        | Plan::Subscribe(_)
        | Plan::SendRows(_)
        | Plan::CopyFrom(_)
        | Plan::Explain(_)
        | Plan::SendDiffs(_)
        | Plan::Insert(_)
        | Plan::AlterNoop(_)
        | Plan::AlterIndexSetOptions(_)
        | Plan::AlterIndexResetOptions(_)
        | Plan::AlterSink(_)
        | Plan::AlterSource(_)
        | Plan::AlterItemRename(_)
        | Plan::AlterSecret(_)
        | Plan::AlterSystemSet(_)
        | Plan::AlterSystemReset(_)
        | Plan::AlterSystemResetAll(_)
        | Plan::Declare(_)
        | Plan::Fetch(_)
        | Plan::Close(_)
        | Plan::ReadThenWrite(_)
        | Plan::Prepare(_)
        | Plan::Execute(_)
        | Plan::Deallocate(_)
        | Plan::Raise(_)
        | Plan::RotateKeys(_) => None,
    }
}

/// Returns the target schema from a plan if one exists.
pub fn extract_target_schema<'a, S: Append>(
    plan: &'a Plan,
    catalog: &'a Catalog<S>,
) -> Option<Vec<&'a ObjectQualifiers>> {
    match plan {
        Plan::CreateConnection(plan) => Some(vec![&plan.name.qualifiers]),
        Plan::CreateSource(plan) => Some(vec![&plan.name.qualifiers]),
        Plan::CreateSecret(plan) => Some(vec![&plan.name.qualifiers]),
        Plan::CreateSink(plan) => Some(vec![&plan.name.qualifiers]),
        Plan::CreateTable(plan) => Some(vec![&plan.name.qualifiers]),
        Plan::CreateView(plan) => Some(vec![&plan.name.qualifiers]),
        Plan::CreateMaterializedView(plan) => Some(vec![&plan.name.qualifiers]),
        Plan::CreateIndex(plan) => Some(vec![&plan.name.qualifiers]),
        Plan::CreateType(plan) => Some(vec![&plan.name.qualifiers]),
        Plan::DropItems(plan) => Some(
            plan.items
                .iter()
                .map(|id| &catalog.get_entry(id).name().qualifiers)
                .collect(),
        ),
        Plan::AlterIndexSetOptions(plan) => {
            Some(vec![&catalog.get_entry(&plan.id).name().qualifiers])
        }
        Plan::AlterIndexResetOptions(plan) => {
            Some(vec![&catalog.get_entry(&plan.id).name().qualifiers])
        }
        Plan::AlterSink(plan) => Some(vec![&catalog.get_entry(&plan.id).name().qualifiers]),
        Plan::AlterSource(plan) => Some(vec![&catalog.get_entry(&plan.id).name().qualifiers]),
        Plan::AlterItemRename(plan) => Some(vec![&catalog.get_entry(&plan.id).name().qualifiers]),
        Plan::AlterSecret(plan) => Some(vec![&catalog.get_entry(&plan.id).name().qualifiers]),
        Plan::CreateDatabase(_)
        | Plan::CreateSchema(_)
        | Plan::CreateRole(_)
        | Plan::CreateComputeInstance(_)
        | Plan::CreateComputeReplica(_)
        | Plan::DiscardTemp
        | Plan::DiscardAll
        | Plan::DropDatabase(_)
        | Plan::DropSchema(_)
        | Plan::DropRoles(_)
        | Plan::DropComputeInstances(_)
        | Plan::DropComputeReplicas(_)
        | Plan::EmptyQuery
        | Plan::ShowAllVariables
        | Plan::ShowVariable(_)
        | Plan::SetVariable(_)
        | Plan::ResetVariable(_)
        | Plan::StartTransaction(_)
        | Plan::CommitTransaction
        | Plan::AbortTransaction
        | Plan::Peek(_)
        | Plan::Subscribe(_)
        | Plan::SendRows(_)
        | Plan::CopyFrom(_)
        | Plan::Explain(_)
        | Plan::SendDiffs(_)
        | Plan::Insert(_)
        | Plan::AlterNoop(_)
        | Plan::AlterSystemSet(_)
        | Plan::AlterSystemReset(_)
        | Plan::AlterSystemResetAll(_)
        | Plan::Declare(_)
        | Plan::Fetch(_)
        | Plan::Close(_)
        | Plan::ReadThenWrite(_)
        | Plan::Prepare(_)
        | Plan::Execute(_)
        | Plan::Deallocate(_)
        | Plan::Raise(_)
        | Plan::RotateKeys(_) => None,
    }
}
