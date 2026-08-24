// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The live catalog state that grant and comment reconciliation compares a
//! project against, read in bulk ahead of the objects that consume it.

use crate::cli::CliError;
use crate::cli::commands::comments::{self, CommentObject};
use crate::cli::commands::grants::{self, GrantNamedObjectKind, GrantObjectKind};
use crate::cli::executor::DeploymentExecutor;
use crate::client::{Client, ObjectComment, ObjectGrant};
use crate::project::ir::compiled;
use crate::project::ir::object_id::ObjectId;
use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};

/// The grants and comments the catalog records for a set of objects.
///
/// Reconciliation decides what to emit one object at a time, but each statement
/// it would take to read one object's state costs a full network round trip,
/// while the catalog relations behind that state are small. So the state for a
/// whole phase is read up front, three statements for any number of objects,
/// and reconciliation then runs without touching the network. The three are
/// issued together so the connection pipelines them and the whole read costs one
/// round trip rather than three.
///
/// An apply phase may plan against state read once at the start of the phase
/// because planning emits no DDL: every statement it produces is collected for
/// [`ApplyPlan::execute`] to run later, so nothing planning does can invalidate
/// what was read.
///
/// `K` is how the caller names its objects: [`ObjectId`] for schema-qualified
/// objects, `String` for named ones (clusters, network policies).
///
/// [`ApplyPlan::execute`]: crate::cli::executor::ApplyPlan::execute
pub struct ReconcileState<K: Ord> {
    grants: BTreeMap<K, Vec<ObjectGrant>>,
    default_privileges: BTreeMap<K, Vec<ObjectGrant>>,
    comments: BTreeMap<K, Vec<ObjectComment>>,
}

impl<K: Ord> ReconcileState<K> {
    /// The privileges the catalog records against `key`.
    pub fn grants<Q>(&self, key: &Q) -> &[ObjectGrant]
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        slice(&self.grants, key)
    }

    /// The privileges an `ALTER DEFAULT PRIVILEGES` rule would re-apply to
    /// `key`, which reconciliation must not revoke.
    pub fn default_privileges<Q>(&self, key: &Q) -> &[ObjectGrant]
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        slice(&self.default_privileges, key)
    }

    /// The comments the catalog records against `key`, including on its columns.
    pub fn comments<Q>(&self, key: &Q) -> &[ObjectComment]
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        slice(&self.comments, key)
    }
}

/// An object the bulk read returned no rows for carries no state, which is the
/// same answer as an empty list.
fn slice<'a, K, Q, V>(map: &'a BTreeMap<K, Vec<V>>, key: &Q) -> &'a [V]
where
    K: Ord + Borrow<Q>,
    Q: Ord + ?Sized,
{
    map.get(key).map_or(&[], Vec::as_slice)
}

impl ReconcileState<ObjectId> {
    /// Read the state for a set of schema-qualified objects of one kind.
    pub async fn for_database_objects(
        client: &Client,
        kind: &GrantObjectKind,
        objects: &BTreeSet<ObjectId>,
    ) -> Result<Self, CliError> {
        let introspection = client.introspection();
        let catalog_table = kind.catalog_table();
        let (grants, default_privileges, comments) = futures::try_join!(
            introspection.get_database_object_grants(catalog_table, objects),
            introspection.get_default_privilege_grants_for_database_objects(
                catalog_table,
                objects,
                kind.object_type_str(),
            ),
            introspection.get_database_object_comments(catalog_table, objects),
        )
        .map_err(CliError::Connection)?;

        Ok(Self {
            grants,
            default_privileges,
            comments,
        })
    }
}

impl ReconcileState<String> {
    /// Read the state for a set of named infrastructure objects of one kind.
    pub async fn for_named_objects(
        client: &Client,
        kind: &GrantNamedObjectKind,
        names: &[&str],
    ) -> Result<Self, CliError> {
        let introspection = client.introspection();
        let privileges = async {
            match kind {
                GrantNamedObjectKind::Cluster => futures::try_join!(
                    introspection.get_cluster_grants(names),
                    introspection.get_default_privilege_grants_for_clusters(names),
                ),
                GrantNamedObjectKind::NetworkPolicy => futures::try_join!(
                    introspection.get_network_policy_grants(names),
                    introspection.get_default_privilege_grants_for_network_policies(names),
                ),
            }
        };
        let ((grants, default_privileges), comments) = futures::try_join!(
            privileges,
            introspection.get_named_object_comments(kind.catalog_table(), names),
        )
        .map_err(CliError::Connection)?;

        Ok(Self {
            grants,
            default_privileges,
            comments,
        })
    }
}

/// Reconcile grants and comments for one database object.
pub async fn grants_and_comments(
    executor: &DeploymentExecutor<'_>,
    obj_id: &ObjectId,
    typed_obj: &compiled::DatabaseObject,
    grant_kind: &GrantObjectKind,
    state: &ReconcileState<ObjectId>,
) -> Result<(), CliError> {
    grants::reconcile(executor, obj_id, &typed_obj.grants, grant_kind, state).await?;
    let comment_object = CommentObject::for_grant_kind(grant_kind, obj_id);
    comments::reconcile(
        executor,
        &comment_object,
        &typed_obj.comments,
        state.comments(obj_id),
    )
    .await
}
