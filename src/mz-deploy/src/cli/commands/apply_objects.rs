// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared helpers for apply commands that reconcile grants and comments on
//! database objects.

use crate::cli::CliError;
use crate::cli::commands::reconcile::{ObjectKind, ReconcileTarget};
use crate::cli::commands::{comments, grants};
use crate::cli::executor::DeploymentExecutor;
use crate::client::{Client, ObjectComment};
use crate::project::ir::compiled;
use crate::project::ir::object_id::ObjectId;

/// Reconcile grants and comments for one database object against `current_comments`,
/// the comments the catalog holds for it.
pub async fn reconcile_grants_and_comments(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    obj_id: &ObjectId,
    typed_obj: &compiled::DatabaseObject,
    kind: ObjectKind,
    current_comments: &[ObjectComment],
) -> Result<(), CliError> {
    let target = ReconcileTarget::item(kind, obj_id);
    grants::reconcile(client, executor, &target, &typed_obj.grants).await?;
    comments::reconcile(executor, &target, &typed_obj.comments, current_comments).await
}
