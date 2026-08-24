// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared helpers for apply commands that reconcile grants/comments on database objects.

use crate::cli::CliError;
use crate::cli::commands::reconcile::{self, ObjectKind, ReconcileTarget};
use crate::cli::executor::DeploymentExecutor;
use crate::client::Client;
use crate::project::ir::compiled;
use crate::project::ir::object_id::ObjectId;

/// Reconcile grants and execute comment statements for one database object.
pub async fn reconcile_grants_and_comments(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    obj_id: &ObjectId,
    typed_obj: &compiled::DatabaseObject,
    kind: ObjectKind,
) -> Result<(), CliError> {
    reconcile::grants(
        client,
        executor,
        &ReconcileTarget::item(kind, obj_id),
        &typed_obj.grants,
    )
    .await?;
    for comment in &typed_obj.comments {
        executor.execute_sql(comment).await?;
    }
    Ok(())
}
