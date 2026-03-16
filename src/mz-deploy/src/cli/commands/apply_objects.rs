//! Shared helpers for apply commands that reconcile grants/comments on database objects.

use crate::cli::CliError;
use crate::cli::commands::grants;
use crate::cli::executor::DeploymentExecutor;
use crate::client::Client;
use crate::project::object_id::ObjectId;
use crate::project::typed;

/// Reconcile grants and execute comment statements for one database object.
pub async fn reconcile_grants_and_comments(
    client: &Client,
    executor: &DeploymentExecutor<'_>,
    obj_id: &ObjectId,
    typed_obj: &typed::DatabaseObject,
    grant_kind: &grants::GrantObjectKind,
) -> Result<(), CliError> {
    grants::reconcile(client, executor, obj_id, &typed_obj.grants, grant_kind).await?;
    for comment in &typed_obj.comments {
        executor.execute_sql(comment).await?;
    }
    Ok(())
}
