// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_catalog::memory::error::ErrorKind;
use mz_sql::catalog::CatalogError;
use mz_sql::session::metadata::SessionMetadata;
use tracing::Span;

use crate::command::ExecuteResponse;
use crate::coord::{
    CatalogCommitOps, CatalogCommitStage, Coordinator, Message, PlanValidity, StageResult, Staged,
};
use crate::{AdapterError, AdapterNotice, ExecuteContext, catalog};

impl Staged for CatalogCommitStage {
    type Ctx = ExecuteContext;

    fn validity(&mut self) -> &mut PlanValidity {
        &mut self.validity
    }

    async fn stage(
        mut self,
        coord: &mut Coordinator,
        ctx: &mut ExecuteContext,
    ) -> Result<StageResult<Box<Self>>, AdapterError> {
        // Feature, privilege and name-resolution decisions belong to the original
        // structural snapshot. Metadata retries retain the logical operation.
        if self.planning_revision != coord.catalog().transient_revision() {
            // No attempt has run in this reacquired turn. Replan within it.
            return Err(AdapterError::CatalogSnapshotChanged);
        }
        let ready = coord
            .prepare_catalog_commit_ops(&mut self.ops, ctx.session())
            .await?;
        if self.planning_revision != coord.catalog().transient_revision() {
            // Preparation may have completed an allocation and applied peer DDL.
            // Do not certify the old plan with the caught-up revision.
            coord.release_ddl_lock(ctx.session().conn_id());
            return Err(AdapterError::CatalogSnapshotChanged);
        }
        if !ready {
            return Ok(self.defer(coord, ctx));
        }
        let CatalogCommitOps::Ready(ops) = &self.ops else {
            unreachable!("preparation reported ready");
        };
        let result = coord
            .try_catalog_transact_with_context(ctx, ops, &mut self.prepared)
            .await;
        match result {
            Ok(false) => return Ok(self.defer(coord, ctx)),
            Err(AdapterError::CatalogSnapshotChanged) => {
                coord.release_ddl_lock(ctx.session().conn_id());
                return Err(AdapterError::CatalogSnapshotChanged);
            }
            _ => (),
        }
        // CREATE ROLE reports this on terminal catalog success or error, not on
        // every failed candidate or on a same-statement replan.
        if matches!(self.response, ExecuteResponse::CreatedRole)
            && let Some(notice) = coord.should_emit_rbac_notice(ctx.session())
        {
            ctx.session().add_notice(notice);
        }
        match (result, self.already_exists_notice) {
            (
                Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                    kind: ErrorKind::Sql(CatalogError::DatabaseAlreadyExists(_)),
                })),
                Some(notice @ AdapterNotice::DatabaseAlreadyExists { .. }),
            )
            | (
                Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                    kind: ErrorKind::Sql(CatalogError::SchemaAlreadyExists(_)),
                })),
                Some(notice @ AdapterNotice::SchemaAlreadyExists { .. }),
            ) => ctx.session().add_notice(notice),
            (result, _) => {
                result?;
            }
        }
        Ok(StageResult::Response(self.response))
    }

    fn message(self, ctx: ExecuteContext, span: Span) -> Message {
        Message::CatalogCommitStageReady {
            ctx,
            span,
            stage: self,
        }
    }

    fn cancel_enabled(&self) -> bool {
        true
    }
}

impl CatalogCommitStage {
    fn defer(self, coord: &mut Coordinator, ctx: &ExecuteContext) -> StageResult<Box<Self>> {
        coord.release_ddl_lock(ctx.session().conn_id());
        let delay = coord.read_protection_conflict_delay();
        StageResult::Await(Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(Box::new(self))
        }))
    }
}

impl Coordinator {
    /// Sequences SQL whose remaining work is a catalog transaction and fixed
    /// response. Startup and statements with separate completion effects retain
    /// their own owners.
    pub(crate) async fn sequence_catalog_commit(
        &mut self,
        ctx: ExecuteContext,
        ops: Vec<catalog::Op>,
        response: ExecuteResponse,
    ) {
        self.sequence_catalog_commit_with_exists_notice(ctx, ops, response, None)
            .await;
    }

    /// Converts only the matching terminal namespace-exists error to a notice.
    /// Contention and structural replanning retain the notice without emitting it.
    pub(super) async fn sequence_catalog_commit_with_exists_notice(
        &mut self,
        ctx: ExecuteContext,
        ops: Vec<catalog::Op>,
        response: ExecuteResponse,
        already_exists_notice: Option<AdapterNotice>,
    ) {
        self.sequence_catalog_commit_ops(
            ctx,
            CatalogCommitOps::Ready(ops),
            response,
            already_exists_notice,
        )
        .await;
    }

    pub(super) async fn sequence_catalog_commit_ops(
        &mut self,
        ctx: ExecuteContext,
        ops: CatalogCommitOps,
        response: ExecuteResponse,
        already_exists_notice: Option<AdapterNotice>,
    ) {
        let stage = CatalogCommitStage {
            validity: PlanValidity::new(
                self.catalog(),
                BTreeSet::new(),
                None,
                None,
                ctx.session().role_metadata().clone(),
            ),
            planning_revision: self.catalog().transient_revision(),
            ops,
            prepared: None,
            response,
            already_exists_notice,
        };
        self.sequence_staged(ctx, Span::current(), stage).await;
    }
}
