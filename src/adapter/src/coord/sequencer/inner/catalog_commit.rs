// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_sql::session::metadata::SessionMetadata;
use tracing::Span;

use crate::command::ExecuteResponse;
use crate::coord::{CatalogCommitStage, Coordinator, Message, PlanValidity, StageResult, Staged};
use crate::{AdapterError, ExecuteContext, catalog};

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
            coord.release_ddl_lock(ctx.session().conn_id());
            return Err(AdapterError::CatalogSnapshotChanged);
        }
        let result = coord
            .try_catalog_transact_with_context(ctx, &self.ops, &mut self.prepared)
            .await;
        match result {
            Ok(false) => {
                let delay = coord.read_protection_conflict_delay();
                return Ok(StageResult::Await(Box::pin(async move {
                    tokio::time::sleep(delay).await;
                    Ok(Box::new(self))
                })));
            }
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
        result?;
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
        };
        self.sequence_staged(ctx, Span::current(), stage).await;
    }
}
