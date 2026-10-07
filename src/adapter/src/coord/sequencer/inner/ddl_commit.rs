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
use crate::coord::{
    Coordinator, DdlCommitContext, DdlCommitStage, Message, PlanValidity, StageResult, Staged,
    StagedContext,
};
use crate::session::{DdlSideEffect, EndTransactionAction, Session};
use crate::{AdapterError, ExecuteContext, catalog};

#[cfg(test)]
#[path = "ddl_commit_tests.rs"]
mod tests;

impl StagedContext for DdlCommitContext {
    fn session(&self) -> Option<&Session> {
        Some(self.0.session())
    }

    fn retire(mut self, result: Result<ExecuteResponse, AdapterError>) {
        // Session transaction extraction happened once before entering this stage.
        // Variable completion still belongs to the final outcome, including cancel.
        let action = if result.is_ok() {
            EndTransactionAction::Commit
        } else {
            EndTransactionAction::Rollback
        };
        let changed = self.0.session_mut().vars_mut().end_transaction(action);
        let result = result.map(|response| {
            let ExecuteResponse::TransactionCommitted { mut params } = response else {
                unreachable!("DDL COMMIT only returns transaction completion")
            };
            params.extend(changed);
            ExecuteResponse::TransactionCommitted { params }
        });
        self.0.retire(result);
    }
}

impl Staged for DdlCommitStage {
    type Ctx = DdlCommitContext;

    fn validity(&mut self) -> &mut PlanValidity {
        &mut self.validity
    }

    fn check_validity(&mut self, catalog: &crate::catalog::Catalog) -> Result<(), AdapterError> {
        if self.planning_revision != catalog.transient_revision() {
            return Err(AdapterError::DDLTransactionRace);
        }
        self.validity.check(catalog)
    }

    async fn stage(
        mut self,
        coord: &mut Coordinator,
        ctx: &mut DdlCommitContext,
    ) -> Result<StageResult<Box<Self>>, AdapterError> {
        // An explicit transaction cannot replan the individual statements whose
        // successful responses have already been sent.
        self.check_validity(coord.catalog())?;
        let committed = coord
            .try_catalog_transact_with_side_effects(
                &mut ctx.0,
                &self.ops,
                &mut self.prepared,
                &mut self.side_effects,
            )
            .await
            .map_err(|error| match error {
                AdapterError::CatalogSnapshotChanged => AdapterError::DDLTransactionRace,
                error => error,
            })?;
        if !committed {
            let delay = coord.read_protection_conflict_delay();
            return Ok(StageResult::Await(Box::pin(async move {
                tokio::time::sleep(delay).await;
                Ok(Box::new(self))
            })));
        }
        Ok(StageResult::Response(
            ExecuteResponse::TransactionCommitted {
                params: Default::default(),
            },
        ))
    }

    fn message(self, ctx: DdlCommitContext, span: Span) -> Message {
        Message::DdlCommitStageReady {
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
    pub(crate) async fn sequence_ddl_commit(
        &mut self,
        ctx: ExecuteContext,
        ops: Vec<catalog::Op>,
        side_effects: Vec<DdlSideEffect>,
        planning_revision: u64,
    ) {
        let stage = DdlCommitStage {
            validity: PlanValidity::new(
                self.catalog(),
                BTreeSet::new(),
                None,
                None,
                ctx.session().role_metadata().clone(),
            ),
            planning_revision,
            ops,
            prepared: None,
            side_effects,
        };
        self.sequence_staged(DdlCommitContext(ctx), Span::current(), stage)
            .await;
    }
}
