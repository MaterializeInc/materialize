// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Serializes a plain persistent view commit while allowing the coordinator to serve reads.
//!
//! The worker owns only the durable transaction. The coordinator retains the execution
//! context and DDL guard, and publishes before any competing catalog mutation or fresh
//! DDL snapshot. A join can block the loop, but the worker never needs the loop to finish.

use std::sync::Arc;
use std::time::Instant;

use derivative::Derivative;
use mz_sql::session::metadata::SessionMetadata;

use mz_catalog::memory::error::{Error, ErrorKind};
use mz_catalog::memory::objects::CatalogItem;
use mz_ore::metrics::MetricsFutureExt;
use mz_ore::task::{self, JoinHandle};
use mz_repr::CatalogItemId;
use mz_sql::catalog::CatalogError;
use tokio::sync::mpsc;
use tracing::Instrument;

use super::{Coordinator, ExecuteContext, Message, StageResult};
use crate::catalog::{self, Catalog, CatalogSession, TransactionResult, UnpublishedTransaction};
use crate::coord::CreateViewStage;
use crate::{AdapterError, AdapterNotice, ExecuteResponse};

#[derive(Derivative)]
#[derivative(Debug)]
pub(super) struct PendingViewCommit {
    id: CatalogItemId,
    #[derivative(Debug = "ignore")]
    base: Arc<Catalog>,
    #[derivative(Debug = "ignore")]
    worker: JoinHandle<(Result<UnpublishedTransaction, AdapterError>, Instant)>,
    ctx: ExecuteContext,
    name: String,
    if_not_exists: bool,
    started: Instant,
}

// A panicking worker must also wake the loop so joining propagates the panic instead
// of stranding the context and DDL guard. The notification does not own the result.
struct NotifyViewCommit {
    tx: mpsc::UnboundedSender<Message>,
    id: CatalogItemId,
}

impl Drop for NotifyViewCommit {
    fn drop(&mut self) {
        let _ = self.tx.send(Message::ViewCommitReady(self.id));
    }
}

fn view_commit_test_barrier(name: &'static str) {
    fail::fail_point!(name);
}

impl Coordinator {
    pub(super) fn can_commit_view_off_coordinator(&self, ctx: &ExecuteContext) -> bool {
        self.pending_view_commit.is_none()
            && !ctx.session().transaction().is_ddl()
            && ctx.finish_ddl_on_coordinator
            && self
                .active_conns
                .get(ctx.session().conn_id())
                .is_some_and(|conn| conn.deferred_lock.is_some())
    }

    pub(super) async fn start_view_commit(
        &mut self,
        ctx: &ExecuteContext,
        id: CatalogItemId,
        name: String,
        if_not_exists: bool,
        ops: Vec<catalog::Op>,
    ) -> Result<StageResult<Box<CreateViewStage>>, AdapterError> {
        assert!(self.can_commit_view_off_coordinator(ctx));
        assert!(matches!(
            ops.as_slice(),
            [catalog::Op::DropObjects { objects, .. }, catalog::Op::CreateItem {
                item: CatalogItem::View(view), ..
            }] if objects.is_empty() && view.conn_id.is_none()
        ));
        if self.controller.read_only() {
            return Err(AdapterError::ReadOnly);
        }
        let started = Instant::now();
        let phase_seconds = self.metrics.catalog_transact_phase_seconds.clone();
        let validation = self
            .catalog()
            .state()
            .validate_resource_limits(&ops, ctx.session().conn_id());
        phase_seconds
            .with_label_values(&["prep"])
            .observe(started.elapsed().as_secs_f64());
        validation?;
        let oracle_write_ts = self
            .get_catalog_write_ts()
            .wall_time()
            .observe(phase_seconds.with_label_values(&["write_ts"]))
            .await;
        let base = Arc::clone(&self.catalog);
        let worker_base = Arc::clone(&base);
        let session = CatalogSession::from(
            self.active_conns
                .get(ctx.session().conn_id())
                .expect("connection must exist"),
        );
        let mut storage_collections = Arc::clone(&self.controller.storage_collections);
        let notify = NotifyViewCommit {
            tx: self.internal_cmd_tx.clone(),
            id,
        };
        let queued = Instant::now();
        let worker = task::spawn(
            || "catalog_view_commit",
            async move {
                let _notify = notify;
                phase_seconds
                    .with_label_values(&["candidate_queue"])
                    .observe(queued.elapsed().as_secs_f64());
                view_commit_test_barrier("view_commit_before_transaction");
                let start = Instant::now();
                let result = worker_base
                    .transact_unpublished(
                        Some(&mut storage_collections),
                        oracle_write_ts,
                        Some(&session),
                        ops,
                    )
                    .await;
                phase_seconds
                    .with_label_values(&["transact"])
                    .observe(start.elapsed().as_secs_f64());
                view_commit_test_barrier("view_commit_after_transaction");
                (result, Instant::now())
            }
            .instrument(tracing::Span::current()),
        );
        Ok(StageResult::Deferred(Box::new(move |coord, ctx| {
            // Cancellation must not discard a possibly committed candidate or release its guard.
            coord
                .connection_cancel_watches
                .remove(ctx.session().conn_id());
            assert!(coord.pending_view_commit.is_none());
            coord.pending_view_commit = Some(PendingViewCommit {
                id,
                base,
                worker,
                ctx,
                name,
                if_not_exists,
                started,
            });
        })))
    }

    pub(super) async fn handle_view_commit_ready(&mut self, id: CatalogItemId) {
        // A mutation may have joined this worker before its notification was dequeued.
        if self
            .pending_view_commit
            .as_ref()
            .is_some_and(|p| p.id == id)
        {
            self.settle_view_commit().await;
        }
    }

    /// Publishes an in-flight view commit before its base can be mutated or snapshotted.
    pub(super) async fn settle_view_commit(&mut self) {
        let Some(PendingViewCommit {
            base,
            worker,
            mut ctx,
            name,
            if_not_exists,
            started,
            ..
        }) = self.pending_view_commit.take()
        else {
            return;
        };
        let phase_seconds = self.metrics.catalog_transact_phase_seconds.clone();
        let join_started = Instant::now();
        view_commit_test_barrier("view_commit_join");
        let (result, completed) = worker.await;
        phase_seconds
            .with_label_values(&["candidate_join_wait"])
            .observe(join_started.elapsed().as_secs_f64());
        phase_seconds
            .with_label_values(&["candidate_publication_queue"])
            .observe(completed.elapsed().as_secs_f64());
        // Holding `base` forces every Arc::make_mut to change the pointer, even for
        // mutations that do not increment the catalog revision.
        if !Arc::ptr_eq(&self.catalog, &base) {
            mz_ore::halt!("catalog changed while a view commit was unpublished");
        }
        drop(base);
        let result = match result {
            Ok(candidate) => {
                let copy_started = Instant::now();
                let catalog = self.catalog_mut();
                phase_seconds
                    .with_label_values(&["catalog_copy"])
                    .observe(copy_started.elapsed().as_secs_f64());
                let TransactionResult {
                    builtin_table_updates,
                    catalog_updates,
                    audit_events,
                } = catalog.publish_transaction(candidate);
                self.track_catalog_audit_events(Some(ctx.session().conn_id()), audit_events);
                let stage_start = Instant::now();
                let table_updates = self
                    .builtin_table_update()
                    .execute_catalog(builtin_table_updates);
                phase_seconds
                    .with_label_values(&["stage_builtin"])
                    .observe(stage_start.elapsed().as_secs_f64());
                self.finish_catalog_transaction(Some(&mut ctx), table_updates, catalog_updates)
                    .await;
                Ok(ExecuteResponse::CreatedView)
            }
            Err(AdapterError::Catalog(Error {
                kind: ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                ctx.session()
                    .add_notice(AdapterNotice::ObjectAlreadyExists { name, ty: "view" });
                Ok(ExecuteResponse::CreatedView)
            }
            Err(error) => Err(error),
        };
        self.metrics
            .catalog_transact_seconds
            .with_label_values(&["catalog_transact_with_context"])
            .observe(started.elapsed().as_secs_f64());
        ctx.retire(result);
    }
}
