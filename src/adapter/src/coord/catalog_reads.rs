// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Catalog freshness at coordinator statement entry, before catalog-derived errors.

use std::sync::Arc;

use futures::future::BoxFuture;
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_sql::plan::{Params, QueryWhen};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::IsolationLevel;
use mz_sql_parser::ast::{Raw, Statement};
use mz_storage_types::sources::Timeline;
use tokio::sync::watch;

use crate::AdapterError;
use crate::catalog::Catalog;
use crate::coord::timestamp_selection::TimestampProvider;
use crate::coord::{
    Coordinator, ExecuteContext, Message, TimestampContext, TimestampDetermination,
};
use crate::peek_client::{CoordinatorClient, PeekClient};
use crate::session::RequireLinearization;

/// The entry point to resume after capturing a fresh catalog. Portal execution
/// has not started logging yet, while direct statement reentries already have.
#[derive(Debug)]
pub enum ExecuteCatalogContinuation {
    Portal {
        portal_name: String,
        nested: bool,
    },
    Statement {
        stmt: Arc<Statement<Raw>>,
        params: Params,
    },
}

impl Coordinator {
    /// Validate a newly chosen execution timestamp before storing transaction state.
    /// Diagnostics and fixed timestamps retain independent statement-entry freshness.
    pub(crate) fn validate_query_catalog(
        &self,
        ctx: &ExecuteContext,
        determination: &TimestampDetermination,
        when: &QueryWhen,
        new_timestamp: bool,
        requires_linearization: RequireLinearization,
    ) -> Option<BoxFuture<'static, Result<(), AdapterError>>> {
        let certified = ctx.query_catalog_timestamp()?;
        let catalog = Arc::clone(ctx.query_catalog()?);
        let TimestampContext::TimelineTimestamp {
            timeline: Timeline::EpochMilliseconds,
            chosen_ts,
            ..
        } = determination.timestamp_context
        else {
            return None;
        };
        if matches!(requires_linearization, RequireLinearization::NotRequired)
            || !new_timestamp
            || chosen_ts <= certified
            || ctx.session().vars().transaction_isolation() != &IsolationLevel::StrictSerializable
            || !Self::needs_linearized_read_ts(&IsolationLevel::StrictSerializable, when)
        {
            return None;
        }
        let mut client = self.background_peek_client(&catalog);
        Some(Box::pin(async move {
            client
                .oracle_read_ts_at_least(Timeline::EpochMilliseconds, chosen_ts)
                .await?;
            let current = client
                .catalog_snapshot_at(Arc::clone(&catalog), chosen_ts)
                .await?;
            if current.planning_position() != catalog.planning_position() {
                return Err(AdapterError::CatalogSnapshotChanged);
            }
            Ok(())
        }))
    }

    pub(crate) fn replan_execute(&mut self, mut ctx: ExecuteContext) {
        ctx.query_replanned = true;
        let input = Arc::clone(
            ctx.query_replan
                .as_ref()
                .expect("replanning has original input"),
        );
        self.start_execute_catalog_read(
            ctx,
            ExecuteCatalogContinuation::Statement {
                stmt: Arc::clone(&input.0),
                params: input.1.clone(),
            },
        );
    }

    /// A client for coordinator-owned work that must not wait on the coordinator
    /// loop. Requests return through the normal internal command channel.
    pub(crate) fn background_peek_client(&self, catalog: &Arc<Catalog>) -> PeekClient {
        let build_version = catalog.state().config().build_info.human_version(None);
        PeekClient::new(
            CoordinatorClient::Background {
                tx: self.internal_cmd_tx.clone(),
                metrics: self.metrics.clone(),
            },
            catalog,
            self.query_client
                .is_none()
                .then(|| Arc::clone(&self.controller.storage_collections)),
            self.query_client.clone(),
            Arc::clone(&self.transient_id_gen),
            self.optimizer_metrics.clone(),
            self.persist_client.clone(),
            self.statement_logging.create_frontend(build_version),
            Arc::clone(&self.occ_write_semaphore),
            self.group_commit_tx.clone(),
            self.read_only_controllers,
        )
    }

    /// Certify within this entry's real-time interval, including direct reentries
    /// after DDL deferral or purification. Never reuse the context's earlier anchor.
    pub(crate) fn start_execute_catalog_read(
        &mut self,
        ctx: ExecuteContext,
        continuation: ExecuteCatalogContinuation,
    ) {
        let (_, cancel_rx) = self
            .connection_cancel_watches
            .entry(ctx.session().conn_id().clone())
            .or_insert_with(|| watch::channel(false));
        if *cancel_rx.borrow() {
            ctx.retire(Err(AdapterError::Canceled));
            return;
        }

        let catalog = self.owned_catalog();
        let mut client = self.background_peek_client(&catalog);
        let otel_ctx = OpenTelemetryContext::obtain();
        let internal_cmd_tx = self.internal_cmd_tx.clone();
        let handle = task::spawn(|| "execute_catalog_read", async move {
            // Keep the seed alive: PeekClient's cache holds only a weak reference.
            let _catalog = catalog;
            client.fresh_catalog_snapshot("coordinator_execute").await
        });
        self.handle_spawn(
            ctx,
            handle.abort_on_drop(),
            true,
            move |mut ctx, (catalog, timestamp)| {
                ctx.set_query_catalog(catalog, timestamp);
                let _ = internal_cmd_tx.send(Message::ExecuteCatalogReady {
                    ctx,
                    continuation,
                    otel_ctx,
                });
            },
        );
    }

    pub(crate) async fn execute_catalog_ready(
        &mut self,
        mut ctx: ExecuteContext,
        continuation: ExecuteCatalogContinuation,
    ) {
        // Cancellation can arrive after handle_spawn sends the continuation but
        // before the loop processes it. In particular, do not start DDL in that gap.
        if self
            .connection_cancel_watches
            .get(ctx.session().conn_id())
            .is_some_and(|(_, rx)| *rx.borrow())
        {
            ctx.retire(Err(AdapterError::Canceled));
            return;
        }
        match continuation {
            ExecuteCatalogContinuation::Portal {
                portal_name,
                nested,
            } => {
                self.handle_execute_certified(portal_name, ctx, nested)
                    .await;
            }
            ExecuteCatalogContinuation::Statement { stmt, params } => {
                if let Some(portal) = ctx.query_portal.clone() {
                    let catalog = Arc::clone(ctx.query_catalog().expect("certified catalog"));
                    if let Err(error) = Self::verify_portal(&catalog, ctx.session_mut(), &portal) {
                        ctx.retire(Err(error));
                        return;
                    }
                }
                self.handle_execute_inner_certified(stmt, params, ctx).await;
            }
        }
    }
}
