// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Various utility methods used by the [`Coordinator`]. Ideally these are all
//! put in more meaningfully named modules.

use mz_adapter_types::connection::ConnectionId;
use mz_ore::now::EpochMillis;
use mz_repr::{Diff, GlobalId, SqlScalarType};
use mz_sql::names::{Aug, ResolvedIds};
use mz_sql::plan::{Params, StatementDesc};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql_parser::ast::{Raw, Statement};

use crate::active_compute_sink::{ActiveComputeSink, ActiveComputeSinkRetireReason};
use crate::catalog::Catalog;
use crate::coord::appends::BuiltinTableAppendNotify;
use crate::coord::{Coordinator, Message};
use crate::session::{Session, StateRevision, TransactionStatus};
use crate::util::describe;
use crate::{AdapterError, ExecuteContext, ExecuteResponse, metrics};

impl Coordinator {
    pub(crate) fn plan_statement(
        &self,
        session: &Session,
        stmt: mz_sql::ast::Statement<Aug>,
        params: &mz_sql::plan::Params,
        resolved_ids: &ResolvedIds,
    ) -> Result<mz_sql::plan::Plan, AdapterError> {
        let pcx = session.pcx();
        let catalog = self.catalog().for_session(session);
        let plan = mz_sql::plan::plan(Some(pcx), &catalog, stmt, params, resolved_ids)?;
        Ok(plan)
    }

    pub(crate) fn declare(
        &self,
        mut ctx: ExecuteContext,
        name: String,
        stmt: Statement<Raw>,
        sql: String,
        params: Params,
    ) {
        let catalog = self.owned_catalog();
        let now = self.now();
        mz_ore::task::spawn(|| "coord::declare", async move {
            let result =
                Self::declare_inner(ctx.session_mut(), &catalog, name, stmt, sql, params, now)
                    .map(|()| ExecuteResponse::DeclaredCursor);
            ctx.retire(result);
        });
    }

    fn declare_inner(
        session: &mut Session,
        catalog: &Catalog,
        name: String,
        stmt: Statement<Raw>,
        sql: String,
        params: Params,
        now: EpochMillis,
    ) -> Result<(), AdapterError> {
        let param_types = params
            .execute_types
            .iter()
            .map(|ty| Some(ty.clone()))
            .collect::<Vec<_>>();
        let desc = describe(catalog, stmt.clone(), &param_types, session)?;
        let params = params
            .datums
            .into_iter()
            .zip(params.execute_types)
            .collect();
        let result_formats = vec![mz_pgwire_common::Format::Text; desc.arity()];
        let logging = session.mint_logging(sql, Some(&stmt), now);
        let state_revision = StateRevision {
            catalog_revision: catalog.transient_revision(),
            session_state_revision: session.state_revision(),
        };
        session.set_portal(
            name,
            desc,
            Some(stmt),
            logging,
            params,
            result_formats,
            state_revision,
        )?;
        Ok(())
    }

    #[mz_ore::instrument(level = "debug")]
    pub(crate) fn describe(
        catalog: &Catalog,
        session: &Session,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<SqlScalarType>>,
    ) -> Result<StatementDesc, AdapterError> {
        if let Some(stmt) = stmt {
            describe(catalog, stmt, &param_types, session)
        } else {
            Ok(StatementDesc::new(None))
        }
    }

    /// Verify a prepared statement is still valid. This will return an error if
    /// the catalog's revision has changed and the statement now produces a
    /// different type than its original.
    pub(crate) fn verify_prepared_statement(
        catalog: &Catalog,
        session: &mut Session,
        name: &str,
    ) -> Result<(), AdapterError> {
        let ps = match session.get_prepared_statement_unverified(name) {
            Some(ps) => ps,
            None => return Err(AdapterError::UnknownPreparedStatement(name.to_string())),
        };
        if let Some(new_revision) = Self::verify_statement_revision(
            catalog,
            session,
            ps.stmt(),
            ps.desc(),
            ps.state_revision,
        )? {
            let ps = session
                .get_prepared_statement_mut_unverified(name)
                .expect("known to exist");
            ps.state_revision = new_revision;
        }

        Ok(())
    }

    /// Verify a portal is still valid.
    pub(crate) fn verify_portal(
        &self,
        session: &mut Session,
        name: &str,
    ) -> Result<(), AdapterError> {
        let portal = match session.get_portal_unverified(name) {
            Some(portal) => portal,
            None => return Err(AdapterError::UnknownCursor(name.to_string())),
        };
        if let Some(new_revision) = Self::verify_statement_revision(
            self.catalog(),
            session,
            portal.stmt.as_deref(),
            &portal.desc,
            portal.state_revision,
        )? {
            let portal = session
                .get_portal_unverified_mut(name)
                .expect("known to exist");
            *portal.state_revision = new_revision;
        }
        Ok(())
    }

    /// If the current catalog/session revisions don't match the given revisions, re-describe the
    /// statement and ensure its result type has not changed. Return `Some((c, s))` with the new
    /// (valid) catalog and session state revisions if its plan has changed. Return `None` if the
    /// revisions match. Return an error if the plan has changed.
    fn verify_statement_revision(
        catalog: &Catalog,
        session: &Session,
        stmt: Option<&Statement<Raw>>,
        desc: &StatementDesc,
        old_state_revision: StateRevision,
    ) -> Result<Option<StateRevision>, AdapterError> {
        let current_state_revision = StateRevision {
            catalog_revision: catalog.transient_revision(),
            session_state_revision: session.state_revision(),
        };
        if old_state_revision != current_state_revision {
            let current_desc = Self::describe(
                catalog,
                session,
                stmt.cloned(),
                desc.param_types.iter().map(|ty| Some(ty.clone())).collect(),
            )?;
            if &current_desc != desc {
                Err(AdapterError::ChangedPlan(
                    "cached plan must not change result type".to_string(),
                ))
            } else {
                Ok(Some(current_state_revision))
            }
        } else {
            Ok(None)
        }
    }

    /// Handle removing in-progress transaction state regardless of the end action
    /// of the transaction.
    pub(crate) async fn clear_transaction(
        &mut self,
        session: &mut Session,
    ) -> TransactionStatus<mz_repr::Timestamp> {
        // This function is *usually* called when transactions end, but it can fail to be called in
        // some cases (for example if the session's role id was dropped, then we return early and
        // don't go through the normal sequence_end_transaction path). The `Command::Commit` handler
        // and `AdapterClient::end_transaction` protect against this by each executing their parts
        // of this function. Thus, if this function changes, ensure that the changes are propogated
        // to either of those components.
        self.clear_connection(session.conn_id()).await;
        session.clear_transaction()
    }

    /// Clears coordinator state for a connection.
    pub(crate) async fn clear_connection(&mut self, conn_id: &ConnectionId) {
        self.staged_cancellation.remove(conn_id);
        self.retire_compute_sinks_for_conn(conn_id, ActiveComputeSinkRetireReason::Finished)
            .await;
        self.retire_cluster_reconfigurations_for_conn(conn_id).await;

        // Release this transaction's compaction hold on collections.
        if let Some(txn_reads) = self.txn_read_holds.remove(conn_id) {
            tracing::debug!(?txn_reads, "releasing txn read holds");

            // Make it explicit that we're dropping these read holds. Dropping
            // them will release them at the Coordinator.
            drop(txn_reads);
        }

        if let Some(_guard) = self
            .active_conns
            .get_mut(conn_id)
            .expect("must exist for active session")
            .deferred_lock
            .take()
        {
            // If there are waiting deferred statements, process one.
            if !self.serialized_ddl.is_empty() {
                let _ = self.internal_cmd_tx.send(Message::DeferredStatementReady);
            }
        }
    }

    /// Adds coordinator bookkeeping for an active compute sink.
    ///
    /// This is a low-level method. The caller is responsible for installing the
    /// sink in the controller.
    pub(crate) async fn add_active_compute_sink(
        &mut self,
        id: GlobalId,
        active_sink: ActiveComputeSink,
    ) -> BuiltinTableAppendNotify {
        let user = self.active_conns()[active_sink.connection_id()].user();
        let session_type = metrics::session_type_label_value(user);

        self.active_conns
            .get_mut(active_sink.connection_id())
            .expect("must exist for active sessions")
            .drop_sinks
            .insert(id);

        let ret_fut = match &active_sink {
            ActiveComputeSink::Subscribe(active_subscribe) => {
                let update =
                    self.catalog()
                        .state()
                        .pack_subscribe_update(id, active_subscribe, Diff::ONE);
                let update = self.catalog().state().resolve_builtin_table_update(update);

                self.metrics
                    .active_subscribes
                    .with_label_values(&[session_type])
                    .inc();

                self.builtin_table_update().execute(vec![update]).await.0
            }
            ActiveComputeSink::CopyTo(_) => {
                self.metrics
                    .active_copy_tos
                    .with_label_values(&[session_type])
                    .inc();
                Box::pin(std::future::ready(()))
            }
        };
        self.active_compute_sinks.insert(id, active_sink);
        ret_fut
    }

    /// Removes coordinator bookkeeping for an active compute sink.
    ///
    /// This is a low-level method. The caller is responsible for dropping the
    /// sink from the controller. Consider calling `drop_compute_sink` or
    /// `retire_compute_sink` instead.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn remove_active_compute_sink(
        &mut self,
        id: GlobalId,
    ) -> Option<ActiveComputeSink> {
        if let Some(sink) = self.active_compute_sinks.remove(&id) {
            let user = self.active_conns()[sink.connection_id()].user();
            let session_type = metrics::session_type_label_value(user);

            self.active_conns
                .get_mut(sink.connection_id())
                .expect("must exist for active compute sink")
                .drop_sinks
                .remove(&id);

            match &sink {
                ActiveComputeSink::Subscribe(active_subscribe) => {
                    let update = self.catalog().state().pack_subscribe_update(
                        id,
                        active_subscribe,
                        Diff::MINUS_ONE,
                    );
                    let update = self.catalog().state().resolve_builtin_table_update(update);
                    self.builtin_table_update().blocking(vec![update]).await;

                    self.metrics
                        .active_subscribes
                        .with_label_values(&[session_type])
                        .dec();
                }
                ActiveComputeSink::CopyTo(_) => {
                    self.metrics
                        .active_copy_tos
                        .with_label_values(&[session_type])
                        .dec();
                }
            }
            Some(sink)
        } else {
            None
        }
    }
}
