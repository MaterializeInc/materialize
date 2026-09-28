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

use std::sync::Arc;

use itertools::Itertools;
use mz_adapter_types::connection::ConnectionId;
use mz_ore::now::EpochMillis;
use mz_repr::{Diff, GlobalId, SqlScalarType};
use mz_sql::names::{Aug, ResolvedIds};
use mz_sql::plan::{Params, StatementDesc};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql_parser::ast::{Raw, Statement};

use crate::active_compute_sink::{ActiveComputeSink, ActiveComputeSinkRetireReason};
use crate::catalog::Catalog;
use crate::coord::appends::{BuiltinTableAppendCompletion, BuiltinTableAppendNotify};
use crate::coord::{Coordinator, Message};
use crate::session::{
    PreparedInvalidation, PreparedQuery, Session, StateRevision, TransactionStatus,
};
use crate::util::describe;
use crate::{AdapterError, ExecuteContext, ExecuteResponse, metrics};

impl Coordinator {
    /// Plans a statement and returns the plan along with any resolved IDs
    /// discovered inside SQL-implemented function bodies. The extra IDs should
    /// only be used for the `restrict_to_user_objects` RBAC check.
    pub(crate) fn plan_statement(
        &self,
        session: &Session,
        stmt: mz_sql::ast::Statement<Aug>,
        params: &mz_sql::plan::Params,
        resolved_ids: &ResolvedIds,
    ) -> Result<(mz_sql::plan::Plan, ResolvedIds), AdapterError> {
        let pcx = session.pcx();
        let catalog = self.catalog().for_session(session);
        let (plan, sql_impl_ids) =
            mz_sql::plan::plan(Some(pcx), &catalog, stmt, params, resolved_ids)?;
        Ok((plan, sql_impl_ids))
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
        let (desc, query) =
            Self::describe_prepared(catalog, session, Some(stmt.clone()), param_types)?;
        let params = params
            .datums
            .into_iter()
            .zip_eq(params.execute_types)
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
            query,
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

    pub(crate) fn describe_prepared(
        catalog: &Catalog,
        session: &Session,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<SqlScalarType>>,
    ) -> Result<(Arc<StatementDesc>, Option<Arc<PreparedQuery>>), AdapterError> {
        if catalog.system_config().enable_prepared_query_reuse()
            && matches!(stmt, Some(Statement::Select(_)))
        {
            let conn_catalog = catalog.for_session(session);
            let (stmt, resolved_ids) =
                mz_sql::names::resolve(&conn_catalog, stmt.expect("SELECT"))?;
            session.metrics().prepared.analysis.inc();
            let analysis =
                mz_sql::plan::describe_analyzed(session.pcx(), &conn_catalog, stmt, &param_types)?;
            let query = analysis.select.map(|select| {
                Arc::new(PreparedQuery::new(
                    catalog,
                    session,
                    select,
                    resolved_ids,
                    analysis.sql_impl_ids,
                    analysis.desc.param_types.clone(),
                ))
            });
            Ok((Arc::new(analysis.desc), query))
        } else {
            Ok((
                Arc::new(Self::describe(catalog, session, stmt, param_types)?),
                None,
            ))
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
        if let Some((new_revision, query)) = Self::verify_statement_revision(
            catalog,
            session,
            ps.stmt(),
            ps.desc(),
            ps.state_revision,
            ps.query.as_deref(),
        )? {
            let ps = session
                .get_prepared_statement_mut_unverified(name)
                .expect("known to exist");
            ps.state_revision = new_revision;
            ps.query = query;
        }

        Ok(())
    }

    /// Verify a portal is still valid.
    pub(crate) fn verify_portal(
        catalog: &Catalog,
        session: &mut Session,
        name: &str,
    ) -> Result<(), AdapterError> {
        let portal = match session.get_portal_unverified(name) {
            Some(portal) => portal,
            None => return Err(AdapterError::UnknownCursor(name.to_string())),
        };
        if let Some((new_revision, query)) = Self::verify_statement_revision(
            catalog,
            session,
            portal.stmt.as_deref(),
            &portal.desc,
            portal.state_revision,
            portal.query.as_deref(),
        )? {
            let portal = session
                .get_portal_unverified_mut(name)
                .expect("known to exist");
            *portal.state_revision = new_revision;
            *portal.query = query;
        }
        Ok(())
    }

    /// Reanalyzes invalid statements while preserving their description contract.
    /// SELECT analysis depends on semantic context, while other statements can
    /// depend on the portal namespace (for example FETCH).
    fn verify_statement_revision(
        catalog: &Catalog,
        session: &Session,
        stmt: Option<&Statement<Raw>>,
        desc: &StatementDesc,
        old_state_revision: StateRevision,
        old_query: Option<&PreparedQuery>,
    ) -> Result<Option<(StateRevision, Option<Arc<PreparedQuery>>)>, AdapterError> {
        let current_state_revision = StateRevision {
            catalog_revision: catalog.transient_revision(),
            session_state_revision: session.state_revision(),
        };
        let reuse_select = catalog.system_config().enable_prepared_query_reuse()
            && matches!(stmt, Some(Statement::Select(_)));
        let valid = if reuse_select {
            // A SELECT can be describable without reusable typed analysis.
            // Absence of that optional artifact does not invalidate its descriptor.
            old_query.map_or(old_state_revision == current_state_revision, |query| {
                let metrics = &session.metrics().prepared;
                match query.invalidation_reason(catalog, session) {
                    None => return true,
                    Some(PreparedInvalidation::Catalog) => metrics.invalid_catalog.inc(),
                    Some(PreparedInvalidation::Settings) => metrics.invalid_settings.inc(),
                    Some(PreparedInvalidation::Roles) => metrics.invalid_roles.inc(),
                }
                false
            })
        } else {
            old_query.is_none() && old_state_revision == current_state_revision
        };
        if !valid {
            let (current_desc, query) = Self::describe_prepared(
                catalog,
                session,
                stmt.cloned(),
                desc.param_types.iter().map(|ty| Some(ty.clone())).collect(),
            )?;
            if current_desc.as_ref() != desc {
                Err(AdapterError::ChangedPlan(
                    "cached plan must not change result type".to_string(),
                ))
            } else {
                Ok(Some((current_state_revision, query)))
            }
        } else {
            Ok(None)
        }
    }

    /// Handle removing in-progress transaction state regardless of the end action
    /// of the transaction.
    ///
    /// Returns a notify that resolves once any `mz_subscriptions` retractions
    /// caused by cleanup are durable.
    pub(crate) async fn clear_transaction(
        &mut self,
        session: &mut Session,
    ) -> (TransactionStatus, BuiltinTableAppendCompletion) {
        // This function is *usually* called when transactions end, but it can fail to be called in
        // some cases (for example if the session's role id was dropped, then we return early and
        // don't go through the normal sequence_end_transaction path). The `Command::Commit` handler
        // and `AdapterClient::end_transaction` protect against this by each executing their parts
        // of this function. Thus, if this function changes, ensure that the changes are propogated
        // to either of those components.
        let retire_notify = self.clear_connection(session.conn_id()).await;
        (session.clear_transaction(), retire_notify)
    }

    /// Clears coordinator state for a connection.
    ///
    /// Returns a notify that resolves once any `mz_subscriptions` retractions
    /// caused by cleanup are durable.
    pub(crate) async fn clear_connection(
        &mut self,
        conn_id: &ConnectionId,
    ) -> BuiltinTableAppendCompletion {
        self.connection_cancel_watches.remove(conn_id);
        let retire_notify = self
            .retire_compute_sinks_for_conn(conn_id, ActiveComputeSinkRetireReason::Finished)
            .await;

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

        retire_notify
    }

    /// Adds coordinator bookkeeping for an active compute sink.
    ///
    /// This is a low-level method. The caller is responsible for installing the
    /// sink in the controller.
    pub(crate) fn add_active_compute_sink(
        &mut self,
        id: GlobalId,
        active_sink: ActiveComputeSink,
    ) -> BuiltinTableAppendNotify {
        let session_type = match active_sink.connection_id() {
            Some(conn_id) => {
                let session_type =
                    metrics::session_type_label_value(self.active_conns()[conn_id].user());
                self.active_conns
                    .get_mut(conn_id)
                    .expect("must exist for active sessions")
                    .drop_sinks
                    .insert(id);
                session_type
            }
            None => "system",
        };

        let ret_fut: BuiltinTableAppendNotify = match &active_sink {
            ActiveComputeSink::Subscribe(active_subscribe) => {
                match active_subscribe.introspection_session_uuid() {
                    // An internal subscribe writes no `mz_subscriptions` row, so
                    // it stays out of the public `mz_active_subscribes` gauge
                    // too. Counting it there would report subscribes that
                    // introspection deliberately shows nothing of. It gets its
                    // own internal gauge instead, since it is still a dataflow
                    // holding cluster resources.
                    None => {
                        self.metrics
                            .active_internal_subscribes
                            .with_label_values(&[session_type])
                            .inc();

                        Box::pin(std::future::ready(()))
                    }
                    Some(session_uuid) => {
                        let update = self.catalog().state().pack_subscribe_update(
                            id,
                            active_subscribe,
                            session_uuid,
                            Diff::ONE,
                        );
                        let update = self.catalog().state().resolve_builtin_table_update(update);

                        self.metrics
                            .active_subscribes
                            .with_label_values(&[session_type])
                            .inc();

                        // Defer the introspection-row write to a group commit instead of
                        // committing it inline. An inline `execute` would block the coordinator
                        // loop on a timestamp-oracle round trip and stall every other session.
                        // `implement_subscribe` waits for this write before returning the
                        // `SUBSCRIBE` response to the subscribing session.
                        self.builtin_table_update().defer(vec![update])
                    }
                }
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
    /// Returns the removed sink together with a notify that resolves once the
    /// `mz_subscriptions` retraction is durable. The retraction is deferred to a group
    /// commit rather than committed inline, which would block the coordinator loop on a
    /// timestamp-oracle round trip. Callers that expose completion of the retirement
    /// should wait on the notify off the coordinator loop before responding. The notify is
    /// already
    /// resolved for sinks that write no introspection row (internal subscribes and COPY TO).
    ///
    /// This is a low-level method. The caller is responsible for dropping the
    /// sink from the controller. Consider calling `drop_compute_sink` or
    /// `retire_compute_sinks` instead.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn remove_active_compute_sink(
        &mut self,
        id: GlobalId,
    ) -> Option<(ActiveComputeSink, BuiltinTableAppendNotify)> {
        if let Some(sink) = self.active_compute_sinks.remove(&id) {
            let session_type = match sink.connection_id() {
                Some(conn_id) => {
                    let session_type =
                        metrics::session_type_label_value(self.active_conns()[conn_id].user());
                    self.active_conns
                        .get_mut(conn_id)
                        .expect("must exist for active compute sink")
                        .drop_sinks
                        .remove(&id);
                    session_type
                }
                None => "system",
            };

            let write_notify: BuiltinTableAppendNotify = match &sink {
                ActiveComputeSink::Subscribe(active_subscribe) => {
                    match active_subscribe.introspection_session_uuid() {
                        // No introspection row to retract, see
                        // `add_active_compute_sink`. The internal gauge is
                        // decremented here to stay symmetric with it.
                        None => {
                            self.metrics
                                .active_internal_subscribes
                                .with_label_values(&[session_type])
                                .dec();

                            Box::pin(std::future::ready(()))
                        }
                        Some(session_uuid) => {
                            let update = self.catalog().state().pack_subscribe_update(
                                id,
                                active_subscribe,
                                session_uuid,
                                Diff::MINUS_ONE,
                            );
                            let update =
                                self.catalog().state().resolve_builtin_table_update(update);

                            self.metrics
                                .active_subscribes
                                .with_label_values(&[session_type])
                                .dec();

                            // Defer the retraction to a group commit, for the same reason we
                            // defer the insert (see `add_active_compute_sink`): committing inline
                            // would block the coordinator loop. Callers that expose the
                            // retirement wait on the notify off the coordinator loop
                            // before responding.
                            self.builtin_table_update().defer(vec![update])
                        }
                    }
                }
                ActiveComputeSink::CopyTo(_) => {
                    self.metrics
                        .active_copy_tos
                        .with_label_values(&[session_type])
                        .dec();

                    Box::pin(std::future::ready(()))
                }
            };
            Some((sink, write_notify))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod prepared_tests {
    use super::*;
    use mz_ore::collections::CollectionExt;
    use mz_repr::Datum;
    use mz_sql::session::vars::{EndTransactionAction, VarInput};

    fn prepare(catalog: &Catalog, session: &mut Session, name: &str, sql: &str) {
        let stmt = mz_sql::parse::parse(sql)
            .expect("test fixture must be valid")
            .into_element()
            .ast;
        let (desc, query) =
            Coordinator::describe_prepared(catalog, session, Some(stmt.clone()), vec![])
                .expect("test fixture must be valid");
        let revision = StateRevision {
            catalog_revision: catalog.transient_revision(),
            session_state_revision: session.state_revision(),
        };
        session.set_prepared_statement(
            name.into(),
            Some(stmt),
            sql.into(),
            desc,
            query,
            revision,
            0,
        );
    }

    fn bind(session: &mut Session, name: &str, portal: &str, params: Vec<(Datum, SqlScalarType)>) {
        let ps = session
            .get_prepared_statement_unverified(name)
            .expect("test fixture must be valid");
        let desc = ps.shared_desc();
        let query = ps.query();
        let stmt = ps.stmt().cloned();
        let logging = Arc::clone(ps.logging());
        let revision = ps.state_revision;
        session
            .set_portal(
                portal.into(),
                desc,
                query,
                stmt,
                logging,
                params,
                vec![mz_pgwire_common::Format::Text],
                revision,
            )
            .expect("test fixture must be valid");
    }

    #[mz_ore::test(tokio::test)]
    async fn prepared_analysis_survives_portals_but_not_settings() {
        Catalog::with_debug(|mut catalog| async move {
            catalog
                .system_config_mut()
                .set("enable_prepared_query_reuse", VarInput::Flat("true"))
                .expect("test fixture must be valid");
            let mut session = Session::dummy();
            session.start_transaction_single_stmt(chrono::Utc::now());
            prepare(&catalog, &mut session, "q", "SELECT $1::int4");
            let original = session
                .get_prepared_statement_unverified("q")
                .expect("test fixture must be valid")
                .query()
                .expect("test fixture must be valid");
            let desc = session
                .get_prepared_statement_unverified("q")
                .expect("test fixture must be valid")
                .shared_desc();
            for value in [1, 9, 42] {
                bind(
                    &mut session,
                    "q",
                    "",
                    vec![(Datum::Int32(value), SqlScalarType::Int32)],
                );
                Coordinator::verify_portal(&catalog, &mut session, "")
                    .expect("test fixture must be valid");
                let portal = session
                    .get_portal_unverified("")
                    .expect("test fixture must be valid");
                assert!(Arc::ptr_eq(
                    &original,
                    portal.query.as_ref().expect("test fixture must be valid")
                ));
                assert!(Arc::ptr_eq(&desc, &portal.desc));
                let _ = session.clear_transaction();
                session.start_transaction_single_stmt(chrono::Utc::now());
                Coordinator::verify_prepared_statement(&catalog, &mut session, "q")
                    .expect("test fixture must be valid");
                assert!(Arc::ptr_eq(
                    &original,
                    &session
                        .get_prepared_statement_unverified("q")
                        .expect("test fixture must be valid")
                        .query()
                        .expect("test fixture must be valid")
                ));
            }
            session
                .vars_mut()
                .set(
                    catalog.system_config(),
                    "search_path",
                    VarInput::Flat("pg_catalog"),
                    true,
                )
                .expect("test fixture must be valid");
            Coordinator::verify_prepared_statement(&catalog, &mut session, "q")
                .expect("test fixture must be valid");
            let local = session
                .get_prepared_statement_unverified("q")
                .expect("test fixture must be valid")
                .query()
                .expect("test fixture must be valid");
            assert!(!Arc::ptr_eq(&original, &local));
            session
                .vars_mut()
                .end_transaction(EndTransactionAction::Rollback);
            Coordinator::verify_prepared_statement(&catalog, &mut session, "q")
                .expect("test fixture must be valid");
            let rolled_back = session
                .get_prepared_statement_unverified("q")
                .expect("test fixture must be valid")
                .query()
                .expect("test fixture must be valid");
            assert!(!Arc::ptr_eq(&local, &rolled_back));
            catalog
                .system_config_mut()
                .set("enable_prepared_query_reuse", VarInput::Flat("false"))
                .expect("test fixture must be valid");
            Coordinator::verify_prepared_statement(&catalog, &mut session, "q")
                .expect("test fixture must be valid");
            assert!(
                session
                    .get_prepared_statement_unverified("q")
                    .expect("test fixture must be valid")
                    .query()
                    .is_none()
            );
            catalog.expire().await;
        })
        .await
    }

    #[mz_ore::test(tokio::test)]
    async fn prepared_fetch_still_depends_on_portal_description() {
        Catalog::with_debug(|mut catalog| async move {
            catalog
                .system_config_mut()
                .set("enable_prepared_query_reuse", VarInput::Flat("true"))
                .expect("test fixture must be valid");
            let mut session = Session::dummy();
            session.start_transaction_single_stmt(chrono::Utc::now());
            prepare(&catalog, &mut session, "integer", "SELECT 1");
            bind(&mut session, "integer", "p", vec![]);
            prepare(&catalog, &mut session, "fetch", "FETCH p");
            assert!(
                session
                    .get_prepared_statement_unverified("fetch")
                    .expect("test fixture must be valid")
                    .query()
                    .is_none()
            );
            assert!(session.remove_portal("p"));
            prepare(&catalog, &mut session, "boolean", "SELECT true");
            bind(&mut session, "boolean", "p", vec![]);
            assert!(matches!(
                Coordinator::verify_prepared_statement(&catalog, &mut session, "fetch"),
                Err(AdapterError::ChangedPlan(_))
            ));
            catalog.expire().await;
        })
        .await
    }
}
