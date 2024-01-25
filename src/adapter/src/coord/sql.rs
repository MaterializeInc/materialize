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
use mz_repr::{GlobalId, ScalarType};
use mz_sql::names::{Aug, ResolvedIds};
use mz_sql::plan::{Params, StatementDesc};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{Raw, Statement, StatementKind};

use crate::catalog::Catalog;
use crate::coord::appends::BuiltinTableAppendNotify;
use crate::coord::Coordinator;
use crate::session::{Session, TransactionStatus};
use crate::subscribe::ActiveSubscribe;
use crate::util::describe;
use crate::{metrics, AdapterError, ExecuteContext, ExecuteResponse};

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
            .types
            .iter()
            .map(|ty| Some(ty.clone()))
            .collect::<Vec<_>>();
        let desc = describe(catalog, stmt.clone(), &param_types, session)?;
        let params = params.datums.into_iter().zip(params.types).collect();
        let result_formats = vec![mz_pgwire_common::Format::Text; desc.arity()];
        let redacted_sql = stmt.to_ast_string_redacted();
        let logging =
            session.mint_logging(sql, redacted_sql, now, Some(StatementKind::from(&stmt)));
        session.set_portal(
            name,
            desc,
            Some(stmt),
            logging,
            params,
            result_formats,
            catalog.transient_revision(),
        )?;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) fn describe(
        catalog: &Catalog,
        session: &Session,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<ScalarType>>,
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
        if let Some(revision) = Self::verify_statement_revision(
            catalog,
            session,
            ps.stmt(),
            ps.desc(),
            ps.catalog_revision,
        )? {
            let ps = session
                .get_prepared_statement_mut_unverified(name)
                .expect("known to exist");
            ps.catalog_revision = revision;
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
        if let Some(revision) = Self::verify_statement_revision(
            self.catalog(),
            session,
            portal.stmt.as_deref(),
            &portal.desc,
            portal.catalog_revision,
        )? {
            let portal = session
                .get_portal_unverified_mut(name)
                .expect("known to exist");
            portal.catalog_revision = revision;
        }
        Ok(())
    }

    /// If the catalog and portal revisions don't match, re-describe the statement
    /// and ensure its result type has not changed. Return `Some(x)` with the new
    /// (valid) revision if its plan has changed. Return `None` if the revisions
    /// match. Return an error if the plan has changed.
    fn verify_statement_revision(
        catalog: &Catalog,
        session: &Session,
        stmt: Option<&Statement<Raw>>,
        desc: &StatementDesc,
        catalog_revision: u64,
    ) -> Result<Option<u64>, AdapterError> {
        let current_revision = catalog.transient_revision();
        if catalog_revision != current_revision {
            let current_desc = Self::describe(
                catalog,
                session,
                stmt.cloned(),
                desc.param_types.iter().map(|ty| Some(ty.clone())).collect(),
            )?;
            if &current_desc != desc {
                Err(AdapterError::ChangedPlan(format!(
                    "cached plan must not change result type",
                )))
            } else {
                Ok(Some(current_revision))
            }
        } else {
            Ok(None)
        }
    }

    /// Handle removing in-progress transaction state regardless of the end action
    /// of the transaction.
    pub(crate) fn clear_transaction(
        &mut self,
        session: &mut Session,
    ) -> TransactionStatus<mz_repr::Timestamp> {
        self.clear_connection(session.conn_id());
        session.clear_transaction()
    }

    /// Clears coordinator state for a connection.
    pub(crate) fn clear_connection(&mut self, conn_id: &ConnectionId) {
        let conn_meta = self
            .active_conns
            .get_mut(conn_id)
            .expect("must exist for active session");
        let drop_sinks = std::mem::take(&mut conn_meta.drop_sinks);
        self.drop_compute_sinks(drop_sinks);

        // Release this transaction's compaction hold on collections.
        if let Some(txn_reads) = self.txn_read_holds.remove(conn_id) {
            self.release_read_hold(&txn_reads);
        }
    }

    /// Handle adding metadata associated with a SUBSCRIBE query, returning a notify that resolves
    /// when our builtin table updates are complete.
    pub(crate) async fn add_active_subscribe(
        &mut self,
        id: GlobalId,
        active_subscribe: ActiveSubscribe,
    ) -> BuiltinTableAppendNotify {
        let update = self
            .catalog()
            .state()
            .pack_subscribe_update(id, &active_subscribe, 1);
        let builtin_update_notify = self.builtin_table_update().execute(vec![update]).await;

        let session_type = metrics::session_type_label_value(&active_subscribe.user);
        self.metrics
            .active_subscribes
            .with_label_values(&[session_type])
            .inc();

        self.active_subscribes.insert(id, active_subscribe);

        builtin_update_notify
    }

    /// Handle removing metadata associated with a SUBSCRIBE query.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn remove_active_subscribe(&mut self, id: GlobalId) {
        if let Some(active_subscribe) = self.active_subscribes.remove(&id) {
            let update = self
                .catalog()
                .state()
                .pack_subscribe_update(id, &active_subscribe, -1);
            self.builtin_table_update().blocking(vec![update]).await;

            let session_type = metrics::session_type_label_value(&active_subscribe.user);
            self.metrics
                .active_subscribes
                .with_label_values(&[session_type])
                .dec();
        }
        // Note: Drop sinks are removed at commit time.
    }
}
