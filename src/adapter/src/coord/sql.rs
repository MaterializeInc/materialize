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

use mz_repr::{GlobalId, ScalarType};
use mz_sql::names::Aug;
use mz_sql::plan::StatementDesc;
use mz_sql_parser::ast::{Raw, Statement};

use crate::catalog::Catalog;
use crate::coord::Coordinator;
use crate::session::{Session, TransactionStatus};
use crate::subscribe::ActiveSubscribe;
use crate::util::{describe, ClientTransmitter};
use crate::{metrics, AdapterError, ExecuteResponse};

impl Coordinator {
    pub(crate) fn plan_statement(
        &mut self,
        session: &mut Session,
        stmt: mz_sql::ast::Statement<Aug>,
        params: &mz_sql::plan::Params,
    ) -> Result<mz_sql::plan::Plan, AdapterError> {
        let pcx = session.pcx();
        let plan = mz_sql::plan::plan(
            Some(pcx),
            &self.catalog().for_session(session),
            stmt,
            params,
        )?;
        Ok(plan)
    }

    pub(crate) fn declare(
        &self,
        tx: ClientTransmitter<ExecuteResponse>,
        mut session: Session,
        name: String,
        stmt: Statement<Raw>,
        param_types: Vec<Option<ScalarType>>,
    ) {
        let catalog = self.owned_catalog();
        mz_ore::task::spawn(|| "coord::declare", async move {
            let res = Self::declare_inner(&mut session, &catalog, name, stmt, param_types)
                .map(|()| ExecuteResponse::DeclaredCursor);
            tx.send(res, session);
        });
    }

    fn declare_inner(
        session: &mut Session,
        catalog: &Catalog,
        name: String,
        stmt: Statement<Raw>,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<(), AdapterError> {
        let desc = describe(catalog, stmt.clone(), &param_types, session)?;
        let params = vec![];
        let result_formats = vec![mz_pgrepr::Format::Text; desc.arity()];
        session.set_portal(
            name,
            desc,
            Some(stmt),
            params,
            result_formats,
            catalog.transient_revision(),
        )?;
        Ok(())
    }

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
            ps.sql(),
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
            portal.stmt.as_ref(),
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
                Err(AdapterError::ChangedPlan)
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
        let conn_meta = self
            .active_conns
            .get_mut(&session.conn_id())
            .expect("must exist for active session");
        let drop_sinks = std::mem::take(&mut conn_meta.drop_sinks);
        self.drop_compute_sinks(drop_sinks);

        // Release this transaction's compaction hold on collections.
        if let Some(txn_reads) = self.txn_reads.remove(&session.conn_id()) {
            self.release_read_hold(&txn_reads);
        }

        session.clear_transaction()
    }

    /// Handle adding metadata associated with a SUBSCRIBE query.
    pub(crate) async fn add_active_subscribe(
        &mut self,
        id: GlobalId,
        active_subscribe: ActiveSubscribe,
    ) {
        let update = self
            .catalog()
            .state()
            .pack_subscribe_update(id, &active_subscribe, 1);
        self.send_builtin_table_updates(vec![update]).await;

        let session_type = metrics::session_type_label_value(&active_subscribe.user);
        self.metrics
            .active_subscribes
            .with_label_values(&[session_type])
            .inc();

        self.active_subscribes.insert(id, active_subscribe);
    }

    /// Handle removing metadata associated with a SUBSCRIBE query.
    pub(crate) async fn remove_active_subscribe(&mut self, id: GlobalId) {
        if let Some(active_subscribe) = self.active_subscribes.remove(&id) {
            let update = self
                .catalog()
                .state()
                .pack_subscribe_update(id, &active_subscribe, -1);
            self.send_builtin_table_updates(vec![update]).await;

            let session_type = metrics::session_type_label_value(&active_subscribe.user);
            self.metrics
                .active_subscribes
                .with_label_values(&[session_type])
                .dec();
        }
        // Note: Drop sinks are removed at commit time.
    }
}
