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

use mz_repr::ScalarType;
use mz_sql::names::Aug;
use mz_sql::plan::StatementDesc;
use mz_sql_parser::ast::{Raw, Statement};
use mz_stash::Append;

use crate::coord::Coordinator;
use crate::session::{Session, TransactionStatus};
use crate::util::describe;
use crate::AdapterError;

impl<S: Append + 'static> Coordinator<S> {
    pub(crate) async fn plan_statement(
        &mut self,
        session: &mut Session,
        stmt: mz_sql::ast::Statement<Aug>,
        params: &mz_sql::plan::Params,
    ) -> Result<mz_sql::plan::Plan, AdapterError> {
        let pcx = session.pcx();
        let plan =
            mz_sql::plan::plan(Some(&pcx), &self.catalog.for_session(session), stmt, params)?;
        Ok(plan)
    }

    pub(crate) fn declare(
        &self,
        session: &mut Session,
        name: String,
        stmt: Statement<Raw>,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<(), AdapterError> {
        let desc = describe(&self.catalog, stmt.clone(), &param_types, session)?;
        let params = vec![];
        let result_formats = vec![mz_pgrepr::Format::Text; desc.arity()];
        session.set_portal(
            name,
            desc,
            Some(stmt),
            params,
            result_formats,
            self.catalog.transient_revision(),
        )?;
        Ok(())
    }

    pub(crate) fn describe(
        &self,
        session: &Session,
        stmt: Option<Statement<Raw>>,
        param_types: Vec<Option<ScalarType>>,
    ) -> Result<StatementDesc, AdapterError> {
        if let Some(stmt) = stmt {
            describe(&self.catalog, stmt, &param_types, session)
        } else {
            Ok(StatementDesc::new(None))
        }
    }

    /// Verify a prepared statement is still valid.
    pub(crate) fn verify_prepared_statement(
        &self,
        session: &mut Session,
        name: &str,
    ) -> Result<(), AdapterError> {
        let ps = match session.get_prepared_statement_unverified(&name) {
            Some(ps) => ps,
            None => return Err(AdapterError::UnknownPreparedStatement(name.to_string())),
        };
        if let Some(revision) =
            self.verify_statement_revision(session, ps.sql(), ps.desc(), ps.catalog_revision)?
        {
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
        let portal = match session.get_portal_unverified(&name) {
            Some(portal) => portal,
            None => return Err(AdapterError::UnknownCursor(name.to_string())),
        };
        if let Some(revision) = self.verify_statement_revision(
            &session,
            portal.stmt.as_ref(),
            &portal.desc,
            portal.catalog_revision,
        )? {
            let portal = session
                .get_portal_unverified_mut(&name)
                .expect("known to exist");
            portal.catalog_revision = revision;
        }
        Ok(())
    }

    fn verify_statement_revision(
        &self,
        session: &Session,
        stmt: Option<&Statement<Raw>>,
        desc: &StatementDesc,
        catalog_revision: u64,
    ) -> Result<Option<u64>, AdapterError> {
        let current_revision = self.catalog.transient_revision();
        if catalog_revision != current_revision {
            let current_desc = self.describe(
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
    pub(crate) async fn clear_transaction(
        &mut self,
        session: &mut Session,
    ) -> TransactionStatus<mz_repr::Timestamp> {
        let (drop_sinks, txn) = session.clear_transaction();
        self.drop_sinks(drop_sinks).await;

        // Release this transaction's compaction hold on collections.
        if let Some(txn_reads) = self.txn_reads.remove(&session.conn_id()) {
            self.release_read_hold(&txn_reads.read_holds).await;
        }
        txn
    }
}
