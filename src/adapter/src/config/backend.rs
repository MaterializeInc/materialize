// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::Row;
use mz_sql::ast::{Ident, Raw, ShowStatement, ShowVariableStatement, Statement};

use crate::catalog::SYSTEM_USER;
use crate::session::EndTransactionAction;
use crate::{AdapterError, Client, ExecuteResponse, PeekResponseUnary, SessionClient};

use super::SynchronizedParameters;

/// A backend client for pushing and pulling [SynchronizedParameters].
///
/// Pulling is required in order to catch concurrent changes before pushing
/// modified values in the [crate::config::system_parameter_sync].
pub struct SystemParameterBackend {
    session_client: SessionClient,
}

impl SystemParameterBackend {
    pub async fn new(client: Client) -> Result<Self, AdapterError> {
        let conn_client = client.new_conn()?;
        let session = conn_client.new_session(SYSTEM_USER.clone());
        let (session_client, _) = conn_client.startup(session, true).await?;
        Ok(Self { session_client })
    }

    /// Push all current values from the given [SynchronizedParameters] that are
    /// marked as modified to the [SystemParameterBackend] and reset their
    /// modified status.
    pub async fn push(&mut self, params: &mut SynchronizedParameters) {
        for param in params.modified() {
            let alter_system = param.as_stmt();
            match self.execute_alter_system(alter_system).await {
                Ok(()) => {
                    tracing::debug!(name = param.name, value = param.value, "sync parameter");
                }
                Err(error) => {
                    tracing::error!("cannot execute `ALTER SYSTEM` query: {}", error);
                }
            }
        }
    }

    /// Pull the current values for all [SynchronizedParameters] from the
    /// [SystemParameterBackend].
    pub async fn pull(&mut self, params: &mut SynchronizedParameters) {
        let show_all = Statement::Show(ShowStatement::ShowVariable(ShowVariableStatement {
            variable: Ident::from("ALL"),
        }));
        match self.execute_query(show_all).await {
            Ok(rows) => {
                let mut datum_vec = mz_repr::DatumVec::new();
                for row in rows {
                    let datums = datum_vec.borrow_with(&row);
                    let name = datums[NAME_COLUMN].unwrap_str();
                    let value = datums[VALUE_COLUMN].unwrap_str();
                    if params.is_synchronized(name) {
                        params.modify(name, value);
                    }
                }
            }
            Err(error) => {
                tracing::error!("cannot execute `SHOW ALL` query: {}", error)
            }
        }
    }

    /// Execute a single `ALTER SYSTEM` statement.
    async fn execute_alter_system(&mut self, stmt: Statement<Raw>) -> Result<(), AdapterError> {
        self.session_client.start_transaction(Some(1)).await?;

        self.prepare(stmt).await?;
        let result = match self.session_client.execute(EMPTY_PORTAL.into()).await? {
            ExecuteResponse::AlteredSystemConfiguration => Ok(()),
            _ => Err(AdapterError::Internal(UNEXPECTED_RESPONSE.to_string())),
        };

        if result.is_ok() {
            self.session_client
                .end_transaction(EndTransactionAction::Commit)
                .await?;
        } else {
            self.session_client.fail_transaction();
        }

        result
    }

    /// Execute a single query and returns the results.
    async fn execute_query(&mut self, stmt: Statement<Raw>) -> Result<Vec<Row>, AdapterError> {
        self.session_client.start_transaction(Some(1)).await?;

        self.prepare(stmt).await?;
        let result = match self.session_client.execute(EMPTY_PORTAL.into()).await? {
            ExecuteResponse::SendingRows { future: rows, .. } => match rows.await {
                PeekResponseUnary::Rows(rows) => Ok(rows),
                PeekResponseUnary::Error(e) => Err(AdapterError::Internal(e)),
                PeekResponseUnary::Canceled => {
                    Err(AdapterError::Internal(UNEXPECTED_RESPONSE.to_string()))
                }
            },
            _ => Err(AdapterError::Internal(UNEXPECTED_RESPONSE.to_string())),
        };

        if result.is_ok() {
            self.session_client
                .end_transaction(EndTransactionAction::Commit)
                .await?;
        } else {
            self.session_client.fail_transaction();
        }

        result
    }

    /// Prepare a statement for execution. After calling this, you can call
    /// [SessionClient::execute] in order to execute the passed [Statement].
    async fn prepare(&mut self, stmt: Statement<Raw>) -> Result<(), AdapterError> {
        self.session_client
            .describe(EMPTY_PORTAL.into(), Some(stmt.clone()), vec![])
            .await?;

        let prep_stmt = self
            .session_client
            .get_prepared_statement(EMPTY_PORTAL)
            .await?;
        let params = vec![];
        let result_formats = vec![
            mz_pgrepr::Format::Text;
            prep_stmt
                .desc()
                .relation_desc
                .clone()
                .map(|desc| desc.typ().column_types.len())
                .unwrap_or(0)
        ];
        let desc = prep_stmt.desc().clone();
        let revision = prep_stmt.catalog_revision;
        let stmt = prep_stmt.sql().cloned();

        self.session_client.session().set_portal(
            EMPTY_PORTAL.into(),
            desc,
            stmt,
            params,
            result_formats,
            revision,
        )
    }
}

const NAME_COLUMN: usize = 0;
const VALUE_COLUMN: usize = 1;
const EMPTY_PORTAL: &str = "";
const UNEXPECTED_RESPONSE: &str = "unexpected response to SessionClient::execute request";
