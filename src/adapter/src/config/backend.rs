// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_sql::session::user::SYSTEM_USER;
use tracing::{error, info};
use uuid::Uuid;

use crate::config::SynchronizedParameters;
use crate::session::SessionConfig;
use crate::{AdapterError, Client, SessionClient};

/// A backend client for pushing and pulling [SynchronizedParameters].
///
/// Pulling is required in order to catch concurrent changes before pushing
/// modified values in the [crate::config::system_parameter_sync].
pub struct SystemParameterBackend {
    session_client: SessionClient,
}

impl SystemParameterBackend {
    pub async fn new(client: Client) -> Result<Self, AdapterError> {
        let conn_id = client.new_conn_id()?;
        let session = client.new_session(SessionConfig {
            conn_id,
            uuid: Uuid::new_v4(),
            user: SYSTEM_USER.name.clone(),
            client_ip: None,
            external_metadata_rx: None,
            helm_chart_version: None,
        });
        let session_client = client.startup(session).await?;
        Ok(Self { session_client })
    }

    /// Push all current values from the given [SynchronizedParameters] that are
    /// marked as modified to the [SystemParameterBackend] and reset their
    /// modified status.
    pub async fn push(&mut self, params: &mut SynchronizedParameters) {
        for param in params.modified() {
            let mut vars = BTreeMap::new();
            info!(name = param.name, value = param.value, "updating parameter");
            vars.insert(param.name.clone(), param.value.clone());
            match self.session_client.set_system_vars(vars).await {
                Ok(()) => {
                    info!(name = param.name, value = param.value, "update success");
                }
                Err(error) => match error {
                    AdapterError::ReadOnly => {
                        info!(
                            name = param.name,
                            value = param.value,
                            "cannot update system variable in read-only mode",
                        );
                    }
                    error => {
                        error!(
                            name = param.name,
                            value = param.value,
                            "cannot update system variable: {}",
                            error
                        );
                    }
                },
            }
        }
    }

    /// Pull the current values for all [SynchronizedParameters] from the
    /// [SystemParameterBackend].
    pub async fn pull(&self, params: &mut SynchronizedParameters) {
        let vars = self.session_client.get_system_vars().await;
        for var in vars.iter() {
            params.modify(var.name(), &var.value());
        }
    }
}
