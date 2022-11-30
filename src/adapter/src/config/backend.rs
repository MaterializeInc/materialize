// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;

use futures::future::join_all;
use tokio_postgres::tls::NoTls;

use super::SynchronizedParameters;

/// A backend client for pushing and pulling [SynchronizedParameters].
///
/// Pulling is required in order to catch concurrent changes before pushing
/// modified values in the [crate::config::system_parameter_sync].
pub struct SystemParameterBackend {
    client: tokio_postgres::Client,
}

impl SystemParameterBackend {
    pub async fn new(addr: SocketAddr) -> Result<Self, anyhow::Error> {
        let (client, connection) = tokio_postgres::connect(
            &format!("host={} port={} user=mz_system", addr.ip(), addr.port()),
            NoTls,
        )
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        mz_ore::task::spawn(|| "sys_param_sync:postgres", async move {
            if let Err(e) = connection.await {
                // TODO (15956): can/should I just log this as a tracing error here?
                // We should probably handle try to reconnect in order to recover.
                tracing::error!("SystemParameterBackend Postgres connection error: {}", e);
            }
        });

        Ok(Self { client })
    }

    /// Push all current values from the given [SynchronizedParameters] that are
    /// marked as modified to the [SystemParameterBackend] and reset the their
    /// modified flags.
    pub async fn push(&self, params: &mut SynchronizedParameters) {
        let mut requests = vec![];
        let mut updates = vec![];

        requests.push(params.window_functions.as_request());
        requests.push(params.allowed_cluster_replica_sizes.as_request());
        requests.push(params.max_result_size.as_request());

        for request in requests.into_iter().flatten() {
            let client = &self.client;
            let update = async move {
                match client.execute(request.sql_statement().as_str(), &[]).await {
                    Ok(_updated_rows) => {
                        *request.parameter_flag = false;
                    }
                    Err(e) => {
                        tracing::error!("SystemParameterBackend::push error: {}", e);
                    }
                }
            };
            updates.push(update);
        }

        join_all(updates).await;
    }

    /// Pull the current values for all [SynchronizedParameters] from the
    /// [SystemParameterBackend].
    pub async fn pull(&self, params: &mut SynchronizedParameters) {
        match self.client.query("SHOW ALL", &[]).await {
            Ok(result) => {
                for row in result {
                    let name: &str = row.get("name");
                    if name == params.window_functions.name {
                        let value = row.get("setting");
                        params.window_functions.modify(value);
                    } else if name == params.allowed_cluster_replica_sizes.name {
                        let value = row.get("setting");
                        params.allowed_cluster_replica_sizes.modify(value);
                    } else if name == params.max_result_size.name {
                        let value = row.get("setting");
                        params.max_result_size.modify(value);
                    }
                }
            }
            Err(e) => {
                tracing::error!("SystemParameterBackend::pull error: {}", e);
            }
        }
    }
}
