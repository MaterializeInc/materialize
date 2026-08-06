// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compute CTP connection and the `Hello` step of the controller handshake.
//! Generic: it sends any `ComputeCommand` and receives any `ComputeResponse`.
//!
//! Only `Hello` (the transport/version step) happens here; the controller
//! handshake proper — `CreateInstance`, `UpdateConfiguration`,
//! `InitializationComplete` — is driven by explicit caller commands, so the
//! caller controls the instance config, the peek-stash setting, and exactly when
//! the reconciliation window opens and closes.

use std::time::Duration;

use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_service::client::GenericClient;
use mz_service::transport::{Client, NoopMetrics};
use uuid::Uuid;

pub type ComputeCtpClient = Client<ComputeCommand, ComputeResponse>;

/// Connects to a clusterd compute controller address and sends `Hello`, leaving
/// the controller handshake (`CreateInstance` onward) to the caller.
///
/// A reconnect re-runs exactly this: a fresh transport connection plus `Hello`.
/// The reconciliation window then opens when the script sends `CreateInstance`
/// and closes when it sends `InitializationComplete`; in between, the replica
/// reconciles the replayed dataflows against its live ones rather than
/// rehydrating.
pub async fn connect_and_hello(compute_addr: &str) -> anyhow::Result<ComputeCtpClient> {
    // Use persist-client's BUILD_INFO: it is release-versioned (synced by
    // bin/bump-version), so it matches the clusterd we connect to. Our own
    // crate is `0.0.0`, which would fail the handshake's version check.
    let version = mz_persist_client::BUILD_INFO.semver_version();
    let mut client = Client::<ComputeCommand, ComputeResponse>::connect(
        compute_addr,
        // Test driver dials a specific address directly, so skip the CTP identity check.
        None,
        version,
        Duration::from_secs(30),
        Duration::from_secs(60),
        NoopMetrics,
    )
    .await?;

    client
        .send(ComputeCommand::Hello {
            nonce: Uuid::new_v4(),
        })
        .await?;

    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::target;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn hello_holds_connection() {
        if !target::e2e_enabled() {
            return;
        }
        let mut client = connect_and_hello(&target::compute_addr())
            .await
            .expect("hello");
        let r = tokio::time::timeout(Duration::from_millis(500), client.recv()).await;
        assert!(r.is_err() || r.unwrap().is_ok());
    }
}
