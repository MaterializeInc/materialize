// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compute CTP connection and controller handshake. Generic: it sends any
//! `ComputeCommand` and receives any `ComputeResponse`.

use std::time::Duration;

use mz_compute_client::protocol::command::{ComputeCommand, ComputeParameters, InstanceConfig};
use mz_compute_client::protocol::response::ComputeResponse;
use mz_compute_types::dyncfgs::ENABLE_PEEK_RESPONSE_STASH;
use mz_dyncfg::ConfigUpdates;
use mz_persist_types::PersistLocation;
use mz_service::client::GenericClient;
use mz_service::transport::{Client, NoopMetrics};
use uuid::Uuid;

pub type ComputeCtpClient = Client<ComputeCommand, ComputeResponse>;

/// Connects to a clusterd compute controller address and sends the pre-init part
/// of the handshake: `Hello` -> `CreateInstance` -> `UpdateConfiguration`, but
/// *not* `InitializationComplete`.
///
/// Leaving the connection before `InitializationComplete` is the reconciliation
/// window: a controller replays the dataflows it expects the replica to be
/// running, and the replica reconciles them against its live dataflows on
/// `InitializationComplete`, keeping the matching ones rather than rehydrating.
/// The initial connect (see [`connect_and_handshake`]) sends an empty window; a
/// reconnect replays the running dataflows before completing.
pub async fn connect_and_hello(
    compute_addr: &str,
    peek_stash_persist_location: PersistLocation,
) -> anyhow::Result<ComputeCtpClient> {
    // Use persist-client's BUILD_INFO: it is release-versioned (synced by
    // bin/bump-version), so it matches the clusterd we connect to. Our own
    // crate is `0.0.0`, which would fail the handshake's version check.
    let version = mz_persist_client::BUILD_INFO.semver_version();
    let mut client = Client::<ComputeCommand, ComputeResponse>::connect(
        compute_addr,
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
    client
        .send(ComputeCommand::CreateInstance(Box::new(InstanceConfig {
            logging: Default::default(),
            expiration_offset: None,
            peek_stash_persist_location,
            arrangement_dictionary_compression: false,
        })))
        .await?;

    // Disable the peek response stash so peeks return their rows inline rather
    // than writing them to persist. The driver's `peek` reads inline `Rows`;
    // stashing would require reading the results back out of persist.
    let mut dyncfg_updates = ConfigUpdates::default();
    dyncfg_updates.add(&ENABLE_PEEK_RESPONSE_STASH, false);
    client
        .send(ComputeCommand::UpdateConfiguration(Box::new(
            ComputeParameters {
                dyncfg_updates,
                ..Default::default()
            },
        )))
        .await?;

    Ok(client)
}

/// Connects and completes the full `Hello` -> `CreateInstance` ->
/// `UpdateConfiguration` -> `InitializationComplete` handshake, leaving the
/// reconciliation window empty.
pub async fn connect_and_handshake(
    compute_addr: &str,
    peek_stash_persist_location: PersistLocation,
) -> anyhow::Result<ComputeCtpClient> {
    let mut client = connect_and_hello(compute_addr, peek_stash_persist_location).await?;
    client.send(ComputeCommand::InitializationComplete).await?;
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::target;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn handshake_holds_connection() {
        if !target::e2e_enabled() {
            return;
        }
        let dir = tempfile::tempdir().expect("tempdir");
        let loc = PersistLocation {
            blob_uri: format!("file://{}", dir.path().display()).parse().unwrap(),
            consensus_uri: std::env::var("COCKROACH_URL")
                .expect("COCKROACH_URL for e2e")
                .parse()
                .unwrap(),
        };
        let mut client = connect_and_handshake(&target::compute_addr(), loc)
            .await
            .expect("handshake");
        let r = tokio::time::timeout(Duration::from_millis(500), client.recv()).await;
        assert!(r.is_err() || r.unwrap().is_ok());
    }
}
