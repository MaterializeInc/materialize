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

use mz_compute_client::protocol::command::{ComputeCommand, InstanceConfig};
use mz_compute_client::protocol::response::ComputeResponse;
use mz_persist_types::PersistLocation;
use mz_service::client::GenericClient;
use mz_service::transport::{Client, NoopMetrics};
use uuid::Uuid;

pub type ComputeCtpClient = Client<ComputeCommand, ComputeResponse>;

/// Connects to a clusterd compute controller address and completes the
/// `Hello` -> `CreateInstance` -> `InitializationComplete` handshake.
pub async fn connect_and_handshake(
    compute_addr: &str,
    peek_stash_persist_location: PersistLocation,
) -> anyhow::Result<ComputeCtpClient> {
    let version = mz_build_info::build_info!().semver_version();
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
