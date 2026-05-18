// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A thin gRPC client for the bogo-consensus service.
//!
//! The client exposes the raw protobuf types; the `mz_persist::bogo` module
//! wraps it and adapts to the persist `Consensus` trait. Keeping the
//! conversion out of this crate avoids a dependency cycle with `mz-persist`.

use std::pin::Pin;

use futures::Stream;
use futures::stream::TryStreamExt;
use tonic::Status;
use tonic::transport::{Channel, Endpoint};

use crate::proto::bogo_consensus_client::BogoConsensusClient as TonicClient;
use crate::proto::{
    CompareAndSetRequest, HeadRequest, ListKeysRequest, ScanRequest, TruncateRequest, VersionedData,
};

pub use crate::proto::CaSResult;

/// A handle to a bogo-consensus server.
///
/// Cheap to clone — internally just clones a tonic [`Channel`].
#[derive(Debug, Clone)]
pub struct BogoConsensusClient {
    inner: TonicClient<Channel>,
}

impl BogoConsensusClient {
    /// Connects to a bogo-consensus server at the given URL.
    ///
    /// `url` must be a tonic-acceptable URI (typically `http://host:port`).
    /// The persist `bogo://` URL scheme is translated to `http://` before
    /// being passed here.
    pub async fn connect(url: String) -> Result<Self, anyhow::Error> {
        let endpoint = Endpoint::from_shared(url)?
            // No TLS — bogo is for local perf testing.
            .tcp_nodelay(true)
            // Bump HTTP/2 flow control windows well above tonic's 65 KiB
            // default. With the default, per-call CAS latency on a loopback
            // bench rises linearly with payload size (~130 ns/byte, ≈ 7 MB/s
            // throughput) because every other request stalls waiting for the
            // server's WINDOW_UPDATE round-trip. Bumping the windows up
            // amortises that across many requests.
            .initial_stream_window_size(8 * 1024 * 1024)
            .initial_connection_window_size(16 * 1024 * 1024);
        let channel = endpoint.connect().await?;
        // Catalog-shard scans grow with shard history; raise the gRPC message
        // size cap well above tonic's 4 MiB default to avoid retry storms.
        Ok(Self {
            inner: TonicClient::new(channel)
                .max_decoding_message_size(256 * 1024 * 1024)
                .max_encoding_message_size(256 * 1024 * 1024),
        })
    }

    pub async fn head(&self, key: &str) -> Result<Option<VersionedData>, Status> {
        let resp = self
            .inner
            .clone()
            .head(HeadRequest {
                key: key.to_owned(),
            })
            .await?
            .into_inner();
        Ok(resp.data)
    }

    pub async fn compare_and_set(
        &self,
        key: &str,
        new: VersionedData,
    ) -> Result<CaSResult, Status> {
        let resp = self
            .inner
            .clone()
            .compare_and_set(CompareAndSetRequest {
                key: key.to_owned(),
                new: Some(new),
            })
            .await?
            .into_inner();
        CaSResult::try_from(resp.result)
            .map_err(|_| Status::internal(format!("unknown CaSResult: {}", resp.result)))
    }

    pub async fn scan(
        &self,
        key: &str,
        from: u64,
        limit: u64,
    ) -> Result<Vec<VersionedData>, Status> {
        let resp = self
            .inner
            .clone()
            .scan(ScanRequest {
                key: key.to_owned(),
                from,
                limit,
            })
            .await?
            .into_inner();
        Ok(resp.data)
    }

    pub async fn truncate(&self, key: &str, seqno: u64) -> Result<Option<u64>, Status> {
        let resp = self
            .inner
            .clone()
            .truncate(TruncateRequest {
                key: key.to_owned(),
                seqno,
            })
            .await?
            .into_inner();
        Ok(resp.deleted)
    }

    pub async fn list_keys(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<String, Status>> + Send>>, Status> {
        let stream = self
            .inner
            .clone()
            .list_keys(ListKeysRequest {})
            .await?
            .into_inner();
        Ok(Box::pin(stream.map_ok(|resp| resp.key)))
    }
}
