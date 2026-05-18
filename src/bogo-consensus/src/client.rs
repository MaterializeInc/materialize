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
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::Stream;
use futures::stream::TryStreamExt;
use tonic::Status;
use tonic::transport::{Channel, Endpoint};

use crate::proto::bogo_consensus_client::BogoConsensusClient as TonicClient;
use crate::proto::{
    CompareAndSetRequest, HeadRequest, ListKeysRequest, ScanRequest, TruncateRequest, VersionedData,
};

pub use crate::proto::CaSResult;

/// Default fan-out of independent gRPC connections from a single
/// [`BogoConsensusClient`] to the server. We open this many TCP
/// connections and round-robin RPCs across them so a multi-MB scan
/// response on one connection does not head-of-line-block the small
/// CAS responses queued on others.
///
/// Empirically (envd DDL audit, `persist_external_op_latency` for
/// `consensus_cas`): with `1` channel ≈ 5-10% of CAS calls land in
/// the 32-64 ms bucket and dominate the mean; bumping to a small
/// pool clears that tail without measurable overhead at idle.
const DEFAULT_CHANNELS: usize = 8;

/// A handle to a bogo-consensus server.
///
/// Internally holds a fixed pool of independent gRPC channels (TCP
/// connections) and round-robins each RPC across them. Cloning is
/// cheap — every field is `Arc`-like under the hood.
#[derive(Debug, Clone)]
pub struct BogoConsensusClient {
    channels: std::sync::Arc<Vec<TonicClient<Channel>>>,
    next: std::sync::Arc<AtomicUsize>,
}

impl BogoConsensusClient {
    /// Connects to a bogo-consensus server at the given URL.
    ///
    /// `url` must be a tonic-acceptable URI (typically `http://host:port`).
    /// The persist `bogo://` URL scheme is translated to `http://` before
    /// being passed here.
    pub async fn connect(url: String) -> Result<Self, anyhow::Error> {
        Self::connect_with_channels(url, DEFAULT_CHANNELS).await
    }

    /// Like [`connect`](Self::connect) but with an explicit number of
    /// underlying gRPC connections. Exposed mostly so the conformance test
    /// can run with `channels = 1` and exercise the single-connection path.
    pub async fn connect_with_channels(
        url: String,
        channels: usize,
    ) -> Result<Self, anyhow::Error> {
        let channels = channels.max(1);
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
        let mut conns = Vec::with_capacity(channels);
        for _ in 0..channels {
            let channel = endpoint.clone().connect().await?;
            // Catalog-shard scans grow with shard history; raise the gRPC
            // message size cap well above tonic's 4 MiB default to avoid
            // retry storms.
            let client = TonicClient::new(channel)
                .max_decoding_message_size(256 * 1024 * 1024)
                .max_encoding_message_size(256 * 1024 * 1024);
            conns.push(client);
        }
        Ok(Self {
            channels: std::sync::Arc::new(conns),
            next: std::sync::Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Picks the next connection in round-robin order and returns an owned
    /// clone of its tonic client. Cloning the `TonicClient` just clones the
    /// underlying `Channel`'s `Arc`, so this is cheap.
    fn pick(&self) -> TonicClient<Channel> {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.channels.len();
        self.channels[i].clone()
    }

    pub async fn head(&self, key: &str) -> Result<Option<VersionedData>, Status> {
        let resp = self
            .pick()
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
            .pick()
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
            .pick()
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
            .pick()
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
            .pick()
            .list_keys(ListKeysRequest {})
            .await?
            .into_inner();
        Ok(Box::pin(stream.map_ok(|resp| resp.key)))
    }
}
