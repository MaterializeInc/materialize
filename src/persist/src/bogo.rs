// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A [`Consensus`] implementation backed by the `mz-bogo-consensus` gRPC
//! server.
//!
//! Use `bogo://host:port` as `--persist-consensus-url` to route persist's
//! Consensus traffic to an in-memory bogo-consensus server. This bypasses
//! Postgres/CRDB entirely; it exists solely to take that backend out of the
//! loop during performance measurements.

use std::sync::Arc;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use futures_util::StreamExt;
use mz_bogo_consensus::client::{BogoConsensusClient, CaSResult as ProtoCaSResult};
use mz_bogo_consensus::proto::VersionedData as ProtoVersionedData;
use mz_ore::url::SensitiveUrl;
use tonic::{Code, Status};

use crate::location::{
    CaSResult, Consensus, Determinate, ExternalError, Indeterminate, ResultStream, SCAN_ALL, SeqNo,
    VersionedData,
};

/// Configuration for opening a [`BogoConsensus`].
#[derive(Debug, Clone)]
pub struct BogoConsensusConfig {
    /// The `bogo://` URL of the server.
    pub url: SensitiveUrl,
}

impl BogoConsensusConfig {
    /// Builds a config from a `bogo://host:port` URL. Returns an error if the
    /// URL is missing the host/port or uses the wrong scheme.
    pub fn new(url: SensitiveUrl) -> Result<Self, ExternalError> {
        if url.scheme() != "bogo" {
            return Err(ExternalError::from(anyhow!(
                "bogo-consensus URL must use the `bogo://` scheme, got `{}://`",
                url.scheme()
            )));
        }
        if url.host_str().is_none() || url.port().is_none() {
            return Err(ExternalError::from(anyhow!(
                "bogo-consensus URL must include host and port, got `{}`",
                url
            )));
        }
        Ok(Self { url })
    }
}

/// A [`Consensus`] backed by a remote `mz-bogo-consensus` server.
#[derive(Debug, Clone)]
pub struct BogoConsensus {
    client: Arc<BogoConsensusClient>,
}

impl BogoConsensus {
    /// Connects to the bogo-consensus server identified by `cfg.url`.
    pub async fn open(cfg: BogoConsensusConfig) -> Result<Self, ExternalError> {
        let host = cfg
            .url
            .host_str()
            .ok_or_else(|| ExternalError::from(anyhow!("bogo URL missing host")))?;
        let port = cfg
            .url
            .port()
            .ok_or_else(|| ExternalError::from(anyhow!("bogo URL missing port")))?;
        // tonic wants an http:// (or https://) endpoint; the `bogo://` scheme
        // is just a marker we use for routing.
        let endpoint = format!("http://{host}:{port}");
        let client = BogoConsensusClient::connect(endpoint)
            .await
            .map_err(|e| ExternalError::from(anyhow!("connecting to bogo-consensus: {e:#}")))?;
        Ok(Self {
            client: Arc::new(client),
        })
    }
}

/// Map a `tonic::Status` to `ExternalError`, classifying codes as
/// determinate vs indeterminate.
///
/// Codes returned by the server's deterministic logic (e.g. invalid seqno,
/// truncate-too-high) are mapped to `Determinate`. Network/transport-class
/// codes are `Indeterminate` — the operation may or may not have committed.
fn status_to_external(rpc: &'static str, status: Status) -> ExternalError {
    let msg = anyhow!(
        "bogo-consensus {rpc}: {} ({})",
        status.message(),
        status.code()
    );
    match status.code() {
        Code::InvalidArgument | Code::FailedPrecondition | Code::OutOfRange => {
            ExternalError::Determinate(Determinate::new(msg))
        }
        _ => ExternalError::Indeterminate(Indeterminate::new(msg)),
    }
}

fn from_proto(v: ProtoVersionedData) -> VersionedData {
    VersionedData {
        seqno: SeqNo(v.seqno),
        data: v.data,
    }
}

fn to_proto(v: VersionedData) -> ProtoVersionedData {
    ProtoVersionedData {
        seqno: v.seqno.0,
        data: v.data,
    }
}

#[async_trait]
impl Consensus for BogoConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        let client = Arc::clone(&self.client);
        Box::pin(try_stream! {
            let mut stream = client
                .list_keys()
                .await
                .map_err(|s| status_to_external("list_keys", s))?;
            while let Some(item) = stream.next().await {
                let key = item.map_err(|s| status_to_external("list_keys", s))?;
                yield key;
            }
        })
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let data = self
            .client
            .head(key)
            .await
            .map_err(|s| status_to_external("head", s))?;
        Ok(data.map(from_proto))
    }

    async fn compare_and_set(
        &self,
        key: &str,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        if new.seqno.0 > i64::MAX.try_into().expect("i64::MAX fits in u64") {
            return Err(ExternalError::from(anyhow!(
                "sequence numbers must fit within [0, i64::MAX], received: {:?}",
                new.seqno
            )));
        }
        let result = self
            .client
            .compare_and_set(key, to_proto(new))
            .await
            .map_err(|s| status_to_external("compare_and_set", s))?;
        Ok(match result {
            ProtoCaSResult::Committed => CaSResult::Committed,
            ProtoCaSResult::ExpectationMismatch => CaSResult::ExpectationMismatch,
            ProtoCaSResult::Unspecified => {
                return Err(ExternalError::Indeterminate(Indeterminate::new(anyhow!(
                    "bogo-consensus returned unspecified CaSResult"
                ))));
            }
        })
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let scan_all_u64 = u64::try_from(SCAN_ALL).unwrap_or(u64::MAX);
        let limit_u64 = u64::try_from(limit).unwrap_or(scan_all_u64);
        let data = self
            .client
            .scan(key, from.0, limit_u64)
            .await
            .map_err(|s| status_to_external("scan", s))?;
        Ok(data.into_iter().map(from_proto).collect())
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        let deleted = self
            .client
            .truncate(key, seqno.0)
            .await
            .map_err(|s| status_to_external("truncate", s))?;
        Ok(deleted.map(|d| usize::try_from(d).unwrap_or(usize::MAX)))
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::str::FromStr;
    use std::sync::Arc;

    use mz_bogo_consensus::metrics::BogoMetrics;
    use mz_bogo_consensus::proto::bogo_consensus_server::BogoConsensusServer as BogoGrpcServer;
    use mz_bogo_consensus::server::BogoConsensusServer;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::url::SensitiveUrl;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use super::*;
    use crate::location::tests::consensus_impl_test;

    async fn spawn_server() -> SocketAddr {
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .await
            .expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local_addr");
        let incoming = TcpListenerStream::new(listener);
        let metrics = Arc::new(BogoMetrics::new(&MetricsRegistry::new()));
        let svc = BogoGrpcServer::new(BogoConsensusServer::new(metrics));
        tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming)
                .await
                .expect("server");
        });
        addr
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(coverage, ignore)]
    async fn bogo_consensus_conformance() -> Result<(), ExternalError> {
        let addr = spawn_server().await;
        consensus_impl_test(|| {
            let url = SensitiveUrl::from_str(&format!("bogo://{}", addr)).expect("valid url");
            async move {
                let cfg = BogoConsensusConfig::new(url)?;
                BogoConsensus::open(cfg).await
            }
        })
        .await
    }
}
