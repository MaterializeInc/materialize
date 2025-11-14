// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! [Consensus] and [Blob] implementations suitable for use with [turmoil].
//!
//! Both implementations function by forwarding calls to a server backend via
//! [turmoil::net::TcpStream]s. The server backend keeps a simple in-memory state (implemented
//! using [MemConsensus] and [MemBlob], respectively), which it uses to respond to RPCs.
//!
//! In a turmoil test, both the consensus and the blob server should be run as separate hosts. This
//! gives turmoil the ability to partition and crash them independently, for the maximum amount of
//! interesting interleaving.
//!
//! ```
//! use mz_persist::turmoil::{BlobState, ConsensusState, serve_blob, serve_consensus};
//!
//! let mut sim = turmoil::Builder::new().build();
//!
//! sim.host("consensus", {
//!     let state = ConsensusState::new();
//!     move || serve_consensus(7000, state.clone())
//! });
//!
//! sim.host("blob", {
//!     let state = BlobState::new();
//!     move || serve_blob(7000, state.clone())
//! });
//! ```
//!
//! To connect to these servers, use the following `PersistLocation`:
//!
//! ```ignore
//! PersistLocation {
//!     blob_uri: "turmoil://blob:7000".parse().unwrap(),
//!     consensus_uri: "turmoil://consensus:7000".parse().unwrap(),
//! }
//! ```

use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastInto;
use mz_ore::future::{TimeoutError, timeout};
use mz_ore::url::SensitiveUrl;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use turmoil::net::{TcpListener, TcpStream};

use crate::location::{
    Blob, BlobMetadata, CaSResult, Consensus, Determinate, ExternalError, Indeterminate,
    ResultStream, SeqNo, VersionedData,
};
use crate::mem::{MemBlob, MemBlobConfig, MemConsensus};

/// Timeout for RPC calls.
const RPC_TIMEOUT: Duration = Duration::from_secs(1);

/// Write an RPC message to the given connection.
async fn rpc_write<T>(conn: &mut TcpStream, msg: T) -> Result<(), ExternalError>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(&msg).unwrap();
    conn.write_u64(bytes.len().cast_into()).await?;
    conn.write_all(&bytes).await?;
    Ok(())
}

/// Read an RPC message from the given connection.
async fn rpc_read<T>(conn: &mut TcpStream) -> Result<T, ExternalError>
where
    T: for<'a> Deserialize<'a>,
{
    let len = conn.read_u64().await?;
    let mut bytes = vec![0; len.cast_into()];
    conn.read_exact(&mut bytes).await?;
    let resp = serde_json::from_slice(&bytes).unwrap();
    Ok(resp)
}

/// Serializable representation of `ExternalError`.
#[derive(Serialize, Deserialize)]
enum RpcError {
    Determinate(String),
    Indeterminate(String),
}

impl From<ExternalError> for RpcError {
    fn from(error: ExternalError) -> Self {
        match error {
            ExternalError::Determinate(e) => Self::Determinate(e.to_string()),
            ExternalError::Indeterminate(e) => Self::Indeterminate(e.to_string()),
        }
    }
}

impl From<RpcError> for ExternalError {
    fn from(error: RpcError) -> Self {
        match error {
            RpcError::Determinate(s) => Self::Determinate(Determinate::new(anyhow!(s))),
            RpcError::Indeterminate(s) => Self::Indeterminate(Indeterminate::new(anyhow!(s))),
        }
    }
}

/// Configuration for a [TurmoilConsensus].
#[derive(Clone, Debug)]
pub struct ConsensusConfig {
    addr: String,
}

impl ConsensusConfig {
    /// Construct a [ConsensusConfig] from a URL.
    pub fn new(url: &SensitiveUrl) -> Self {
        let addr = format!(
            "{}:{}",
            url.host().expect("must have a host"),
            url.port().expect("must have a port")
        );

        Self { addr }
    }
}

/// A [Consensus] implementation for use in turmoil tests.
#[derive(Debug)]
pub struct TurmoilConsensus {
    addr: String,
    /// Healthy connections to the consensus server.
    ///
    /// When making a call, we take one of these, perform the RPC, and then put it back if it's
    /// still healthy afterwards. This way we avoid having to hold a mutex across await points.
    connections: Mutex<Vec<TcpStream>>,
}

impl TurmoilConsensus {
    /// Open a [TurmoilConsensus] instance.
    pub fn open(config: ConsensusConfig) -> Self {
        Self {
            addr: config.addr,
            connections: Mutex::new(Vec::new()),
        }
    }

    /// Call the remote consensus server.
    async fn call<R>(&self, cmd: ConsensusCommand) -> Result<R, ExternalError>
    where
        R: for<'a> Deserialize<'a>,
    {
        let res = timeout::<_, _, ExternalError>(RPC_TIMEOUT, async {
            let conn = self.connections.lock().expect("poisoned").pop();
            let mut conn = match conn {
                Some(c) => c,
                None => TcpStream::connect(&self.addr).await?,
            };

            rpc_write(&mut conn, cmd).await?;
            let resp: Result<R, RpcError> = rpc_read(&mut conn).await?;

            self.connections.lock().expect("poisoned").push(conn);

            Ok(resp?)
        })
        .await;

        match res {
            Ok(resp) => Ok(resp),
            Err(TimeoutError::DeadlineElapsed) => Err(ExternalError::new_timeout(Instant::now())),
            Err(TimeoutError::Inner(e)) => Err(e),
        }
    }
}

#[async_trait]
impl Consensus for TurmoilConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        async_stream::try_stream! {
            let keys = self.call::<Vec<String>>(ConsensusCommand::ListKeys).await?;
            for key in keys {
                yield key;
            }
        }
        .boxed()
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        self.call(ConsensusCommand::Head { key: key.into() }).await
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        self.call(ConsensusCommand::CompareAndSet {
            key: key.into(),
            expected,
            new,
        })
        .await
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        self.call(ConsensusCommand::Scan {
            key: key.into(),
            from,
            limit,
        })
        .await
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<usize, ExternalError> {
        self.call(ConsensusCommand::Truncate {
            key: key.into(),
            seqno,
        })
        .await
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum ConsensusCommand {
    ListKeys,
    Head {
        key: String,
    },
    CompareAndSet {
        key: String,
        expected: Option<SeqNo>,
        new: VersionedData,
    },
    Scan {
        key: String,
        from: SeqNo,
        limit: usize,
    },
    Truncate {
        key: String,
        seqno: SeqNo,
    },
}

/// State of a turmoil consensus server.
#[derive(Clone, Debug)]
pub struct ConsensusState(MemConsensus);

impl ConsensusState {
    /// Create a new [ConsensusState].
    pub fn new() -> Self {
        Self(MemConsensus::default())
    }
}

/// Run a turmoil consensus server.
///
/// Intended to be used as the host logic passed to [turmoil::Sim::host].
pub async fn serve_consensus(port: u16, state: ConsensusState) -> turmoil::Result {
    let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;

    loop {
        let (conn, peer) = listener.accept().await?;
        mz_ore::task::spawn(
            || format!("turmoil consensus connection: {peer}"),
            serve_consensus_connection(conn, state.clone()),
        );
    }
}

async fn serve_consensus_connection(
    mut conn: TcpStream,
    state: ConsensusState,
) -> Result<(), ExternalError> {
    loop {
        let cmd = rpc_read(&mut conn).await?;
        match cmd {
            ConsensusCommand::ListKeys => {
                let keys = state.0.list_keys().try_collect::<Vec<_>>().await;
                let keys = keys.map_err(RpcError::from);
                rpc_write(&mut conn, keys).await?;
            }
            ConsensusCommand::Head { key } => {
                let data = state.0.head(&key).await;
                let data = data.map_err(RpcError::from);
                rpc_write(&mut conn, data).await?;
            }
            ConsensusCommand::CompareAndSet { key, expected, new } => {
                let result = state.0.compare_and_set(&key, expected, new).await;
                let result = result.map_err(RpcError::from);
                rpc_write(&mut conn, result).await?;
            }
            ConsensusCommand::Scan { key, from, limit } => {
                let data = state.0.scan(&key, from, limit).await;
                let data = data.map_err(RpcError::from);
                rpc_write(&mut conn, data).await?;
            }
            ConsensusCommand::Truncate { key, seqno } => {
                let count = state.0.truncate(&key, seqno).await;
                let count = count.map_err(RpcError::from);
                rpc_write(&mut conn, count).await?;
            }
        }
    }
}

#[derive(Clone, Debug)]
/// Configuration for a [TurmoilBlob].
pub struct BlobConfig {
    addr: String,
}

impl BlobConfig {
    /// Construct a [BlobConfig] from a URL.
    pub fn new(url: &SensitiveUrl) -> Self {
        let addr = format!(
            "{}:{}",
            url.host().expect("must have a host"),
            url.port().expect("must have a port")
        );

        Self { addr }
    }
}

/// A [Blob] implementation for use in turmoil tests.
#[derive(Debug)]
pub struct TurmoilBlob {
    addr: String,
    /// Healthy connections to the blob server.
    ///
    /// When making a call, we take one of these, perform the RPC, and then put it back if it's
    /// still healthy afterwards. This way we avoid having to hold a mutex across await points.
    connections: Mutex<Vec<TcpStream>>,
}

impl TurmoilBlob {
    /// Open a [TurmoilBlob] instance.
    pub fn open(config: BlobConfig) -> Self {
        Self {
            addr: config.addr,
            connections: Mutex::new(Vec::new()),
        }
    }

    /// Call the remote blob server.
    async fn call<R>(&self, cmd: BlobCommand) -> Result<R, ExternalError>
    where
        R: for<'a> Deserialize<'a>,
    {
        let res = timeout::<_, _, ExternalError>(RPC_TIMEOUT, async {
            let conn = self.connections.lock().expect("poisoned").pop();
            let mut conn = match conn {
                Some(c) => c,
                None => TcpStream::connect(&self.addr).await?,
            };

            rpc_write(&mut conn, cmd).await?;
            let resp: Result<R, RpcError> = rpc_read(&mut conn).await?;

            self.connections.lock().expect("poisoned").push(conn);

            Ok(resp?)
        })
        .await;

        match res {
            Ok(resp) => Ok(resp),
            Err(TimeoutError::DeadlineElapsed) => Err(anyhow!("timeout").into()),
            Err(TimeoutError::Inner(e)) => Err(e),
        }
    }
}

#[async_trait]
impl Blob for TurmoilBlob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        self.call(BlobCommand::Get { key: key.into() }).await
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        let items = self
            .call::<Vec<(String, u64)>>(BlobCommand::ListKeysAndMetadata {
                key_prefix: key_prefix.into(),
            })
            .await?;

        for (key, size_in_bytes) in items {
            f(BlobMetadata {
                key: &key,
                size_in_bytes,
            })
        }

        Ok(())
    }

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        self.call(BlobCommand::Set {
            key: key.into(),
            value,
        })
        .await
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        self.call(BlobCommand::Delete { key: key.into() }).await
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        self.call(BlobCommand::Restore { key: key.into() }).await
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum BlobCommand {
    Get { key: String },
    ListKeysAndMetadata { key_prefix: String },
    Set { key: String, value: Bytes },
    Delete { key: String },
    Restore { key: String },
}

/// State of a turmoil blob server.
#[derive(Clone, Debug)]
pub struct BlobState(MemBlob);

impl BlobState {
    /// Create a new [BlobState].
    pub fn new() -> Self {
        Self(MemBlob::open(MemBlobConfig::new(true)))
    }
}

/// Run a turmoil blob server.
///
/// Intended to be used as the host logic passed to [turmoil::Sim::host].
pub async fn serve_blob(port: u16, state: BlobState) -> turmoil::Result {
    let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;

    loop {
        let (conn, peer) = listener.accept().await?;
        mz_ore::task::spawn(
            || format!("turmoil blob connection: {peer}"),
            serve_blob_connection(conn, state.clone()),
        );
    }
}

async fn serve_blob_connection(mut conn: TcpStream, state: BlobState) -> Result<(), ExternalError> {
    loop {
        let cmd = rpc_read(&mut conn).await?;
        match cmd {
            BlobCommand::Get { key } => {
                let value = state.0.get(&key).await;
                let value = value.map_err(RpcError::from);
                rpc_write(&mut conn, value).await?;
            }
            BlobCommand::ListKeysAndMetadata { key_prefix } => {
                let mut items = Vec::new();
                let mut f = |m: BlobMetadata| {
                    items.push((m.key.to_string(), m.size_in_bytes));
                };
                let result = state.0.list_keys_and_metadata(&key_prefix, &mut f).await;
                let result = result.map(|()| items).map_err(RpcError::from);
                rpc_write(&mut conn, result).await?;
            }
            BlobCommand::Set { key, value } => {
                let result = state.0.set(&key, value).await;
                let result = result.map_err(RpcError::from);
                rpc_write(&mut conn, result).await?;
            }
            BlobCommand::Delete { key } => {
                let size = state.0.delete(&key).await;
                let size = size.map_err(RpcError::from);
                rpc_write(&mut conn, size).await?;
            }
            BlobCommand::Restore { key } => {
                let result = state.0.restore(&key).await;
                let result = result.map_err(RpcError::from);
                rpc_write(&mut conn, result).await?;
            }
        }
    }
}
