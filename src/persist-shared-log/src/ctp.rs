// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Custom TCP Protocol (CTP) for the consensus service.
//!
//! A minimal length-prefixed bincode protocol over raw TCP with request-level
//! multiplexing. Each TCP connection supports many concurrent in-flight
//! requests, identified by a 4-byte request ID. This gives us HTTP/2-style
//! stream multiplexing without the h2/tonic/hyper/tower overhead.
//!
//! Wire format (request):
//!   `[4B request_id][1B opcode][4B BE payload length][bincode payload]`
//!
//! Wire format (response):
//!   `[4B request_id][1B status (0=ok, 1=err)][4B BE payload length][bincode payload | UTF-8 error]`
//!
//! Both client and server use write-coalescing: a dedicated writer task drains
//! a channel and flushes once per batch, naturally batching concurrent frames
//! into fewer syscalls.
//!
//! Protobuf is used only for WAL persistence. All network RPCs use bincode
//! for fast zero-copy serde without schema overhead.

use mz_ore::cast::CastFrom;
use mz_ore::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

use crate::acceptor::AcceptorHandle;
use crate::learner::LearnerHandle;

// ---------------------------------------------------------------------------
// Wire protocol constants
// ---------------------------------------------------------------------------

const OP_APPEND: u8 = 1;
const OP_AWAIT_CAS: u8 = 2;
const OP_AWAIT_TRUNCATE: u8 = 3;
const OP_HEAD: u8 = 4;
const OP_SCAN: u8 = 5;
const OP_LATEST_BATCH: u8 = 7;
/// Combined CAS: append + await result in one round-trip.
const OP_CAS: u8 = 8;
/// Combined truncate: append + await result in one round-trip.
const OP_TRUNCATE: u8 = 9;

const STATUS_OK: u8 = 0;
const STATUS_ERR: u8 = 1;

// ---------------------------------------------------------------------------
// RPC message types (bincode-serialized, NOT protobuf)
// ---------------------------------------------------------------------------

/// A CAS proposal sent over the wire. Contains the same fields as the protobuf
/// ProtoCasProposal but serialized with bincode.
#[derive(Serialize, Deserialize)]
pub struct CasProposal {
    pub key: String,
    pub expected: Option<u64>,
    pub new_seqno: u64,
    pub data: Vec<u8>,
}

/// A truncate proposal sent over the wire.
#[derive(Serialize, Deserialize)]
pub struct TruncateProposal {
    pub key: String,
    pub seqno: u64,
}

/// Append request — either a CAS or truncate proposal.
#[derive(Serialize, Deserialize)]
pub enum AppendRequest {
    Cas(CasProposal),
    Truncate(TruncateProposal),
}

/// Receipt returned after a successful append.
#[derive(Serialize, Deserialize)]
pub struct AppendResponse {
    pub batch_number: u64,
    pub position: u32,
}

/// Request to await a result from a specific batch position.
/// Includes the key so the server can route to the correct sharded learner.
#[derive(Serialize, Deserialize)]
pub struct AwaitResultRequest {
    pub batch_number: u64,
    pub position: u32,
    pub key: String,
}

/// CAS result.
#[derive(Serialize, Deserialize)]
pub struct CasResponse {
    pub committed: bool,
}

/// Truncate result.
#[derive(Serialize, Deserialize)]
pub struct TruncateResponse {
    pub deleted: Option<u64>,
}

/// Head request.
#[derive(Serialize, Deserialize)]
pub struct HeadRequest {
    pub key: String,
}

/// Head response.
#[derive(Serialize, Deserialize)]
pub struct HeadResponse {
    pub seqno: Option<u64>,
    pub data_len: usize,
}

/// Scan request.
#[derive(Serialize, Deserialize)]
pub struct ScanRequest {
    pub key: String,
    pub from: u64,
    pub limit: u64,
}

/// Scan response.
#[derive(Serialize, Deserialize)]
pub struct ScanResponse {
    pub count: u64,
}

/// Latest committed batch response.
#[derive(Serialize, Deserialize)]
pub struct LatestBatchResponse {
    pub batch_number: Option<u64>,
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// CTP server that accepts TCP connections and dispatches to acceptor/learner.
pub struct CtpServer {
    listener: TcpListener,
    acceptor: AcceptorHandle,
    learner: LearnerHandle,
}

impl CtpServer {
    pub async fn bind(
        addr: impl tokio::net::ToSocketAddrs,
        acceptor: AcceptorHandle,
        learner: LearnerHandle,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(CtpServer {
            listener,
            acceptor,
            learner,
        })
    }

    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn serve(self) {
        loop {
            let (stream, _) = match self.listener.accept().await {
                Ok(conn) => conn,
                Err(_) => continue,
            };
            stream.set_nodelay(true).ok();
            let acceptor = self.acceptor.clone();
            let learner = self.learner.clone();
            mz_ore::task::spawn(
                || "ctp-connection",
                handle_connection(stream, acceptor, learner),
            );
        }
    }
}

/// A response frame queued for the writer task.
struct ResponseFrame {
    req_id: u32,
    status: u8,
    payload: Vec<u8>,
}

async fn handle_connection(stream: TcpStream, acceptor: AcceptorHandle, learner: LearnerHandle) {
    let (rd, wr) = stream.into_split();
    let mut reader = BufReader::new(rd);
    let (resp_tx, resp_rx) = mpsc::unbounded_channel::<ResponseFrame>();

    // Writer task: drains response channel, coalesces writes, flushes once.
    mz_ore::task::spawn(
        || "ctp-server-writer",
        server_writer_task(BufWriter::new(wr), resp_rx),
    );

    // Reader loop: reads multiplexed request frames, spawns dispatch tasks.
    loop {
        let req_id = match reader.read_u32().await {
            Ok(v) => v,
            Err(_) => break,
        };
        let opcode = match reader.read_u8().await {
            Ok(v) => v,
            Err(_) => break,
        };
        let len = match reader.read_u32().await {
            Ok(v) => usize::cast_from(v),
            Err(_) => break,
        };
        let mut payload = vec![0u8; len];
        if len > 0 && reader.read_exact(&mut payload).await.is_err() {
            break;
        }

        let acceptor = acceptor.clone();
        let learner = learner.clone();
        let resp_tx = resp_tx.clone();
        mz_ore::task::spawn(|| "ctp-dispatch", async move {
            let (status, resp_payload) = match dispatch(opcode, &payload, &acceptor, &learner).await
            {
                Ok(data) => (STATUS_OK, data),
                Err(msg) => (STATUS_ERR, msg.into_bytes()),
            };
            let _ = resp_tx.send(ResponseFrame {
                req_id,
                status,
                payload: resp_payload,
            });
        });
    }
}

async fn server_writer_task(
    mut writer: BufWriter<OwnedWriteHalf>,
    mut rx: mpsc::UnboundedReceiver<ResponseFrame>,
) {
    while let Some(frame) = rx.recv().await {
        write_response_frame(&mut writer, &frame).await;
        // Drain all buffered responses before flushing (write coalescing).
        while let Ok(frame) = rx.try_recv() {
            write_response_frame(&mut writer, &frame).await;
        }
        if writer.flush().await.is_err() {
            break;
        }
    }
}

async fn write_response_frame(writer: &mut BufWriter<OwnedWriteHalf>, frame: &ResponseFrame) {
    let _ = writer.write_u32(frame.req_id).await;
    let _ = writer.write_u8(frame.status).await;
    let _ = writer
        .write_u32(u32::try_from(frame.payload.len()).unwrap_or(u32::MAX))
        .await;
    let _ = writer.write_all(&frame.payload).await;
}

async fn dispatch(
    opcode: u8,
    payload: &[u8],
    acceptor: &AcceptorHandle,
    learner: &LearnerHandle,
) -> Result<Vec<u8>, String> {
    use mz_persist::generated::consensus_service::{
        ProtoCasProposal, ProtoTruncateProposal, ProtoWalProposal, proto_wal_proposal,
    };

    match opcode {
        OP_APPEND => {
            let req: AppendRequest = bincode::deserialize(payload).map_err(|e| e.to_string())?;
            let proposal = match req {
                AppendRequest::Cas(cas) => ProtoWalProposal {
                    op: Some(proto_wal_proposal::Op::Cas(ProtoCasProposal {
                        key: cas.key,
                        expected: cas.expected,
                        new_seqno: cas.new_seqno,
                        data: cas.data,
                    })),
                },
                AppendRequest::Truncate(trunc) => ProtoWalProposal {
                    op: Some(proto_wal_proposal::Op::Truncate(ProtoTruncateProposal {
                        key: trunc.key,
                        seqno: trunc.seqno,
                    })),
                },
            };
            let resp = acceptor
                .append(proposal)
                .await
                .map_err(|e| format!("{e:?}"))?;
            let out = AppendResponse {
                batch_number: resp.batch_number,
                position: resp.position,
            };
            bincode::serialize(&out).map_err(|e| e.to_string())
        }
        OP_AWAIT_CAS => {
            let req: AwaitResultRequest =
                bincode::deserialize(payload).map_err(|e| e.to_string())?;
            let resp = learner
                .await_cas_result(req.batch_number, req.position)
                .await
                .map_err(|e| format!("{e:?}"))?;
            let out = CasResponse {
                committed: resp.committed,
            };
            bincode::serialize(&out).map_err(|e| e.to_string())
        }
        OP_AWAIT_TRUNCATE => {
            let req: AwaitResultRequest =
                bincode::deserialize(payload).map_err(|e| e.to_string())?;
            let resp = learner
                .await_truncate_result(req.batch_number, req.position)
                .await
                .map_err(|e| format!("{e:?}"))?;
            let out = TruncateResponse {
                deleted: resp.deleted,
            };
            bincode::serialize(&out).map_err(|e| e.to_string())
        }
        OP_HEAD => {
            let req: HeadRequest = bincode::deserialize(payload).map_err(|e| e.to_string())?;
            let resp = learner.head(req.key).await.map_err(|e| format!("{e:?}"))?;
            let out = HeadResponse {
                seqno: resp.data.as_ref().map(|d| d.seqno),
                data_len: resp.data.as_ref().map(|d| d.data.len()).unwrap_or(0),
            };
            bincode::serialize(&out).map_err(|e| e.to_string())
        }
        OP_SCAN => {
            let req: ScanRequest = bincode::deserialize(payload).map_err(|e| e.to_string())?;
            let resp = learner
                .scan(req.key, req.from, req.limit)
                .await
                .map_err(|e| format!("{e:?}"))?;
            let out = ScanResponse {
                count: u64::try_from(resp.data.len()).unwrap_or(0),
            };
            bincode::serialize(&out).map_err(|e| e.to_string())
        }
        OP_LATEST_BATCH => {
            let batch = acceptor.latest_committed_batch();
            let out = LatestBatchResponse {
                batch_number: batch,
            };
            bincode::serialize(&out).map_err(|e| e.to_string())
        }
        OP_CAS => {
            let req: CasProposal = bincode::deserialize(payload).map_err(|e| e.to_string())?;
            let proposal = ProtoWalProposal {
                op: Some(proto_wal_proposal::Op::Cas(ProtoCasProposal {
                    key: req.key,
                    expected: req.expected,
                    new_seqno: req.new_seqno,
                    data: req.data,
                })),
            };
            let receipt = acceptor
                .append(proposal)
                .await
                .map_err(|e| format!("{e:?}"))?;
            let resp = learner
                .await_cas_result(receipt.batch_number, receipt.position)
                .await
                .map_err(|e| format!("{e:?}"))?;
            let out = CasResponse {
                committed: resp.committed,
            };
            bincode::serialize(&out).map_err(|e| e.to_string())
        }
        OP_TRUNCATE => {
            let req: TruncateProposal = bincode::deserialize(payload).map_err(|e| e.to_string())?;
            let proposal = ProtoWalProposal {
                op: Some(proto_wal_proposal::Op::Truncate(ProtoTruncateProposal {
                    key: req.key,
                    seqno: req.seqno,
                })),
            };
            let receipt = acceptor
                .append(proposal)
                .await
                .map_err(|e| format!("{e:?}"))?;
            let resp = learner
                .await_truncate_result(receipt.batch_number, receipt.position)
                .await
                .map_err(|e| format!("{e:?}"))?;
            let out = TruncateResponse {
                deleted: resp.deleted,
            };
            bincode::serialize(&out).map_err(|e| e.to_string())
        }
        _ => Err(format!("unknown opcode: {opcode}")),
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// The pending response map, shared between the client request path and the
/// reader task. Extracted into its own Arc so the reader task does not hold
/// a strong reference to `ClientInner`, which would prevent the shutdown
/// chain: all CtpClients dropped → ClientInner dropped → write_tx dropped →
/// writer exits → OwnedWriteHalf dropped → TCP FIN → server reader exits →
/// server drops AcceptorHandle/LearnerHandle → learner can join.
type PendingMap = std::sync::Mutex<HashMap<u32, oneshot::Sender<Result<Vec<u8>, String>>>>;

/// Multiplexed CTP client. Many callers share one TCP connection via
/// request IDs. Clone is cheap (Arc).
#[derive(Clone)]
pub struct CtpClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    write_tx: mpsc::UnboundedSender<RequestFrame>,
    pending: Arc<PendingMap>,
    next_id: AtomicU32,
}

struct RequestFrame {
    req_id: u32,
    opcode: u8,
    payload: Vec<u8>,
}

impl CtpClient {
    pub async fn connect(addr: std::net::SocketAddr) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (rd, wr) = stream.into_split();

        let (write_tx, write_rx) = mpsc::unbounded_channel();
        let pending = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let inner = Arc::new(ClientInner {
            write_tx,
            pending: Arc::clone(&pending),
            next_id: AtomicU32::new(0),
        });

        // Reader task gets only the pending map — NOT an Arc<ClientInner>.
        // This lets ClientInner drop (closing write_tx) when all CtpClients
        // are dropped, enabling clean TCP shutdown.
        mz_ore::task::spawn(
            || "ctp-client-reader",
            client_reader_task(BufReader::new(rd), pending),
        );
        // Writer task owns the write half and drains the request channel.
        mz_ore::task::spawn(
            || "ctp-client-writer",
            client_writer_task(BufWriter::new(wr), write_rx),
        );

        Ok(CtpClient { inner })
    }

    async fn request<Req: Serialize, Resp: for<'de> Deserialize<'de>>(
        &self,
        opcode: u8,
        req: &Req,
    ) -> Result<Resp, String> {
        let payload = bincode::serialize(req).map_err(|e| e.to_string())?;
        let req_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);

        let (tx, rx) = oneshot::channel();
        self.inner.pending.lock().unwrap().insert(req_id, tx);

        self.inner
            .write_tx
            .send(RequestFrame {
                req_id,
                opcode,
                payload,
            })
            .map_err(|_| "writer task died".to_string())?;

        let resp_bytes = rx.await.map_err(|_| "reader task died".to_string())??;
        bincode::deserialize(&resp_bytes).map_err(|e| e.to_string())
    }

    pub async fn append(&self, req: AppendRequest) -> Result<AppendResponse, String> {
        self.request(OP_APPEND, &req).await
    }

    pub async fn await_cas_result(&self, req: AwaitResultRequest) -> Result<CasResponse, String> {
        self.request(OP_AWAIT_CAS, &req).await
    }

    pub async fn await_truncate_result(
        &self,
        req: AwaitResultRequest,
    ) -> Result<TruncateResponse, String> {
        self.request(OP_AWAIT_TRUNCATE, &req).await
    }

    pub async fn head(&self, req: HeadRequest) -> Result<HeadResponse, String> {
        self.request(OP_HEAD, &req).await
    }

    pub async fn scan(&self, req: ScanRequest) -> Result<ScanResponse, String> {
        self.request(OP_SCAN, &req).await
    }

    /// Combined CAS: append + await result in one round-trip.
    pub async fn cas(&self, req: CasProposal) -> Result<CasResponse, String> {
        self.request(OP_CAS, &req).await
    }

    /// Combined truncate: append + await result in one round-trip.
    pub async fn truncate(&self, req: TruncateProposal) -> Result<TruncateResponse, String> {
        self.request(OP_TRUNCATE, &req).await
    }
}

async fn client_writer_task(
    mut writer: BufWriter<OwnedWriteHalf>,
    mut rx: mpsc::UnboundedReceiver<RequestFrame>,
) {
    while let Some(frame) = rx.recv().await {
        write_request_frame(&mut writer, &frame).await;
        // Drain all buffered requests before flushing (write coalescing).
        while let Ok(frame) = rx.try_recv() {
            write_request_frame(&mut writer, &frame).await;
        }
        if writer.flush().await.is_err() {
            break;
        }
    }
    // Channel closed (all CtpClients dropped) — writer exits, dropping
    // OwnedWriteHalf which sends TCP FIN to the server.
}

async fn write_request_frame(writer: &mut BufWriter<OwnedWriteHalf>, frame: &RequestFrame) {
    let _ = writer.write_u32(frame.req_id).await;
    let _ = writer.write_u8(frame.opcode).await;
    let _ = writer
        .write_u32(u32::try_from(frame.payload.len()).unwrap_or(u32::MAX))
        .await;
    let _ = writer.write_all(&frame.payload).await;
}

async fn client_reader_task(mut reader: BufReader<OwnedReadHalf>, pending: Arc<PendingMap>) {
    loop {
        let req_id = match reader.read_u32().await {
            Ok(v) => v,
            Err(_) => break,
        };
        let status = match reader.read_u8().await {
            Ok(v) => v,
            Err(_) => break,
        };
        let len = match reader.read_u32().await {
            Ok(v) => usize::cast_from(v),
            Err(_) => break,
        };
        let mut buf = vec![0u8; len];
        if len > 0 && reader.read_exact(&mut buf).await.is_err() {
            break;
        }

        let result = if status == STATUS_OK {
            Ok(buf)
        } else {
            Err(String::from_utf8_lossy(&buf).to_string())
        };

        if let Some(tx) = pending.lock().unwrap().remove(&req_id) {
            let _ = tx.send(result);
        }
    }
}
