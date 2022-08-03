// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An implementation of the Maelstrom txn-list-append workload using persist

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::lattice::Lattice;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::sync::Mutex;
use tracing::{debug, info, trace};

use mz_persist::cfg::{BlobConfig, ConsensusConfig};
use mz_persist::location::{Blob, Consensus, ExternalError};
use mz_persist::unreliable::{UnreliableBlob, UnreliableConsensus, UnreliableHandle};
use mz_persist_client::async_runtime::CpuHeavyRuntime;
use mz_persist_client::read::{Listen, ListenEvent, ReadHandle};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Metrics, PersistClient, PersistConfig, ShardId};

use crate::maelstrom::api::{Body, ErrorCode, MaelstromError, NodeId, ReqTxnOp, ResTxnOp};
use crate::maelstrom::node::{Handle, Service};
use crate::maelstrom::services::{CachingBlob, MaelstromBlob, MaelstromConsensus};
use crate::maelstrom::Args;

pub fn run(args: Args) -> Result<(), anyhow::Error> {
    let read = std::io::stdin();
    let write = std::io::stdout();

    crate::maelstrom::node::run::<_, _, TransactorService>(args, read.lock(), write)?;
    Ok(())
}

/// Key of the persist shard used by [Transactor]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MaelstromKey(u64);

/// Val of the persist shard used by [Transactor]
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct MaelstromVal(Vec<u64>);

/// An implementation of read-write transactions on top of persist
///
/// This executes Maelstrom [txn-list-append] transactions. The timestamp
/// selection is modeled after Materialize's SQL implementation.
///
/// [txn-list-append]: https://github.com/jepsen-io/maelstrom/blob/v0.2.1/doc/workloads.md#workload-txn-list-append
///
/// A persist shard is maintained that directly represents a key-value map as it
/// evolves over time. (Our real SQL implementation would likely instead
/// maintain a WAL of updates to multiple tables.) Each transaction has
/// associated timestamps:
///
/// - `read_ts`: at which the contents of the map are read (inclusive)
/// - `write_ts`: at which txn mutations (if any) are written
/// - `expected_upper`: the upper bound of the previous txn
/// - `new_upper`: the upper bound after this transaction commits, if it does
///
/// To keep things simple, `write_ts` is always `read_ts+1`, `expected_upper` is
/// `antichain[write_ts]`, and `new_upper` is `antichain[write_ts+1]`.
///
/// Transactions are "committed" by using `[WriteHandle::compare_and_append]`,
/// which atomically:
/// - verifies that `expected_upper` matches
/// - writes the updates
/// - advances the upper to `new_upper`.
///
/// This guarantees that all transactions are linearized with each txn's
/// `read_ts` equal to the previous txn's `write_ts`. If two transactions race
/// by reading at the same `read_ts` and writing at the same `write_ts`, the
/// `compare_and_append` guarantees that only one of them succeeds and the other
/// must retry with new timestamps.
///
/// Upon first use, the persist shard is initialized by advancing the upper to
/// `antichain[T::minimum() + 1]`. This allows the first txn to use 0 as the
/// `read_ts` and 1 as the `write_ts`.
///
/// Similarly modeling Materialize, the since of the shard is kept following the
/// upper. To keep things simple, this is done by a fixed offset. This exercises
/// persist compaction.
///
/// To ensure that both [ReadHandle::snapshot] and [ReadHandle::listen] are
/// exercised, when a txn reads the state at `read_ts`, it artificially picks an
/// `as_of` timestamp in `[since, read_ts]` and splits up the read data between
/// snapshot and listen along this timestamp.
#[derive(Debug)]
pub struct Transactor {
    read: ReadHandle<MaelstromKey, MaelstromVal, u64, i64>,
    write: WriteHandle<MaelstromKey, MaelstromVal, u64, i64>,

    since_ts: u64,
    read_ts: u64,
}

impl Transactor {
    pub async fn new(client: &PersistClient, shard_id: ShardId) -> Result<Self, MaelstromError> {
        let (mut write, read) = client.open(shard_id).await?;
        let since_ts = Self::extract_ts(read.since())?;
        let read_ts = Self::maybe_init_shard(&mut write).await?;
        Ok(Transactor {
            read,
            write,
            since_ts,
            read_ts,
        })
    }

    /// Initializes the shard, if it hasn't been already, and returns the read
    /// timestamp.
    async fn maybe_init_shard(
        write: &mut WriteHandle<MaelstromKey, MaelstromVal, u64, i64>,
    ) -> Result<u64, MaelstromError> {
        debug!("Transactor::maybe_init");
        const EMPTY_UPDATES: &[((MaelstromKey, MaelstromVal), u64, i64)] = &[];
        let ts_min = u64::minimum();
        let initial_upper = Antichain::from_elem(ts_min);
        let new_upper = Antichain::from_elem(ts_min + 1);
        // Unreliable, if selected, is hooked up at this point so we need to
        // retry ExternalError here. No point in having a backoff since it's
        // also happy to use the frontier of an expected upper mismatch.
        let cas_res = loop {
            let res = write
                .compare_and_append(EMPTY_UPDATES, initial_upper.clone(), new_upper.clone())
                .await;
            match res {
                Ok(x) => break x,
                Err(err) => {
                    info!(
                        "external operation maybe_init_shard::caa failed, retrying: {}",
                        err
                    );
                    continue;
                }
            }
        };
        let read_ts = match cas_res? {
            Ok(()) => 0,
            Err(current) => Self::extract_ts(&current.0)? - 1,
        };
        Ok(read_ts)
    }

    pub async fn transact(
        &mut self,
        req_ops: &[ReqTxnOp],
    ) -> Result<Vec<ResTxnOp>, MaelstromError> {
        loop {
            trace!("transact req={:?}", req_ops);
            let state = self.read().await?;
            debug!("transact req={:?} state={:?}", req_ops, state);
            let (writes, res_ops) = Self::eval_txn(&state, req_ops);

            // NB: We do the CaS even if writes is empty, so that read-only txns
            // are also linearizable.
            let write_ts = self.read_ts + 1;
            let updates = writes
                .into_iter()
                .map(|(k, v, diff)| ((k, v), write_ts, diff));
            let expected_upper = Antichain::from_elem(write_ts);
            let new_upper = Antichain::from_elem(write_ts + 1);
            let cas_res = self
                .write
                .compare_and_append(updates, expected_upper.clone(), new_upper)
                .await??;
            match cas_res {
                Ok(()) => {
                    self.read_ts = write_ts;
                    self.advance_since().await;
                    return Ok(res_ops);
                }
                // We lost the CaS race, try again.
                Err(current) => {
                    info!(
                        "transact lost the CaS race, retrying: {:?} vs {:?}",
                        expected_upper, current
                    );
                    self.read_ts = Self::extract_ts(&current.0)? - 1;
                    continue;
                }
            }
        }
    }

    async fn read(&self) -> Result<HashMap<MaelstromKey, MaelstromVal>, MaelstromError> {
        // We're reading as of read_ts, but we can split the read between the
        // snapshot and listen at any ts in `[since_ts, read_ts]`. Intentionally
        // pick one that uses a combination of both to get coverage.
        let as_of = Antichain::from_elem(self.read_ts);
        assert!(self.read_ts >= self.since_ts);
        let snap_ts = self.since_ts + (self.read_ts - self.since_ts) / 2;
        let snap_as_of = Antichain::from_elem(snap_ts);

        let mut snap = self
            .read
            .snapshot(snap_as_of.clone())
            .await
            .map_err(|since| MaelstromError {
                code: ErrorCode::Abort,
                text: format!(
                    "snapshot cannot serve requested as_of {} since is {:?}",
                    snap_ts,
                    since.0.as_option(),
                ),
            })?;
        let listen = self
            .read
            .clone()
            .await
            .listen(snap_as_of)
            .await
            .map_err(|since| MaelstromError {
                code: ErrorCode::Abort,
                text: format!(
                    "listen cannot serve requested as_of {} since is {:?}",
                    snap_ts,
                    since.0.as_option(),
                ),
            })?;

        let mut updates = Vec::new();
        while let Some(mut dataz) = snap.next().await {
            updates.append(&mut dataz);
        }
        trace!(
            "read updates from snapshot as_of {}: {:?}",
            snap_ts,
            updates
        );
        let listen_updates = Self::listen_through(listen, &as_of).await?;
        trace!(
            "read updates from listener as_of {} through {}: {:?}",
            snap_ts,
            self.read_ts,
            listen_updates
        );
        updates.extend(listen_updates);

        // Compute the contents of the collection as of `as_of`.
        for (_, t, _) in updates.iter_mut() {
            t.advance_by(as_of.borrow());
        }
        consolidate_updates(&mut updates);

        Self::extract_state_map(self.read_ts, updates)
    }

    async fn listen_through(
        mut listen: Listen<MaelstromKey, MaelstromVal, u64, i64>,
        frontier: &Antichain<u64>,
    ) -> Result<
        Vec<(
            (Result<MaelstromKey, String>, Result<MaelstromVal, String>),
            u64,
            i64,
        )>,
        ExternalError,
    > {
        let mut ret = Vec::new();
        loop {
            for event in listen.next().await {
                match event {
                    ListenEvent::Progress(x) => {
                        // NB: Unlike the snapshot as_of, a listener frontier is
                        // not inclusive, so we have to wait until it's > our
                        // as_of to be sure we have everything.
                        if PartialOrder::less_than(frontier, &x) {
                            return Ok(ret);
                        }
                    }
                    ListenEvent::Updates(x) => {
                        // We want the collection at as_of, so skip anything
                        // past that.
                        ret.extend(x.into_iter().filter(|(_, ts, _)| !frontier.less_than(ts)));
                    }
                }
            }
        }
    }

    fn extract_state_map(
        read_ts: u64,
        updates: Vec<(
            (Result<MaelstromKey, String>, Result<MaelstromVal, String>),
            u64,
            i64,
        )>,
    ) -> Result<HashMap<MaelstromKey, MaelstromVal>, MaelstromError> {
        let mut ret = HashMap::new();
        for ((k, v), _, d) in updates {
            if d != 1 {
                return Err(MaelstromError {
                    code: ErrorCode::Crash,
                    text: format!("invalid read at time {}", read_ts),
                });
            }
            let k = k.map_err(|err| MaelstromError {
                code: ErrorCode::Crash,
                text: format!("invalid key {}", err),
            })?;
            let v = v.map_err(|err| MaelstromError {
                code: ErrorCode::Crash,
                text: format!("invalid val {}", err),
            })?;
            if ret.contains_key(&k) {
                return Err(MaelstromError {
                    code: ErrorCode::Crash,
                    text: format!("unexpected duplicate key {:?}", k),
                });
            }
            ret.insert(k, v);
        }
        Ok(ret)
    }

    fn eval_txn(
        state: &HashMap<MaelstromKey, MaelstromVal>,
        req_ops: &[ReqTxnOp],
    ) -> (Vec<(MaelstromKey, MaelstromVal, i64)>, Vec<ResTxnOp>) {
        let mut res_ops = Vec::new();
        let mut updates = Vec::new();
        let mut txn_state = HashMap::new();

        for req_op in req_ops.iter() {
            match req_op {
                ReqTxnOp::Read { key } => {
                    let current = txn_state
                        .get(&MaelstromKey(*key))
                        .or_else(|| state.get(&MaelstromKey(*key)));
                    let val = current.cloned().unwrap_or_default().0;
                    res_ops.push(ResTxnOp::Read { key: *key, val })
                }
                ReqTxnOp::Append { key, val } => {
                    let current = txn_state
                        .get(&MaelstromKey(*key))
                        .or_else(|| state.get(&MaelstromKey(*key)));
                    let mut vals = match current {
                        Some(val) => {
                            // Retract the value we're about to overwrite.
                            updates.push((MaelstromKey(*key), val.clone(), -1));
                            val.clone()
                        }
                        None => MaelstromVal::default(),
                    };
                    vals.0.push(*val);
                    txn_state.insert(MaelstromKey(*key), vals.clone());
                    updates.push((MaelstromKey(*key), vals, 1));
                    res_ops.push(ResTxnOp::Append {
                        key: key.clone(),
                        val: *val,
                    })
                }
            }
        }

        debug!(
            "eval_txn\n  req={:?}\n  res={:?}\n  updates={:?}\n  state={:?}\n  txn_state={:?}",
            req_ops, res_ops, updates, state, txn_state
        );
        (updates, res_ops)
    }

    async fn advance_since(&mut self) {
        // To keep things interesting, advance the since.
        const SINCE_LAG: u64 = 10;
        let new_since = self.read_ts.saturating_sub(SINCE_LAG);
        debug!("downgrading since to {}", new_since);
        self.read
            .downgrade_since(Antichain::from_elem(new_since))
            .await;
        self.since_ts = new_since;
    }

    fn extract_ts<T: TotalOrder + Copy>(frontier: &Antichain<T>) -> Result<T, MaelstromError> {
        frontier.as_option().copied().ok_or_else(|| MaelstromError {
            code: ErrorCode::Crash,
            text: "shard unexpectedly closed".into(),
        })
    }
}

/// An adaptor to implement [Service] using [Transactor]
#[derive(Debug)]
pub struct TransactorService(pub Arc<Mutex<Transactor>>);

#[async_trait]
impl Service for TransactorService {
    async fn init(args: &Args, handle: &Handle) -> Result<Self, MaelstromError> {
        // Use the Maelstrom services to initialize a new random ShardId (so we
        // can repeatedly run tests against the same Blob and Consensus without
        // conflicting) and communicate it between processes.
        let shard_id = handle.maybe_init_shard_id().await?;

        // Make sure the seed is recomputed each time through the retry
        // closure, so we don't retry the same deterministic timeouts.
        let seed: u64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos()
            .into();
        // It doesn't particularly matter what we set should_happen to, so we do
        // this to have a convenient single tunable param.
        let should_happen = 1.0 - args.unreliability;
        // For consensus, set should_timeout to `args.unreliability` so that once we split
        // ExternalErrors into determinate vs indeterminate, then
        // `args.unreliability` will also be the fraction of txns that it's
        // not save for Maelstrom to retry (b/c indeterminate error in
        // Consensus CaS).
        let should_timeout = args.unreliability;
        // It doesn't particularly matter what we set should_happen and
        // should_timeout to for blobs, so use the same handle for both.
        let unreliable = UnreliableHandle::new(seed, should_happen, should_timeout);

        // Construct requested Blob.
        let blob = match &args.blob_uri {
            Some(blob_uri) => BlobConfig::try_from(blob_uri).await?.open().await?,
            None => MaelstromBlob::new(handle.clone()),
        };
        let blob =
            Arc::new(UnreliableBlob::new(blob, unreliable.clone())) as Arc<dyn Blob + Send + Sync>;
        // Normal production persist usage (even including a real SQL txn impl)
        // isn't particularly benefitted by a cache, so we don't have one baked
        // into persist. In contrast, our Maelstrom transaction model
        // intentionally exercises both a new snapshot and new listener on each
        // txn. As a result, without a cache, things would be terribly slow,
        // unreliable would cause more retries than are interesting, and the
        // Lamport diagrams that Maelstrom generates would be noisy.
        let blob = CachingBlob::new(blob);

        // Construct requested Consensus.
        let config = PersistConfig::new(SYSTEM_TIME.clone());
        let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));
        let consensus = match &args.consensus_uri {
            Some(consensus_uri) => {
                ConsensusConfig::try_from(
                    consensus_uri,
                    config.consensus_connection_pool_max_size,
                    metrics.postgres_consensus.clone(),
                )
                .await?
                .open()
                .await?
            }
            None => MaelstromConsensus::new(handle.clone()),
        };
        let consensus = Arc::new(UnreliableConsensus::new(consensus, unreliable))
            as Arc<dyn Consensus + Send + Sync>;

        // Wire up the TransactorService.
        let cpu_heavy_runtime = Arc::new(CpuHeavyRuntime::new());
        let client =
            PersistClient::new(config, blob, consensus, metrics, cpu_heavy_runtime).await?;
        let transactor = Transactor::new(&client, shard_id).await?;
        let service = TransactorService(Arc::new(Mutex::new(transactor)));
        Ok(service)
    }

    async fn eval(&self, handle: Handle, src: NodeId, req: Body) {
        match req {
            Body::ReqTxn { msg_id, txn } => {
                let in_reply_to = msg_id;
                match self.0.lock().await.transact(&txn).await {
                    Ok(txn) => handle.send_res(src, |msg_id| Body::ResTxn {
                        msg_id,
                        in_reply_to,
                        txn,
                    }),
                    Err(MaelstromError { code, text }) => {
                        handle.send_res(src, |msg_id| Body::Error {
                            msg_id: Some(msg_id),
                            in_reply_to,
                            code,
                            text,
                        })
                    }
                }
            }
            req => unimplemented!("unsupported req: {:?}", req),
        }
    }
}

mod codec_impls {
    use mz_persist_types::Codec;

    use crate::maelstrom::txn::{MaelstromKey, MaelstromVal};

    impl Codec for MaelstromKey {
        fn codec_name() -> String {
            "MaelstromKey".into()
        }

        fn encode<B>(&self, buf: &mut B)
        where
            B: bytes::BufMut,
        {
            let bytes = serde_json::to_vec(&self.0).expect("failed to encode key");
            buf.put(bytes.as_slice());
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            Ok(MaelstromKey(
                serde_json::from_slice(buf).map_err(|err| err.to_string())?,
            ))
        }
    }

    impl Codec for MaelstromVal {
        fn codec_name() -> String {
            "MaelstromVal".into()
        }

        fn encode<B>(&self, buf: &mut B)
        where
            B: bytes::BufMut,
        {
            let bytes = serde_json::to_vec(&self.0).expect("failed to encode val");
            buf.put(bytes.as_slice());
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            Ok(MaelstromVal(
                serde_json::from_slice(buf).map_err(|err| err.to_string())?,
            ))
        }
    }
}
