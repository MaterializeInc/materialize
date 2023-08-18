// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An implementation of the Maelstrom txn-list-append workload using the
//! multi-shard txn abstraction.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use differential_dataflow::consolidation::consolidate_updates;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist::cfg::{BlobConfig, ConsensusConfig};
use mz_persist::location::{Blob, Consensus};
use mz_persist::unreliable::{UnreliableBlob, UnreliableConsensus, UnreliableHandle};
use mz_persist_client::async_runtime::IsolatedRuntime;
use mz_persist_client::cache::StateCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::metrics::Metrics;
use mz_persist_client::read::{ListenEvent, ReadHandle};
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_txn::txns::TxnsHandle;
use mz_persist_types::codec_impls::{UnitSchema, VecU8Schema};
use timely::progress::Antichain;
use tokio::sync::Mutex;
use tracing::{debug, info, trace};

use crate::maelstrom::api::{Body, MaelstromError, NodeId, ReqTxnOp, ResTxnOp};
use crate::maelstrom::node::{Handle, Service};
use crate::maelstrom::services::{CachingBlob, MaelstromBlob, MaelstromConsensus};
use crate::maelstrom::Args;

#[derive(Debug)]
pub struct Transactor {
    txns_id: ShardId,
    oracle: LocalOracle,
    client: PersistClient,
    txns: TxnsHandle,
    data_reads: BTreeMap<ShardId, (u64, ReadHandle<Vec<u8>, (), u64, i64>)>,
}

impl Transactor {
    pub async fn new(client: PersistClient, txns_id: ShardId) -> Result<Self, MaelstromError> {
        let mut oracle = LocalOracle::new();
        let init_ts = oracle.write_ts();
        let txns = TxnsHandle::open(init_ts, client.clone(), txns_id).await;
        Ok(Transactor {
            txns_id,
            oracle,
            txns,
            client,
            data_reads: BTreeMap::default(),
        })
    }

    pub async fn transact(
        &mut self,
        req_ops: &[ReqTxnOp],
    ) -> Result<Vec<ResTxnOp>, MaelstromError> {
        let mut read_ids = Vec::new();
        let mut writes = BTreeMap::<ShardId, Vec<(Vec<u8>, i64)>>::new();
        for op in req_ops {
            match op {
                ReqTxnOp::Read { key } => {
                    read_ids.push(self.key_shard(*key));
                }
                ReqTxnOp::Append { key, val } => writes
                    .entry(self.key_shard(*key))
                    .or_default()
                    .push((val.to_le_bytes().to_vec(), 1)),
            }
        }

        // First create and register any data shards as necessary.
        for data_id in writes.keys().chain(read_ids.iter()) {
            let init_ts = self.ensure_registered(data_id).await;
            self.oracle.read_ts = std::cmp::max(self.oracle.read_ts, init_ts)
        }

        // Run the core read+write, retry-at-a-higher-ts-on-conflict loop.
        let mut read_ts = self.oracle.read_ts();
        debug!("read ts {}", read_ts);
        let mut reads = self.read_at(read_ts, read_ids.iter()).await;
        if writes.is_empty() {
            debug!("req committed at read_ts={}", read_ts);
        } else {
            let mut txn = self.txns.begin();
            for (data_id, writes) in writes {
                for (data, diff) in writes {
                    txn.write(&data_id, data, diff).await;
                }
            }
            let mut write_ts = self.oracle.write_ts();
            debug!("write ts {}", write_ts);
            loop {
                match txn.commit_at(&mut self.txns, write_ts).await {
                    Ok(maintenance) => {
                        debug!("req committed at read_ts={} write_ts={}", read_ts, write_ts);
                        let () = maintenance.apply(&mut self.txns).await;
                        break;
                    }
                    Err(_current) => {
                        // Have to redo our reads.
                        read_ts = self.oracle.read_ts();
                        debug!("read ts {}", read_ts);
                        write_ts = self.oracle.write_ts();
                        debug!("write ts {}", write_ts);
                        // TODO(txn): Read this incrementally between the old
                        // and new read timestamps, instead.
                        reads = self.read_at(read_ts, read_ids.iter()).await;
                        continue;
                    }
                }
            }
        }

        // Normally, txns would have to be all reads followed by all writes. To
        // support any txn-list-append txns, this map is filled in with writes
        // from _this_ txn as we walk through the request, allowing us to append
        // them to reads.
        let mut this_txn_writes = BTreeMap::<_, Vec<_>>::new();

        let res = req_ops
            .iter()
            .map(|op| match op {
                ReqTxnOp::Read { key } => {
                    let key_shard = self.key_shard(*key);
                    let mut data = reads
                        .get(&key_shard)
                        .expect("key should have been read")
                        .iter()
                        .map(|(k, t, d)| {
                            let k = u64::from_le_bytes(
                                <[u8; 8]>::try_from(k.as_slice()).expect("valid u64"),
                            );
                            (k, *t, *d)
                        })
                        .collect::<Vec<_>>();
                    let mut seen = BTreeSet::new();
                    let mut val = Vec::new();
                    consolidate_updates(&mut data);
                    // Sort things in commit (ts) order, then by key, then with
                    // insertions before retractions (so we can assert that
                    // retractions mean removal from the `seen` map).
                    data.sort_by_key(|(k, t, d)| (*t, *k, std::cmp::Reverse(*d)));
                    debug!(
                        "{} {:.9} read after sort {:?}",
                        key,
                        key_shard.to_string(),
                        data
                    );
                    for (x, _, d) in data {
                        if d == 1 {
                            assert!(seen.insert(x));
                            val.push(x);
                        } else if d == -1 {
                            assert!(seen.remove(&x));
                            val.retain(|y| *y != x);
                        } else {
                            panic!("unexpected diff: {}", d);
                        }
                    }
                    if let Some(this_writes) = this_txn_writes.get(key) {
                        val.extend(this_writes.iter().copied());
                    }
                    ResTxnOp::Read { key: *key, val }
                }
                ReqTxnOp::Append { key, val } => {
                    this_txn_writes.entry(key).or_default().push(val);
                    ResTxnOp::Append {
                        key: *key,
                        val: *val,
                    }
                }
            })
            .collect();
        Ok(res)
    }

    // Returns the minimum timestamp at which this can be read.
    async fn ensure_registered(&mut self, data_id: &ShardId) -> u64 {
        // Already registered.
        if let Some((init_ts, _)) = self.data_reads.get(data_id) {
            return *init_ts;
        }

        // Not registered
        let data_read = self
            .client
            .open_leased_reader(
                *data_id,
                Arc::new(VecU8Schema),
                Arc::new(UnitSchema),
                Diagnostics::from_purpose("txn data"),
            )
            .await
            .expect("data schema shouldn't change");

        let mut init_ts = self.oracle.write_ts();
        loop {
            let res = self.txns.register(*data_id, init_ts).await;
            match res {
                Ok(()) => {
                    self.data_reads.insert(*data_id, (init_ts, data_read));
                    return init_ts;
                }
                Err(new_init_ts) => {
                    debug!(
                        "register {:.9} at {} mismatch current={}",
                        data_id, init_ts, new_init_ts
                    );
                    assert!(init_ts < new_init_ts);
                    init_ts = new_init_ts;
                    continue;
                }
            }
        }
    }

    async fn read_at(
        &mut self,
        read_ts: u64,
        data_ids: impl Iterator<Item = &ShardId>,
    ) -> BTreeMap<ShardId, Vec<(Vec<u8>, u64, i64)>> {
        // Ensure these reads don't block.
        let () = self.txns.apply_le(read_ts).await;

        let mut reads = BTreeMap::new();
        for data_id in data_ids {
            let (_, data_read) = self
                .data_reads
                .get(data_id)
                .expect("data handle was registered");
            let data_read = data_read.clone("read_at").await;
            // SUBTLE! Maelstrom txn-list-append requires that we be able to
            // reconstruct the order in which we appended list items. To avoid
            // needing to change the staged writes if our read_ts advances, we
            // instead do something overly clever and use the update timestamps.
            // To recover them, instead of grabbing a snapshot at the read_ts,
            // we have to start a subscription at time 0 and walk it forward
            // until we pass read_ts.
            //
            // TODO: It would be lovely to do this without cloning the
            // ReadHandle, but that would require adding a method to fetch the
            // un-advanced data in some `(lower, upper)`.
            let target_inclusive = self
                .txns
                .read_cache()
                .to_data_inclusive(data_id, read_ts)
                .expect("data shard was registered");
            let mut subscribe = data_read
                .subscribe(Antichain::from_elem(0))
                .await
                .expect("data shard is not compacted");
            let mut data = Vec::new();
            let mut progress_exclusive = 0;
            while progress_exclusive <= target_inclusive {
                let events = subscribe.fetch_next().await;
                for event in events {
                    match event {
                        ListenEvent::Progress(x) => {
                            progress_exclusive = *x.as_option().expect("data shard not closed");
                            trace!(
                                "{} read {} progress: {}",
                                data_id,
                                target_inclusive,
                                progress_exclusive
                            );
                        }
                        ListenEvent::Updates(x) => {
                            let updates = x
                                .into_iter()
                                .map(|((k, v), t, d)| {
                                    let (k, ()) = (k.unwrap(), v.unwrap());
                                    (k, t, d)
                                })
                                .filter(|(_, t, _)| *t <= target_inclusive)
                                .map(|x| {
                                    trace!("{} read {} data: {:?}", data_id, target_inclusive, x);
                                    x
                                });
                            data.extend(updates);
                        }
                    }
                }
            }
            reads.insert(*data_id, data);
        }
        reads
    }

    // Constructs a ShardId that is stable per key (so each maelstrom process
    // gets the same one) and per txns_id (so runs of maelstrom don't interfere
    // with each other).
    fn key_shard(&self, key: u64) -> ShardId {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        self.txns_id.hash(&mut h);
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&h.finish().to_le_bytes());
        let shard_id = format!("s{}", uuid::Uuid::from_bytes(buf));
        shard_id.parse().expect("valid shard id")
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

        let mut config = PersistConfig::new(&mz_persist_client::BUILD_INFO, SYSTEM_TIME.clone());
        let metrics = Arc::new(Metrics::new(&config, &MetricsRegistry::new()));

        // Construct requested Blob.
        let blob = match &args.blob_uri {
            Some(blob_uri) => {
                let cfg = BlobConfig::try_from(
                    blob_uri,
                    Box::new(config.clone()),
                    metrics.s3_blob.clone(),
                )
                .await
                .expect("blob_uri should be valid");
                loop {
                    match cfg.clone().open().await {
                        Ok(x) => break x,
                        Err(err) => {
                            info!("failed to open blob, trying again: {}", err);
                        }
                    }
                }
            }
            None => MaelstromBlob::new(handle.clone()),
        };
        let blob: Arc<dyn Blob + Send + Sync> =
            Arc::new(UnreliableBlob::new(blob, unreliable.clone()));
        // Normal production persist usage (even including a real SQL txn impl)
        // isn't particularly benefitted by a cache, so we don't have one baked
        // into persist. In contrast, our Maelstrom transaction model
        // intentionally exercises both a new snapshot and new listener on each
        // txn. As a result, without a cache, things would be terribly slow,
        // unreliable would cause more retries than are interesting, and the
        // Lamport diagrams that Maelstrom generates would be noisy.
        let blob = CachingBlob::new(blob);
        // to simplify some downstream logic (+ a bit more stress testing),
        // always downgrade the since of critical handles when asked
        config.critical_downgrade_interval = Duration::from_secs(0);
        // set a live diff scan limit such that we'll explore both the fast and slow paths
        config.set_state_versions_recent_live_diffs_limit(5);
        let consensus = match &args.consensus_uri {
            Some(consensus_uri) => {
                let cfg = ConsensusConfig::try_from(
                    consensus_uri,
                    Box::new(config.clone()),
                    metrics.postgres_consensus.clone(),
                )
                .expect("consensus_uri should be valid");
                loop {
                    match cfg.clone().open().await {
                        Ok(x) => break x,
                        Err(err) => {
                            info!("failed to open consensus, trying again: {}", err);
                        }
                    }
                }
            }
            None => MaelstromConsensus::new(handle.clone()),
        };
        let consensus: Arc<dyn Consensus + Send + Sync> =
            Arc::new(UnreliableConsensus::new(consensus, unreliable));

        // Wire up the TransactorService.
        let isolated_runtime = Arc::new(IsolatedRuntime::new());
        let pubsub_sender = PubSubClientConnection::noop().sender;
        let shared_states = Arc::new(StateCache::new(
            &config,
            Arc::clone(&metrics),
            Arc::clone(&pubsub_sender),
        ));
        let client = PersistClient::new(
            config,
            blob,
            consensus,
            metrics,
            isolated_runtime,
            shared_states,
            pubsub_sender,
        )?;
        let transactor = Transactor::new(client, shard_id).await?;
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

#[derive(Debug)]
struct LocalOracle {
    read_ts: u64,
}

impl LocalOracle {
    fn new() -> Self {
        LocalOracle { read_ts: 0 }
    }
}

impl LocalOracle {
    fn write_ts(&mut self) -> u64 {
        // TODO(txn): Make this linearizable, potentially using the maelstrom
        // "lin-tso" service
        self.read_ts = self.read_ts + 1;
        self.read_ts
    }
    fn read_ts(&mut self) -> u64 {
        self.read_ts
    }
}
