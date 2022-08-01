// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus monitoring metrics.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use mz_ore::cast::CastFrom;
use mz_ore::metric;
use mz_ore::metrics::{ComputedIntGauge, ComputedUIntGauge, Counter, IntCounter, MetricsRegistry};
use mz_persist::location::{
    Atomicity, Blob, BlobMetadata, Consensus, ExternalError, SeqNo, VersionedData,
};
use mz_persist::retry::RetryStream;
use mz_persist_types::Codec64;
use prometheus::core::{Atomic, AtomicI64, AtomicU64};
use prometheus::{CounterVec, IntCounterVec};
use timely::progress::Antichain;

use crate::ShardId;

/// Prometheus monitoring metrics.
///
/// Intentionally not Clone because we expect this to be passed around in an
/// Arc.
#[derive(Debug)]
pub struct Metrics {
    _vecs: MetricsVecs,

    /// Metrics for [Blob] usage.
    pub blob: BlobMetrics,
    /// Metrics for [Consensus] usage.
    pub consensus: ConsensusMetrics,
    /// Metrics of command evaluation.
    pub cmds: CmdsMetrics,
    /// Metrics for each retry loop.
    pub retries: RetriesMetrics,
    /// Metrics for batches written directly on behalf of a user (BatchBuilder
    /// or one of the sugar methods that use it).
    pub user: BatchWriteMetrics,
    /// Metrics for compaction.
    pub compaction: CompactionMetrics,
    /// Metrics for garbage collection.
    pub gc: GcMetrics,
    /// Metrics for leasing and automatic lease expiry.
    pub lease: LeaseMetrics,
    /// Metrics for various encodings and decodings.
    pub codecs: CodecsMetrics,
    /// Metrics for various per-shard measurements.
    pub shards: ShardsMetrics,
}

impl Metrics {
    /// Returns a new [Metrics] instance connected to the given registry.
    pub fn new(registry: &MetricsRegistry) -> Self {
        let vecs = MetricsVecs::new(registry);
        Metrics {
            blob: vecs.blob_metrics(),
            consensus: vecs.consensus_metrics(),
            cmds: vecs.cmds_metrics(),
            retries: vecs.retries_metrics(),
            codecs: vecs.codecs_metrics(),
            user: BatchWriteMetrics::new(registry, "user"),
            compaction: CompactionMetrics::new(registry),
            gc: GcMetrics::new(registry),
            lease: LeaseMetrics::new(registry),
            shards: ShardsMetrics::new(registry),
            _vecs: vecs,
        }
    }

    /// Returns the current lifetime write amplification reflected in these
    /// metrics.
    ///
    /// Only exposed for tests, persistcli, and benchmarks.
    pub fn write_amplification(&self) -> f64 {
        // This intentionally uses "bytes" for total and "goodbytes" for user so
        // that the overhead of our blob format is included.
        let total_written = self.blob.set.bytes.get();
        let user_written = self.user.goodbytes.get();
        #[allow(clippy::cast_precision_loss)]
        {
            total_written as f64 / user_written as f64
        }
    }
}

#[derive(Debug)]
struct MetricsVecs {
    cmd_started: IntCounterVec,
    cmd_cas_mismatch: IntCounterVec,
    cmd_succeeded: IntCounterVec,
    cmd_failed: IntCounterVec,
    cmd_seconds: CounterVec,

    external_op_started: IntCounterVec,
    external_op_succeeded: IntCounterVec,
    external_op_failed: IntCounterVec,
    external_op_bytes: IntCounterVec,
    external_op_seconds: CounterVec,
    external_consensus_truncated_count: IntCounter,
    external_blob_delete_noop_count: IntCounter,

    retry_started: IntCounterVec,
    retry_finished: IntCounterVec,
    retry_retries: IntCounterVec,
    retry_sleep_seconds: CounterVec,

    encode_count: IntCounterVec,
    encode_seconds: CounterVec,
    decode_count: IntCounterVec,
    decode_seconds: CounterVec,
}

impl MetricsVecs {
    fn new(registry: &MetricsRegistry) -> Self {
        MetricsVecs {
            cmd_started: registry.register(metric!(
                name: "mz_persist_cmd_started_count",
                help: "count of commands started",
                var_labels: ["cmd"],
            )),
            cmd_cas_mismatch: registry.register(metric!(
                name: "mz_persist_cmd_cas_mismatch_count",
                help: "count of command retries from CaS mismatch",
                var_labels: ["cmd"],
            )),
            cmd_succeeded: registry.register(metric!(
                name: "mz_persist_cmd_succeeded_count",
                help: "count of commands succeeded",
                var_labels: ["cmd"],
            )),
            cmd_failed: registry.register(metric!(
                name: "mz_persist_cmd_failed_count",
                help: "count of commands failed",
                var_labels: ["cmd"],
            )),
            cmd_seconds: registry.register(metric!(
                name: "mz_persist_cmd_seconds",
                help: "time spent applying commands",
                var_labels: ["cmd"],
            )),

            external_op_started: registry.register(metric!(
                name: "mz_persist_external_started_count",
                help: "count of external service calls started",
                var_labels: ["op"],
            )),
            external_op_succeeded: registry.register(metric!(
                name: "mz_persist_external_succeeded_count",
                help: "count of external service calls succeeded",
                var_labels: ["op"],
            )),
            external_op_failed: registry.register(metric!(
                name: "mz_persist_external_failed_count",
                help: "count of external service calls failed",
                var_labels: ["op"],
            )),
            external_op_bytes: registry.register(metric!(
                name: "mz_persist_external_bytes_count",
                help: "total size represented by external service calls",
                var_labels: ["op"],
            )),
            external_op_seconds: registry.register(metric!(
                name: "mz_persist_external_seconds",
                help: "time spent in external service calls",
                var_labels: ["op"],
            )),
            external_consensus_truncated_count: registry.register(metric!(
                name: "mz_persist_external_consensus_truncated_count",
                help: "count of versions deleted by consensus truncate calls",
            )),
            external_blob_delete_noop_count: registry.register(metric!(
                name: "mz_persist_external_blob_delete_noop_count",
                help: "count of blob delete calls that deleted a non-existent key",
            )),

            retry_started: registry.register(metric!(
                name: "mz_persist_retry_started_count",
                help: "count of retry loops started",
                var_labels: ["op"],
            )),
            retry_finished: registry.register(metric!(
                name: "mz_persist_retry_finished_count",
                help: "count of retry loops finished",
                var_labels: ["op"],
            )),
            retry_retries: registry.register(metric!(
                name: "mz_persist_retry_retries_count",
                help: "count of total attempts by retry loops",
                var_labels: ["op"],
            )),
            retry_sleep_seconds: registry.register(metric!(
                name: "mz_persist_retry_sleep_seconds",
                help: "time spent in retry loop backoff",
                var_labels: ["op"],
            )),

            encode_count: registry.register(metric!(
                name: "mz_persist_encode_count",
                help: "count of op encodes",
                var_labels: ["op"],
            )),
            encode_seconds: registry.register(metric!(
                name: "mz_persist_encode_seconds",
                help: "time spent in op encodes",
                var_labels: ["op"],
            )),
            decode_count: registry.register(metric!(
                name: "mz_persist_decode_count",
                help: "count of op decodes",
                var_labels: ["op"],
            )),
            decode_seconds: registry.register(metric!(
                name: "mz_persist_decode_seconds",
                help: "time spent in op decodes",
                var_labels: ["op"],
            )),
        }
    }

    fn cmds_metrics(&self) -> CmdsMetrics {
        CmdsMetrics {
            init_state: self.cmd_metrics("init_state"),
            register: self.cmd_metrics("register"),
            clone_reader: self.cmd_metrics("clone_reader"),
            compare_and_append: self.cmd_metrics("compare_and_append"),
            downgrade_since: self.cmd_metrics("downgrade_since"),
            heartbeat_reader: self.cmd_metrics("heartbeat_reader"),
            expire_reader: self.cmd_metrics("expire_reader"),
            expire_writer: self.cmd_metrics("expire_writer"),
            merge_res: self.cmd_metrics("merge_res"),
        }
    }

    fn cmd_metrics(&self, cmd: &str) -> CmdMetrics {
        CmdMetrics {
            name: cmd.to_owned(),
            started: self.cmd_started.with_label_values(&[cmd]),
            succeeded: self.cmd_succeeded.with_label_values(&[cmd]),
            cas_mismatch: self.cmd_cas_mismatch.with_label_values(&[cmd]),
            failed: self.cmd_failed.with_label_values(&[cmd]),
            seconds: self.cmd_seconds.with_label_values(&[cmd]),
        }
    }

    fn retries_metrics(&self) -> RetriesMetrics {
        RetriesMetrics {
            determinate: RetryDeterminate {
                apply_unbatched_cmd_cas: self.retry_metrics("apply_unbatched_cmd::cas"),
            },
            external: RetryExternal {
                batch_set: self.retry_metrics("batch::set"),
                blob_open: self.retry_metrics("blob::open"),
                compaction_noop_delete: self.retry_metrics("compaction_noop::delete"),
                consensus_open: self.retry_metrics("consensus::open"),
                fetch_and_update_state_head: self.retry_metrics("fetch_and_update_state::head"),
                fetch_batch_get: self.retry_metrics("fetch_batch::get"),
                maybe_init_state_cas: self.retry_metrics("maybe_init_state::cas"),
                maybe_init_state_head: self.retry_metrics("maybe_init_state::head"),
                gc_scan: self.retry_metrics("gc::scan"),
                gc_delete: self.retry_metrics("gc::delete"),
                gc_truncate: self.retry_metrics("gc::truncate"),
                storage_usage_shard_size: self.retry_metrics("storage_usage::shard_size"),
            },
            append_batch: self.retry_metrics("append_batch"),
            fetch_batch_part: self.retry_metrics("fetch_batch_part"),
            idempotent_cmd: self.retry_metrics("idempotent_cmd"),
            next_listen_batch: self.retry_metrics("next_listen_batch"),
            snapshot: self.retry_metrics("snapshot"),
        }
    }

    fn retry_metrics(&self, name: &str) -> RetryMetrics {
        RetryMetrics {
            name: name.to_owned(),
            started: self.retry_started.with_label_values(&[name]),
            finished: self.retry_finished.with_label_values(&[name]),
            retries: self.retry_retries.with_label_values(&[name]),
            sleep_seconds: self.retry_sleep_seconds.with_label_values(&[name]),
        }
    }

    fn codecs_metrics(&self) -> CodecsMetrics {
        CodecsMetrics {
            state: self.codec_metrics("state"),
            batch: self.codec_metrics("batch"),
            key: self.codec_metrics("key"),
            val: self.codec_metrics("val"),
        }
    }

    fn codec_metrics(&self, op: &str) -> CodecMetrics {
        CodecMetrics {
            encode_count: self.encode_count.with_label_values(&[op]),
            encode_seconds: self.encode_seconds.with_label_values(&[op]),
            decode_count: self.decode_count.with_label_values(&[op]),
            decode_seconds: self.decode_seconds.with_label_values(&[op]),
        }
    }

    fn blob_metrics(&self) -> BlobMetrics {
        BlobMetrics {
            set: self.external_op_metrics("blob_set"),
            get: self.external_op_metrics("blob_get"),
            list_keys: self.external_op_metrics("blob_list_keys"),
            delete: self.external_op_metrics("blob_delete"),
            delete_noop: self.external_blob_delete_noop_count.clone(),
        }
    }

    fn consensus_metrics(&self) -> ConsensusMetrics {
        ConsensusMetrics {
            head: self.external_op_metrics("consensus_head"),
            compare_and_set: self.external_op_metrics("consensus_cas"),
            scan: self.external_op_metrics("consensus_scan"),
            truncate: self.external_op_metrics("consensus_truncate"),
            truncated_count: self.external_consensus_truncated_count.clone(),
        }
    }

    fn external_op_metrics(&self, op: &str) -> ExternalOpMetrics {
        ExternalOpMetrics {
            started: self.external_op_started.with_label_values(&[op]),
            succeeded: self.external_op_succeeded.with_label_values(&[op]),
            failed: self.external_op_failed.with_label_values(&[op]),
            bytes: self.external_op_bytes.with_label_values(&[op]),
            seconds: self.external_op_seconds.with_label_values(&[op]),
        }
    }
}

#[derive(Debug)]
pub struct CmdCasMismatchMetric(pub(crate) IntCounter);

#[derive(Debug)]
pub struct CmdMetrics {
    pub(crate) name: String,
    pub(crate) started: IntCounter,
    pub(crate) cas_mismatch: IntCounter,
    pub(crate) succeeded: IntCounter,
    pub(crate) failed: IntCounter,
    pub(crate) seconds: Counter,
}

impl CmdMetrics {
    pub async fn run_cmd<R, E, F, CmdFn>(&self, cmd_fn: CmdFn) -> Result<R, E>
    where
        F: std::future::Future<Output = Result<R, E>>,
        CmdFn: FnOnce(CmdCasMismatchMetric) -> F,
    {
        self.started.inc();
        let start = Instant::now();
        let res = cmd_fn(CmdCasMismatchMetric(self.cas_mismatch.clone())).await;
        self.seconds.inc_by(start.elapsed().as_secs_f64());
        match res.as_ref() {
            Ok(_) => self.succeeded.inc(),
            Err(_) => self.failed.inc(),
        };
        res
    }
}

#[derive(Debug)]
pub struct CmdsMetrics {
    pub(crate) init_state: CmdMetrics,
    pub(crate) register: CmdMetrics,
    pub(crate) clone_reader: CmdMetrics,
    pub(crate) compare_and_append: CmdMetrics,
    pub(crate) downgrade_since: CmdMetrics,
    pub(crate) heartbeat_reader: CmdMetrics,
    pub(crate) expire_reader: CmdMetrics,
    pub(crate) expire_writer: CmdMetrics,
    pub(crate) merge_res: CmdMetrics,
}

#[derive(Debug)]
pub struct RetryMetrics {
    pub(crate) name: String,
    pub(crate) started: IntCounter,
    pub(crate) finished: IntCounter,
    pub(crate) retries: IntCounter,
    pub(crate) sleep_seconds: Counter,
}

impl RetryMetrics {
    pub(crate) fn stream(&self, retry: RetryStream) -> MetricsRetryStream {
        MetricsRetryStream::new(retry, self)
    }
}

#[derive(Debug)]
pub struct RetryDeterminate {
    pub(crate) apply_unbatched_cmd_cas: RetryMetrics,
}

#[derive(Debug)]
pub struct RetryExternal {
    pub(crate) batch_set: RetryMetrics,
    pub(crate) blob_open: RetryMetrics,
    pub(crate) consensus_open: RetryMetrics,
    pub(crate) compaction_noop_delete: RetryMetrics,
    pub(crate) fetch_and_update_state_head: RetryMetrics,
    pub(crate) fetch_batch_get: RetryMetrics,
    pub(crate) maybe_init_state_cas: RetryMetrics,
    pub(crate) maybe_init_state_head: RetryMetrics,
    pub(crate) gc_scan: RetryMetrics,
    pub(crate) gc_delete: RetryMetrics,
    pub(crate) gc_truncate: RetryMetrics,
    pub(crate) storage_usage_shard_size: RetryMetrics,
}

#[derive(Debug)]
pub struct RetriesMetrics {
    pub(crate) determinate: RetryDeterminate,
    pub(crate) external: RetryExternal,

    pub(crate) append_batch: RetryMetrics,
    pub(crate) fetch_batch_part: RetryMetrics,
    pub(crate) idempotent_cmd: RetryMetrics,
    pub(crate) next_listen_batch: RetryMetrics,
    pub(crate) snapshot: RetryMetrics,
}

// This one is Clone in contrast to the others because it has to get moved into
// a task.
#[derive(Debug, Clone)]
pub struct BatchWriteMetrics {
    pub(crate) bytes: IntCounter,
    pub(crate) goodbytes: IntCounter,
}

impl BatchWriteMetrics {
    fn new(registry: &MetricsRegistry, name: &str) -> Self {
        BatchWriteMetrics {
            bytes: registry.register(metric!(
                name: format!("mz_persist_{}_bytes", name),
                help: format!("total encoded size of {} batches written", name),
            )),
            goodbytes: registry.register(metric!(
                name: format!("mz_persist_{}_goodbytes", name),
                help: format!("total logical size of {} batches written", name),
            )),
        }
    }
}

#[derive(Debug)]
pub struct CompactionMetrics {
    pub(crate) skipped: IntCounter,
    pub(crate) started: IntCounter,
    pub(crate) applied: IntCounter,
    pub(crate) failed: IntCounter,
    pub(crate) noop: IntCounter,
    pub(crate) seconds: Counter,

    pub(crate) batch: BatchWriteMetrics,
}

impl CompactionMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        CompactionMetrics {
            skipped: registry.register(metric!(
                name: "mz_persist_compaction_skipped",
                help: "count of compactions skipped due to heuristics",
            )),
            started: registry.register(metric!(
                name: "mz_persist_compaction_started",
                help: "count of compactions started",
            )),
            failed: registry.register(metric!(
                name: "mz_persist_compaction_failed",
                help: "count of compactions failed",
            )),
            applied: registry.register(metric!(
                name: "mz_persist_compaction_applied",
                help: "count of compactions applied to state",
            )),
            noop: registry.register(metric!(
                name: "mz_persist_compaction_noop",
                help: "count of compactions discarded (obsolete)",
            )),
            seconds: registry.register(metric!(
                name: "mz_persist_compaction_seconds",
                help: "time spent in compaction",
            )),
            batch: BatchWriteMetrics::new(registry, "compaction"),
        }
    }
}

#[derive(Debug)]
pub struct GcMetrics {
    pub(crate) skipped: IntCounter,
    pub(crate) started: IntCounter,
    pub(crate) finished: IntCounter,
    pub(crate) seconds: Counter,
}

impl GcMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        GcMetrics {
            skipped: registry.register(metric!(
                name: "mz_persist_gc_skipped",
                help: "count of garbage collections skipped due to heuristics",
            )),
            started: registry.register(metric!(
                name: "mz_persist_gc_started",
                help: "count of garbage collections started",
            )),
            finished: registry.register(metric!(
                name: "mz_persist_gc_finished",
                help: "count of garbage collections finished",
            )),
            seconds: registry.register(metric!(
                name: "mz_persist_gc_seconds",
                help: "time spent in garbage collections",
            )),
        }
    }
}

#[derive(Debug)]
pub struct LeaseMetrics {
    pub(crate) timeout_read: IntCounter,
}

impl LeaseMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        LeaseMetrics {
            timeout_read: registry.register(metric!(
                name: "mz_persist_lease_timeout_read",
                help: "count of readers whose lease timed out",
            )),
        }
    }
}

struct IncOnDrop(IntCounter);

impl Drop for IncOnDrop {
    fn drop(&mut self) {
        self.0.inc()
    }
}

pub struct MetricsRetryStream {
    retry: RetryStream,
    retries: IntCounter,
    sleep_seconds: Counter,
    _finished: IncOnDrop,
}

impl MetricsRetryStream {
    pub fn new(retry: RetryStream, metrics: &RetryMetrics) -> Self {
        metrics.started.inc();
        MetricsRetryStream {
            retry,
            retries: metrics.retries.clone(),
            sleep_seconds: metrics.sleep_seconds.clone(),
            _finished: IncOnDrop(metrics.finished.clone()),
        }
    }

    /// How many times [Self::sleep] has been called.
    pub fn attempt(&self) -> usize {
        self.retry.attempt()
    }

    /// The next sleep (without jitter for easy printing in logs).
    pub fn next_sleep(&self) -> Duration {
        self.retry.next_sleep()
    }

    /// Executes the next sleep in the series.
    ///
    /// This isn't cancel-safe, so it consumes and returns self, to prevent
    /// accidental mis-use.
    pub async fn sleep(self) -> Self {
        self.retries.inc();
        self.sleep_seconds
            .inc_by(self.retry.next_sleep().as_secs_f64());
        let retry = self.retry.sleep().await;
        MetricsRetryStream {
            retry,
            retries: self.retries,
            sleep_seconds: self.sleep_seconds,
            _finished: self._finished,
        }
    }
}

#[derive(Debug)]
pub struct CodecsMetrics {
    pub(crate) state: CodecMetrics,
    pub(crate) batch: CodecMetrics,
    pub(crate) key: CodecMetrics,
    pub(crate) val: CodecMetrics,
    // Intentionally not adding time and diff because they're just
    // `{to,from}_le_bytes`.
}

#[derive(Debug)]
pub struct CodecMetrics {
    pub(crate) encode_count: IntCounter,
    pub(crate) encode_seconds: Counter,
    pub(crate) decode_count: IntCounter,
    pub(crate) decode_seconds: Counter,
}

impl CodecMetrics {
    pub(crate) fn encode<R, F: FnOnce() -> R>(&self, f: F) -> R {
        let now = Instant::now();
        let r = f();
        self.encode_count.inc();
        self.encode_seconds.inc_by(now.elapsed().as_secs_f64());
        r
    }

    pub(crate) fn decode<R, F: FnOnce() -> R>(&self, f: F) -> R {
        let now = Instant::now();
        let r = f();
        self.decode_count.inc();
        self.decode_seconds.inc_by(now.elapsed().as_secs_f64());
        r
    }
}

#[derive(Debug)]
pub struct ShardsMetrics {
    // Ideally, we'd break these down per-shard, but that might be too expensive
    // for prometheus. Instead, start with the plumbing and aggregate the
    // interesting per-shard metrics across a process (min of since, max of
    // upper, sum of state size bytes). It's not ideal, but it's definitely safe
    // to ship.
    //
    // The structure of the code makes it pretty trivial to do this swap
    // (actually tracking them per-shard) later: replace the ComputedFooGauge in
    // ShardsMetrics with a FooCounterVec and the AtomicFoo in ShardMetrics with
    // a FooCounter.
    _count: ComputedUIntGauge,
    _since: ComputedIntGauge,
    _upper: ComputedIntGauge,
    _encoded_state_size: ComputedUIntGauge,
    // We hand out `Arc<ShardMetrics>` to read and write handles, but store it
    // here as `Weak`. This allows us to discover if it's no longer in use and
    // so we can remove it from the map.
    shards: Arc<Mutex<HashMap<ShardId, Weak<ShardMetrics>>>>,
}

impl ShardsMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        let shards = Arc::new(Mutex::new(HashMap::new()));
        let shards_count = Arc::clone(&shards);
        let shards_since = Arc::clone(&shards);
        let shards_upper = Arc::clone(&shards);
        let shards_size = Arc::clone(&shards);
        ShardsMetrics {
            _count: registry.register_computed_gauge(
                metric!(
                    name: "mz_persist_shard_count",
                    help: "count of all active shards on this process",
                ),
                move || {
                    let mut ret = 0;
                    Self::compute(&shards_count, |_m| ret += 1);
                    ret
                },
            ),
            _since: registry.register_computed_gauge(
                metric!(
                    name: "mz_persist_shard_since",
                    help: "minimum since of all active shards on this process",
                ),
                move || {
                    let mut ret = i64::MAX;
                    Self::compute(&shards_since, |m| ret = std::cmp::min(ret, m.since.get()));
                    ret
                },
            ),
            _upper: registry.register_computed_gauge(
                metric!(
                    name: "mz_persist_shard_upper",
                    help: "maximum upper of all active shards on this process",
                ),
                move || {
                    let mut ret = 0;
                    Self::compute(&shards_upper, |m| ret = std::cmp::max(ret, m.upper.get()));
                    ret
                },
            ),
            _encoded_state_size: registry.register_computed_gauge(
                metric!(
                    name: "mz_persist_shard_state_size_bytes",
                    help: "total encoded state size of all active shards on this process",
                ),
                move || {
                    let mut ret = 0;
                    Self::compute(&shards_size, |m| ret += m.encoded_state_size.get());
                    ret
                },
            ),
            shards,
        }
    }

    pub fn shard(&self, shard_id: &ShardId) -> Arc<ShardMetrics> {
        let mut shards = self.shards.lock().expect("mutex poisoned");
        if let Some(shard) = shards.get(shard_id) {
            if let Some(shard) = shard.upgrade() {
                return Arc::clone(&shard);
            } else {
                assert!(shards.remove(shard_id).is_some());
            }
        }
        let shard = Arc::new(ShardMetrics::new(self));
        assert!(shards
            .insert(shard_id.clone(), Arc::downgrade(&shard))
            .is_none());
        shard
    }

    fn compute<F: FnMut(&ShardMetrics)>(
        shards: &Arc<Mutex<HashMap<ShardId, Weak<ShardMetrics>>>>,
        mut f: F,
    ) {
        let mut shards = shards.lock().expect("mutex poisoned");
        let mut deleted_shards = Vec::new();
        for (shard_id, metrics) in shards.iter() {
            if let Some(metrics) = metrics.upgrade() {
                f(&metrics);
            } else {
                deleted_shards.push(shard_id.clone());
            }
        }
        for deleted_shard_id in deleted_shards {
            assert!(shards.remove(&deleted_shard_id).is_some());
        }
    }
}

#[derive(Debug)]
pub struct ShardMetrics {
    since: AtomicI64,
    upper: AtomicI64,
    encoded_state_size: AtomicU64,
}

impl ShardMetrics {
    pub fn new(_shards_metrics: &ShardsMetrics) -> Self {
        ShardMetrics {
            since: AtomicI64::new(0),
            upper: AtomicI64::new(0),
            encoded_state_size: AtomicU64::new(0),
        }
    }

    fn encode_ts_metric<T: Codec64>(ts: &Antichain<T>) -> i64 {
        // We have two problems in mapping a persist frontier into a metric.
        // First is that we only have a `T: Timestamp+Codec64`. Second, is
        // mapping an antichain to a single counter value. We solve both by
        // taking advantage of the fact that in practice, timestamps in mz are
        // currently always a u64 (and if we switch them, it will be to an i64).
        // This means that for all values that mz would actually produce,
        // interpreting the the encoded bytes as a little-endian i64 will work.
        // Both of them impl PartialOrder, so in practice, there will always be
        // zero or one elements in the antichain.
        match ts.elements().first() {
            Some(ts) => i64::from_le_bytes(Codec64::encode(ts)),
            None => i64::MAX,
        }
    }

    pub fn set_since<T: Codec64>(&self, since: &Antichain<T>) {
        self.since.set(Self::encode_ts_metric(since))
    }

    pub fn set_upper<T: Codec64>(&self, upper: &Antichain<T>) {
        self.upper.set(Self::encode_ts_metric(upper))
    }

    pub fn set_encoded_state_size(&self, encoded_state_size: usize) {
        self.encoded_state_size
            .set(u64::cast_from(encoded_state_size))
    }
}

#[derive(Debug)]
pub struct ExternalOpMetrics {
    started: IntCounter,
    succeeded: IntCounter,
    failed: IntCounter,
    bytes: IntCounter,
    seconds: Counter,
}

impl ExternalOpMetrics {
    async fn run_op<R, F, OpFn>(&self, op_fn: OpFn) -> Result<R, ExternalError>
    where
        F: std::future::Future<Output = Result<R, ExternalError>>,
        OpFn: FnOnce() -> F,
    {
        self.started.inc();
        let start = Instant::now();
        let res = op_fn().await;
        self.seconds.inc_by(start.elapsed().as_secs_f64());
        match res.as_ref() {
            Ok(_) => self.succeeded.inc(),
            Err(_) => self.failed.inc(),
        };
        res
    }
}

#[derive(Debug)]
pub struct BlobMetrics {
    set: ExternalOpMetrics,
    get: ExternalOpMetrics,
    list_keys: ExternalOpMetrics,
    delete: ExternalOpMetrics,
    delete_noop: IntCounter,
}

#[derive(Debug)]
pub struct MetricsBlob {
    blob: Arc<dyn Blob + Send + Sync>,
    metrics: Arc<Metrics>,
}

impl MetricsBlob {
    pub fn new(blob: Arc<dyn Blob + Send + Sync>, metrics: Arc<Metrics>) -> Self {
        MetricsBlob { blob, metrics }
    }
}

#[async_trait]
impl Blob for MetricsBlob {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ExternalError> {
        let res = self.metrics.blob.get.run_op(|| self.blob.get(key)).await;
        if let Ok(Some(value)) = res.as_ref() {
            self.metrics
                .blob
                .get
                .bytes
                .inc_by(u64::cast_from(value.len()));
        }
        res
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        let mut byte_total = 0;
        let mut instrumented = |blob_metadata: BlobMetadata| {
            byte_total += blob_metadata.size_in_bytes;
            f(blob_metadata)
        };

        let res = self
            .metrics
            .blob
            .list_keys
            .run_op(|| {
                self.blob
                    .list_keys_and_metadata(key_prefix, &mut instrumented)
            })
            .await;

        self.metrics.blob.list_keys.bytes.inc_by(byte_total);

        res
    }

    async fn set(&self, key: &str, value: Bytes, atomic: Atomicity) -> Result<(), ExternalError> {
        let bytes = value.len();
        let res = self
            .metrics
            .blob
            .set
            .run_op(|| self.blob.set(key, value, atomic))
            .await;
        if res.is_ok() {
            self.metrics.blob.set.bytes.inc_by(u64::cast_from(bytes));
        }
        res
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        let bytes = self
            .metrics
            .blob
            .delete
            .run_op(|| self.blob.delete(key))
            .await?;
        if let Some(bytes) = bytes {
            self.metrics.blob.delete.bytes.inc_by(u64::cast_from(bytes));
        } else {
            self.metrics.blob.delete_noop.inc();
        }
        Ok(bytes)
    }
}

#[derive(Debug)]
pub struct ConsensusMetrics {
    head: ExternalOpMetrics,
    compare_and_set: ExternalOpMetrics,
    scan: ExternalOpMetrics,
    truncate: ExternalOpMetrics,
    truncated_count: IntCounter,
}

#[derive(Debug)]
pub struct MetricsConsensus {
    consensus: Arc<dyn Consensus + Send + Sync>,
    metrics: Arc<Metrics>,
}

impl MetricsConsensus {
    pub fn new(consensus: Arc<dyn Consensus + Send + Sync>, metrics: Arc<Metrics>) -> Self {
        MetricsConsensus { consensus, metrics }
    }
}

#[async_trait]
impl Consensus for MetricsConsensus {
    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let res = self
            .metrics
            .consensus
            .head
            .run_op(|| self.consensus.head(key))
            .await;
        if let Ok(Some(data)) = res.as_ref() {
            self.metrics
                .consensus
                .head
                .bytes
                .inc_by(u64::cast_from(data.data.len()));
        }
        res
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<Result<(), Option<VersionedData>>, ExternalError> {
        let bytes = new.data.len();
        let res = self
            .metrics
            .consensus
            .compare_and_set
            .run_op(|| self.consensus.compare_and_set(key, expected, new))
            .await;
        if let Ok(Ok(())) = res.as_ref() {
            self.metrics
                .consensus
                .compare_and_set
                .bytes
                .inc_by(u64::cast_from(bytes));
        }
        res
    }

    async fn scan(&self, key: &str, from: SeqNo) -> Result<Vec<VersionedData>, ExternalError> {
        let res = self
            .metrics
            .consensus
            .scan
            .run_op(|| self.consensus.scan(key, from))
            .await;
        if let Ok(dataz) = res.as_ref() {
            let bytes = dataz.iter().map(|x| x.data.len()).sum();
            self.metrics
                .consensus
                .scan
                .bytes
                .inc_by(u64::cast_from(bytes));
        }
        res
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<usize, ExternalError> {
        let deleted = self
            .metrics
            .consensus
            .truncate
            .run_op(|| self.consensus.truncate(key, seqno))
            .await?;
        self.metrics
            .consensus
            .truncated_count
            .inc_by(u64::cast_from(deleted));
        Ok(deleted)
    }
}
