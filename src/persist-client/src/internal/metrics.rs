// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus monitoring metrics.

use async_stream::stream;
use mz_persist_types::stats::PartStatsMetrics;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::instrument;
use mz_ore::metric;
use mz_ore::metrics::{
    raw, ComputedGauge, ComputedIntGauge, Counter, CounterVecExt, DeleteOnDropCounter,
    DeleteOnDropGauge, GaugeVecExt, IntCounter, MakeCollector, MetricsRegistry, UIntGauge,
    UIntGaugeVec,
};
use mz_ore::stats::histogram_seconds_buckets;
use mz_persist::location::{
    Blob, BlobMetadata, CaSResult, Consensus, ExternalError, ResultStream, SeqNo, VersionedData,
};
use mz_persist::metrics::{ColumnarMetrics, S3BlobMetrics};
use mz_persist::retry::RetryStream;
use mz_persist_types::Codec64;
use mz_postgres_client::metrics::PostgresClientMetrics;
use prometheus::core::{AtomicI64, AtomicU64, Collector, Desc, GenericGauge};
use prometheus::proto::MetricFamily;
use prometheus::{CounterVec, Gauge, GaugeVec, Histogram, HistogramVec, IntCounterVec};
use timely::progress::Antichain;
use tokio_metrics::TaskMonitor;
use tracing::error;

use crate::internal::paths::BlobKey;
use crate::{PersistConfig, ShardId};

/// Prometheus monitoring metrics.
///
/// Intentionally not Clone because we expect this to be passed around in an
/// Arc.
pub struct Metrics {
    _vecs: MetricsVecs,
    _uptime: ComputedGauge,

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
    /// Metrics for reading batch parts
    pub read: BatchPartReadMetrics,
    /// Metrics for compaction.
    pub compaction: CompactionMetrics,
    /// Metrics for garbage collection.
    pub gc: GcMetrics,
    /// Metrics for leasing and automatic lease expiry.
    pub lease: LeaseMetrics,
    /// Metrics for various encodings and decodings.
    pub codecs: CodecsMetrics,
    /// Metrics for (incremental) state updates and fetches.
    pub state: StateMetrics,
    /// Metrics for various per-shard measurements.
    pub shards: ShardsMetrics,
    /// Metrics for auditing persist usage
    pub audit: UsageAuditMetrics,
    /// Metrics for locking.
    pub locks: LocksMetrics,
    /// Metrics for StateWatch.
    pub watch: WatchMetrics,
    /// Metrics for PubSub client.
    pub pubsub_client: PubSubClientMetrics,
    /// Metrics for mfp/filter pushdown.
    pub pushdown: PushdownMetrics,
    /// Metrics for consolidation.
    pub consolidation: ConsolidationMetrics,
    /// Metrics for blob caching.
    pub blob_cache_mem: BlobMemCache,
    /// Metrics for tokio tasks.
    pub tasks: TasksMetrics,
    /// Metrics for columnar data encoding and decoding.
    pub columnar: ColumnarMetrics,

    /// Metrics for the persist sink.
    pub sink: SinkMetrics,

    /// Metrics for S3-backed blob implementation
    pub s3_blob: S3BlobMetrics,
    /// Metrics for Postgres-backed consensus implementation
    pub postgres_consensus: PostgresClientMetrics,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

impl Metrics {
    /// Returns a new [Metrics] instance connected to the given registry.
    pub fn new(cfg: &PersistConfig, registry: &MetricsRegistry) -> Self {
        let vecs = MetricsVecs::new(registry);
        let start = Instant::now();
        let uptime = registry.register_computed_gauge(
            metric!(
                name: "mz_persist_metadata_seconds",
                help: "server uptime, labels are build metadata",
                const_labels: {
                    "version" => cfg.build_version,
                    "build_type" => if cfg!(release) { "release" } else { "debug" }
                },
            ),
            move || start.elapsed().as_secs_f64(),
        );
        let s3_blob = S3BlobMetrics::new(registry);
        let columnar =
            ColumnarMetrics::new(&s3_blob.lgbytes, cfg.configs.clone(), cfg.is_cc_active);
        Metrics {
            blob: vecs.blob_metrics(),
            consensus: vecs.consensus_metrics(),
            cmds: vecs.cmds_metrics(registry),
            retries: vecs.retries_metrics(),
            codecs: vecs.codecs_metrics(),
            user: BatchWriteMetrics::new(registry, "user"),
            read: vecs.batch_part_read_metrics(),
            compaction: CompactionMetrics::new(registry),
            gc: GcMetrics::new(registry),
            lease: LeaseMetrics::new(registry),
            state: StateMetrics::new(registry),
            shards: ShardsMetrics::new(registry),
            audit: UsageAuditMetrics::new(registry),
            locks: vecs.locks_metrics(),
            watch: WatchMetrics::new(registry),
            pubsub_client: PubSubClientMetrics::new(registry),
            pushdown: PushdownMetrics::new(registry),
            consolidation: ConsolidationMetrics::new(registry),
            blob_cache_mem: BlobMemCache::new(registry),
            tasks: TasksMetrics::new(registry),
            columnar,
            sink: SinkMetrics::new(registry),
            s3_blob,
            postgres_consensus: PostgresClientMetrics::new(registry, "mz_persist"),
            _vecs: vecs,
            _uptime: uptime,
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
        #[allow(clippy::as_conversions)]
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
    external_blob_sizes: Histogram,
    external_rtt_latency: GaugeVec,
    external_op_latency: HistogramVec,

    retry_started: IntCounterVec,
    retry_finished: IntCounterVec,
    retry_retries: IntCounterVec,
    retry_sleep_seconds: CounterVec,

    encode_count: IntCounterVec,
    encode_seconds: CounterVec,
    decode_count: IntCounterVec,
    decode_seconds: CounterVec,

    read_part_bytes: IntCounterVec,
    read_part_goodbytes: IntCounterVec,
    read_part_count: IntCounterVec,
    read_part_seconds: CounterVec,
    read_ts_rewrite: IntCounterVec,

    lock_acquire_count: IntCounterVec,
    lock_blocking_acquire_count: IntCounterVec,
    lock_blocking_seconds: CounterVec,

    /// A minimal set of metrics imported into honeycomb for alerting.
    alerts_metrics: Arc<AlertsMetrics>,
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
            external_blob_sizes: registry.register(metric!(
                name: "mz_persist_external_blob_sizes",
                help: "histogram of blob sizes at put time",
                buckets: mz_ore::stats::HISTOGRAM_BYTE_BUCKETS.to_vec(),
            )),
            external_rtt_latency: registry.register(metric!(
                name: "mz_persist_external_rtt_latency",
                help: "roundtrip-time to external service as seen by this process",
                var_labels: ["external"],
            )),
            external_op_latency: registry.register(metric!(
                name: "mz_persist_external_op_latency",
                help: "rountrip latency observed by individual performance-critical operations",
                var_labels: ["op"],
                // NB: If we end up overrunning metrics quotas, we could plausibly cut this
                // down by switching to a factor of 4 between buckets (vs. the standard 2).
                buckets: histogram_seconds_buckets(0.000_500, 32.0),
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

            read_part_bytes: registry.register(metric!(
                name: "mz_persist_read_batch_part_bytes",
                help: "total encoded size of batch parts read",
                var_labels: ["op"],
            )),
            read_part_goodbytes: registry.register(metric!(
                name: "mz_persist_read_batch_part_goodbytes",
                help: "total logical size of batch parts read",
                var_labels: ["op"],
            )),
            read_part_count: registry.register(metric!(
                name: "mz_persist_read_batch_part_count",
                help: "count of batch parts read",
                var_labels: ["op"],
            )),
            read_part_seconds: registry.register(metric!(
                name: "mz_persist_read_batch_part_seconds",
                help: "time spent reading batch parts",
                var_labels: ["op"],
            )),
            read_ts_rewrite: registry.register(metric!(
                name: "mz_persist_read_ts_rewite",
                help: "count of updates read with rewritten ts",
                var_labels: ["op"],
            )),

            lock_acquire_count: registry.register(metric!(
                name: "mz_persist_lock_acquire_count",
                help: "count of locks acquired",
                var_labels: ["op"],
            )),
            lock_blocking_acquire_count: registry.register(metric!(
                name: "mz_persist_lock_blocking_acquire_count",
                help: "count of locks acquired that required blocking",
                var_labels: ["op"],
            )),
            lock_blocking_seconds: registry.register(metric!(
                name: "mz_persist_lock_blocking_seconds",
                help: "time spent blocked for a lock",
                var_labels: ["op"],
            )),

            alerts_metrics: Arc::new(AlertsMetrics::new(registry)),
        }
    }

    fn cmds_metrics(&self, registry: &MetricsRegistry) -> CmdsMetrics {
        CmdsMetrics {
            init_state: self.cmd_metrics("init_state"),
            add_rollup: self.cmd_metrics("add_rollup"),
            remove_rollups: self.cmd_metrics("remove_rollups"),
            register: self.cmd_metrics("register"),
            compare_and_append: self.cmd_metrics("compare_and_append"),
            compare_and_append_noop:             registry.register(metric!(
                name: "mz_persist_cmd_compare_and_append_noop",
                help: "count of compare_and_append retries that were discoverd to have already committed",
            )),
            compare_and_downgrade_since: self.cmd_metrics("compare_and_downgrade_since"),
            downgrade_since: self.cmd_metrics("downgrade_since"),
            heartbeat_reader: self.cmd_metrics("heartbeat_reader"),
            expire_reader: self.cmd_metrics("expire_reader"),
            expire_writer: self.cmd_metrics("expire_writer"),
            merge_res: self.cmd_metrics("merge_res"),
            become_tombstone: self.cmd_metrics("become_tombstone"),
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
                batch_delete: Arc::new(self.retry_metrics("batch::delete")),
                batch_set: self.retry_metrics("batch::set"),
                blob_open: self.retry_metrics("blob::open"),
                compaction_noop_delete: Arc::new(self.retry_metrics("compaction_noop::delete")),
                consensus_open: self.retry_metrics("consensus::open"),
                fetch_batch_get: self.retry_metrics("fetch_batch::get"),
                fetch_state_scan: self.retry_metrics("fetch_state::scan"),
                gc_truncate: self.retry_metrics("gc::truncate"),
                maybe_init_cas: self.retry_metrics("maybe_init::cas"),
                rollup_delete: self.retry_metrics("rollup::delete"),
                rollup_get: self.retry_metrics("rollup::get"),
                rollup_set: self.retry_metrics("rollup::set"),
                storage_usage_shard_size: self.retry_metrics("storage_usage::shard_size"),
            },
            compare_and_append_idempotent: self.retry_metrics("compare_and_append_idempotent"),
            fetch_latest_state: self.retry_metrics("fetch_latest_state"),
            fetch_live_states: self.retry_metrics("fetch_live_states"),
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
            state_diff: self.codec_metrics("state_diff"),
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
            set: self.external_op_metrics("blob_set", true),
            get: self.external_op_metrics("blob_get", true),
            list_keys: self.external_op_metrics("blob_list_keys", false),
            delete: self.external_op_metrics("blob_delete", false),
            restore: self.external_op_metrics("restore", false),
            delete_noop: self.external_blob_delete_noop_count.clone(),
            blob_sizes: self.external_blob_sizes.clone(),
            rtt_latency: self.external_rtt_latency.with_label_values(&["blob"]),
        }
    }

    fn consensus_metrics(&self) -> ConsensusMetrics {
        ConsensusMetrics {
            list_keys: self.external_op_metrics("consensus_list_keys", false),
            head: self.external_op_metrics("consensus_head", false),
            compare_and_set: self.external_op_metrics("consensus_cas", true),
            scan: self.external_op_metrics("consensus_scan", false),
            truncate: self.external_op_metrics("consensus_truncate", false),
            truncated_count: self.external_consensus_truncated_count.clone(),
            rtt_latency: self.external_rtt_latency.with_label_values(&["consensus"]),
        }
    }

    fn external_op_metrics(&self, op: &str, latency_histogram: bool) -> ExternalOpMetrics {
        ExternalOpMetrics {
            started: self.external_op_started.with_label_values(&[op]),
            succeeded: self.external_op_succeeded.with_label_values(&[op]),
            failed: self.external_op_failed.with_label_values(&[op]),
            bytes: self.external_op_bytes.with_label_values(&[op]),
            seconds: self.external_op_seconds.with_label_values(&[op]),
            seconds_histogram: if latency_histogram {
                Some(self.external_op_latency.with_label_values(&[op]))
            } else {
                None
            },
            alerts_metrics: Arc::clone(&self.alerts_metrics),
        }
    }

    fn batch_part_read_metrics(&self) -> BatchPartReadMetrics {
        BatchPartReadMetrics {
            listen: self.read_metrics("listen"),
            snapshot: self.read_metrics("snapshot"),
            batch_fetcher: self.read_metrics("batch_fetcher"),
            compaction: self.read_metrics("compaction"),
        }
    }

    fn read_metrics(&self, op: &str) -> ReadMetrics {
        ReadMetrics {
            part_bytes: self.read_part_bytes.with_label_values(&[op]),
            part_goodbytes: self.read_part_goodbytes.with_label_values(&[op]),
            part_count: self.read_part_count.with_label_values(&[op]),
            seconds: self.read_part_seconds.with_label_values(&[op]),
            ts_rewrite: self.read_ts_rewrite.with_label_values(&[op]),
        }
    }

    fn locks_metrics(&self) -> LocksMetrics {
        LocksMetrics {
            applier_read_cacheable: self.lock_metrics("applier_read_cacheable"),
            applier_read_noncacheable: self.lock_metrics("applier_read_noncacheable"),
            applier_write: self.lock_metrics("applier_write"),
            watch: self.lock_metrics("watch"),
        }
    }

    fn lock_metrics(&self, op: &str) -> LockMetrics {
        LockMetrics {
            acquire_count: self.lock_acquire_count.with_label_values(&[op]),
            blocking_acquire_count: self.lock_blocking_acquire_count.with_label_values(&[op]),
            blocking_seconds: self.lock_blocking_seconds.with_label_values(&[op]),
        }
    }
}

#[derive(Debug)]
pub struct CmdCasMismatchMetric(#[allow(dead_code)] pub(crate) IntCounter);

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
    pub async fn run_cmd<R, E, F, CmdFn>(
        &self,
        shard_metrics: &ShardMetrics,
        cmd_fn: CmdFn,
    ) -> Result<R, E>
    where
        F: std::future::Future<Output = Result<R, E>>,
        CmdFn: FnOnce() -> F,
    {
        self.started.inc();
        let start = Instant::now();
        let res = cmd_fn().await;
        self.seconds.inc_by(start.elapsed().as_secs_f64());
        match res.as_ref() {
            Ok(_) => {
                self.succeeded.inc();
                shard_metrics.cmd_succeeded.inc();
            }
            Err(_) => self.failed.inc(),
        };
        res
    }
}

#[derive(Debug)]
pub struct CmdsMetrics {
    pub(crate) init_state: CmdMetrics,
    pub(crate) add_rollup: CmdMetrics,
    pub(crate) remove_rollups: CmdMetrics,
    pub(crate) register: CmdMetrics,
    pub(crate) compare_and_append: CmdMetrics,
    pub(crate) compare_and_append_noop: IntCounter,
    pub(crate) compare_and_downgrade_since: CmdMetrics,
    pub(crate) downgrade_since: CmdMetrics,
    pub(crate) heartbeat_reader: CmdMetrics,
    pub(crate) expire_reader: CmdMetrics,
    pub(crate) expire_writer: CmdMetrics,
    pub(crate) merge_res: CmdMetrics,
    pub(crate) become_tombstone: CmdMetrics,
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
    pub(crate) batch_delete: Arc<RetryMetrics>,
    pub(crate) batch_set: RetryMetrics,
    pub(crate) blob_open: RetryMetrics,
    pub(crate) compaction_noop_delete: Arc<RetryMetrics>,
    pub(crate) consensus_open: RetryMetrics,
    pub(crate) fetch_batch_get: RetryMetrics,
    pub(crate) fetch_state_scan: RetryMetrics,
    pub(crate) gc_truncate: RetryMetrics,
    pub(crate) maybe_init_cas: RetryMetrics,
    pub(crate) rollup_delete: RetryMetrics,
    pub(crate) rollup_get: RetryMetrics,
    pub(crate) rollup_set: RetryMetrics,
    pub(crate) storage_usage_shard_size: RetryMetrics,
}

#[derive(Debug)]
pub struct RetriesMetrics {
    pub(crate) determinate: RetryDeterminate,
    pub(crate) external: RetryExternal,

    pub(crate) compare_and_append_idempotent: RetryMetrics,
    pub(crate) fetch_latest_state: RetryMetrics,
    pub(crate) fetch_live_states: RetryMetrics,
    pub(crate) idempotent_cmd: RetryMetrics,
    pub(crate) next_listen_batch: RetryMetrics,
    pub(crate) snapshot: RetryMetrics,
}

#[derive(Debug)]
pub struct BatchPartReadMetrics {
    pub(crate) listen: ReadMetrics,
    pub(crate) snapshot: ReadMetrics,
    pub(crate) batch_fetcher: ReadMetrics,
    pub(crate) compaction: ReadMetrics,
}

#[derive(Debug, Clone)]
pub struct ReadMetrics {
    pub(crate) part_bytes: IntCounter,
    pub(crate) part_goodbytes: IntCounter,
    pub(crate) part_count: IntCounter,
    pub(crate) seconds: Counter,
    pub(crate) ts_rewrite: IntCounter,
}

// This one is Clone in contrast to the others because it has to get moved into
// a task.
#[derive(Debug, Clone)]
pub struct BatchWriteMetrics {
    pub(crate) bytes: IntCounter,
    pub(crate) goodbytes: IntCounter,
    pub(crate) seconds: Counter,
    pub(crate) write_stalls: IntCounter,

    pub(crate) step_consolidation: Counter,
    pub(crate) step_columnar_encoding: Counter,
    pub(crate) step_stats: Counter,
    pub(crate) step_part_writing: Counter,
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
            seconds: registry.register(metric!(
                name: format!("mz_persist_{}_write_batch_part_seconds", name),
                help: format!("time spent writing {} batches", name),
            )),
            write_stalls: registry.register(metric!(
                name: format!("mz_persist_{}_write_stall_count", name),
                help: format!(
                    "count of {} writes stalling to await max outstanding reqs",
                    name
                ),
            )),
            step_consolidation: registry.register(metric!(
                name: format!("mz_persist_{}_step_consolidation", name),
                help: format!("time spent consolidating {} updates", name),
            )),
            step_columnar_encoding: registry.register(metric!(
                name: format!("mz_persist_{}_step_columnar_encoding", name),
                help: format!("time spent columnar encoding {} updates", name),
            )),
            step_stats: registry.register(metric!(
                name: format!("mz_persist_{}_step_stats", name),
                help: format!("time spent computing {} update stats", name),
            )),
            step_part_writing: registry.register(metric!(
                name: format!("mz_persist_{}_step_part_writing", name),
                help: format!("blocking time spent writing parts for {} updates", name),
            )),
        }
    }
}

#[derive(Debug)]
pub struct CompactionMetrics {
    pub(crate) requested: IntCounter,
    pub(crate) dropped: IntCounter,
    pub(crate) skipped: IntCounter,
    pub(crate) started: IntCounter,
    pub(crate) applied: IntCounter,
    pub(crate) timed_out: IntCounter,
    pub(crate) failed: IntCounter,
    pub(crate) noop: IntCounter,
    pub(crate) seconds: Counter,
    pub(crate) concurrency_waits: IntCounter,
    pub(crate) queued_seconds: Counter,
    pub(crate) memory_violations: IntCounter,
    pub(crate) runs_compacted: IntCounter,
    pub(crate) chunks_compacted: IntCounter,
    pub(crate) not_all_prefetched: IntCounter,
    pub(crate) parts_prefetched: IntCounter,
    pub(crate) parts_waited: IntCounter,
    pub(crate) fast_path_eligible: IntCounter,

    pub(crate) applied_exact_match: IntCounter,
    pub(crate) applied_subset_match: IntCounter,
    pub(crate) not_applied_too_many_updates: IntCounter,

    pub(crate) batch: BatchWriteMetrics,
    pub(crate) steps: CompactionStepTimings,

    pub(crate) _steps_vec: CounterVec,
}

impl CompactionMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        let step_timings: CounterVec = registry.register(metric!(
                name: "mz_persist_compaction_step_seconds",
                help: "time spent on individual steps of compaction",
                var_labels: ["step"],
        ));

        CompactionMetrics {
            requested: registry.register(metric!(
                name: "mz_persist_compaction_requested",
                help: "count of total compaction requests",
            )),
            dropped: registry.register(metric!(
                name: "mz_persist_compaction_dropped",
                help: "count of total compaction requests dropped due to a full queue",
            )),
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
            timed_out: registry.register(metric!(
                name: "mz_persist_compaction_timed_out",
                help: "count of compactions that timed out",
            )),
            noop: registry.register(metric!(
                name: "mz_persist_compaction_noop",
                help: "count of compactions discarded (obsolete)",
            )),
            seconds: registry.register(metric!(
                name: "mz_persist_compaction_seconds",
                help: "time spent in compaction",
            )),
            concurrency_waits: registry.register(metric!(
                name: "mz_persist_compaction_concurrency_waits",
                help: "count of compaction requests that ever blocked due to concurrency limit",
            )),
            queued_seconds: registry.register(metric!(
                name: "mz_persist_compaction_queued_seconds",
                help: "time that compaction requests spent queued",
            )),
            memory_violations: registry.register(metric!(
                name: "mz_persist_compaction_memory_violations",
                help: "count of compaction memory requirement violations",
            )),
            runs_compacted: registry.register(metric!(
                name: "mz_persist_compaction_runs_compacted",
                help: "count of runs compacted",
            )),
            chunks_compacted: registry.register(metric!(
                name: "mz_persist_compaction_chunks_compacted",
                help: "count of run chunks compacted",
            )),
            not_all_prefetched: registry.register(metric!(
                name: "mz_persist_compaction_not_all_prefetched",
                help: "count of compactions where not all inputs were prefetched",
            )),
            parts_prefetched: registry.register(metric!(
                name: "mz_persist_compaction_parts_prefetched",
                help: "count of compaction parts completely prefetched by the time they're needed",
            )),
            parts_waited: registry.register(metric!(
                name: "mz_persist_compaction_parts_waited",
                help: "count of compaction parts that had to be waited on",
            )),
            fast_path_eligible: registry.register(metric!(
                name: "mz_persist_compaction_fast_path_eligible",
                help: "count of compaction requests that could have used the fast-path optimization",
            )),
            applied_exact_match: registry.register(metric!(
                name: "mz_persist_compaction_applied_exact_match",
                help: "count of merge results that exactly replaced a SpineBatch",
            )),
            applied_subset_match: registry.register(metric!(
                name: "mz_persist_compaction_applied_subset_match",
                help: "count of merge results that replaced a subset of a SpineBatch",
            )),
            not_applied_too_many_updates: registry.register(metric!(
                name: "mz_persist_compaction_not_applied_too_many_updates",
                help: "count of merge results that did not apply due to too many updates",
            )),
            batch: BatchWriteMetrics::new(registry, "compaction"),
            steps: CompactionStepTimings::new(step_timings.clone()),
            _steps_vec: step_timings,
        }
    }
}

#[derive(Debug)]
pub struct CompactionStepTimings {
    pub(crate) part_fetch_seconds: Counter,
    pub(crate) heap_population_seconds: Counter,
}

impl CompactionStepTimings {
    fn new(step_timings: CounterVec) -> CompactionStepTimings {
        CompactionStepTimings {
            part_fetch_seconds: step_timings.with_label_values(&["part_fetch"]),
            heap_population_seconds: step_timings.with_label_values(&["heap_population"]),
        }
    }
}

#[derive(Debug)]
pub struct GcMetrics {
    pub(crate) noop: IntCounter,
    pub(crate) started: IntCounter,
    pub(crate) finished: IntCounter,
    pub(crate) merged: IntCounter,
    pub(crate) seconds: Counter,
    pub(crate) steps: GcStepTimings,
}

#[derive(Debug)]
pub struct GcStepTimings {
    pub(crate) find_removable_rollups: Counter,
    pub(crate) fetch_seconds: Counter,
    pub(crate) find_deletable_blobs_seconds: Counter,
    pub(crate) delete_rollup_seconds: Counter,
    pub(crate) delete_batch_part_seconds: Counter,
    pub(crate) truncate_diff_seconds: Counter,
    pub(crate) remove_rollups_from_state: Counter,
    pub(crate) post_gc_calculations_seconds: Counter,
}

impl GcStepTimings {
    fn new(step_timings: CounterVec) -> Self {
        Self {
            find_removable_rollups: step_timings.with_label_values(&["find_removable_rollups"]),
            fetch_seconds: step_timings.with_label_values(&["fetch"]),
            find_deletable_blobs_seconds: step_timings.with_label_values(&["find_deletable_blobs"]),
            delete_rollup_seconds: step_timings.with_label_values(&["delete_rollup"]),
            delete_batch_part_seconds: step_timings.with_label_values(&["delete_batch_part"]),
            truncate_diff_seconds: step_timings.with_label_values(&["truncate_diff"]),
            remove_rollups_from_state: step_timings
                .with_label_values(&["remove_rollups_from_state"]),
            post_gc_calculations_seconds: step_timings.with_label_values(&["post_gc_calculations"]),
        }
    }
}

impl GcMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        let step_timings: CounterVec = registry.register(metric!(
                name: "mz_persist_gc_step_seconds",
                help: "time spent on individual steps of gc",
                var_labels: ["step"],
        ));
        GcMetrics {
            noop: registry.register(metric!(
                name: "mz_persist_gc_noop",
                help: "count of garbage collections skipped because they were already done",
            )),
            started: registry.register(metric!(
                name: "mz_persist_gc_started",
                help: "count of garbage collections started",
            )),
            finished: registry.register(metric!(
                name: "mz_persist_gc_finished",
                help: "count of garbage collections finished",
            )),
            merged: registry.register(metric!(
                name: "mz_persist_gc_merged_reqs",
                help: "count of garbage collection requests merged",
            )),
            seconds: registry.register(metric!(
                name: "mz_persist_gc_seconds",
                help: "time spent in garbage collections",
            )),
            steps: GcStepTimings::new(step_timings),
        }
    }
}

#[derive(Debug)]
pub struct LeaseMetrics {
    pub(crate) timeout_read: IntCounter,
    pub(crate) dropped_part: IntCounter,
}

impl LeaseMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        LeaseMetrics {
            timeout_read: registry.register(metric!(
                name: "mz_persist_lease_timeout_read",
                help: "count of readers whose lease timed out",
            )),
            dropped_part: registry.register(metric!(
                name: "mz_persist_lease_dropped_part",
                help: "count of LeasedBatchParts that were dropped without being politely returned",
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
    pub(crate) retries: IntCounter,
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
    pub(crate) state_diff: CodecMetrics,
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
pub struct StateMetrics {
    pub(crate) apply_spine_fast_path: IntCounter,
    pub(crate) apply_spine_slow_path: IntCounter,
    pub(crate) apply_spine_slow_path_lenient: IntCounter,
    pub(crate) apply_spine_slow_path_lenient_adjustment: IntCounter,
    pub(crate) apply_spine_slow_path_with_reconstruction: IntCounter,
    pub(crate) apply_spine_flattened: IntCounter,
    pub(crate) update_state_noop_path: IntCounter,
    pub(crate) update_state_empty_path: IntCounter,
    pub(crate) update_state_fast_path: IntCounter,
    pub(crate) update_state_slow_path: IntCounter,
    pub(crate) rollup_at_seqno_migration: IntCounter,
    pub(crate) fetch_recent_live_diffs_fast_path: IntCounter,
    pub(crate) fetch_recent_live_diffs_slow_path: IntCounter,
    pub(crate) writer_added: IntCounter,
    pub(crate) writer_removed: IntCounter,
    pub(crate) force_apply_hostname: IntCounter,
    pub(crate) rollup_write_success: IntCounter,
    pub(crate) rollup_write_noop_latest: IntCounter,
    pub(crate) rollup_write_noop_truncated: IntCounter,
}

impl StateMetrics {
    pub(crate) fn new(registry: &MetricsRegistry) -> Self {
        let rollup_write_noop: IntCounterVec = registry.register(metric!(
                name: "mz_persist_state_rollup_write_noop",
                help: "count of no-op rollup writes",
                var_labels: ["reason"],
        ));

        StateMetrics {
            apply_spine_fast_path: registry.register(metric!(
                name: "mz_persist_state_apply_spine_fast_path",
                help: "count of spine diff applications that hit the fast path",
            )),
            apply_spine_slow_path: registry.register(metric!(
                name: "mz_persist_state_apply_spine_slow_path",
                help: "count of spine diff applications that hit the slow path",
            )),
            apply_spine_slow_path_lenient: registry.register(metric!(
                name: "mz_persist_state_apply_spine_slow_path_lenient",
                help: "count of spine diff applications that hit the lenient compaction apply path",
            )),
            apply_spine_slow_path_lenient_adjustment: registry.register(metric!(
                name: "mz_persist_state_apply_spine_slow_path_lenient_adjustment",
                help: "count of adjustments made by the lenient compaction apply path",
            )),
            apply_spine_slow_path_with_reconstruction: registry.register(metric!(
                name: "mz_persist_state_apply_spine_slow_path_with_reconstruction",
                help: "count of spine diff applications that hit the slow path with extra spine reconstruction step",
            )),
            apply_spine_flattened: registry.register(metric!(
                name: "mz_persist_state_apply_spine_flattened",
                help: "count of spine diff applications that flatten the trace",
            )),
            update_state_noop_path: registry.register(metric!(
                name: "mz_persist_state_update_state_noop_path",
                help: "count of state update applications that no-oped due to shared state",
            )),
            update_state_empty_path: registry.register(metric!(
                name: "mz_persist_state_update_state_empty_path",
                help: "count of state update applications that found no new updates",
            )),
            update_state_fast_path: registry.register(metric!(
                name: "mz_persist_state_update_state_fast_path",
                help: "count of state update applications that hit the fast path",
            )),
            update_state_slow_path: registry.register(metric!(
                name: "mz_persist_state_update_state_slow_path",
                help: "count of state update applications that hit the slow path",
            )),
            rollup_at_seqno_migration: registry.register(metric!(
                name: "mz_persist_state_rollup_at_seqno_migration",
                help: "count of fetch_rollup_at_seqno calls that only worked because of the migration",
            )),
            fetch_recent_live_diffs_fast_path: registry.register(metric!(
                name: "mz_persist_state_fetch_recent_live_diffs_fast_path",
                help: "count of fetch_recent_live_diffs that hit the fast path",
            )),
            fetch_recent_live_diffs_slow_path: registry.register(metric!(
                name: "mz_persist_state_fetch_recent_live_diffs_slow_path",
                help: "count of fetch_recent_live_diffs that hit the slow path",
            )),
            writer_added: registry.register(metric!(
                name: "mz_persist_state_writer_added",
                help: "count of writers added to the state",
            )),
            writer_removed: registry.register(metric!(
                name: "mz_persist_state_writer_removed",
                help: "count of writers removed from the state",
            )),
            force_apply_hostname: registry.register(metric!(
                name: "mz_persist_state_force_applied_hostname",
                help: "count of when hostname diffs needed to be force applied",
            )),
            rollup_write_success: registry.register(metric!(
                name: "mz_persist_state_rollup_write_success",
                help: "count of rollups written successful (may not be linked in to state)",
            )),
            rollup_write_noop_latest: rollup_write_noop.with_label_values(&["latest"]),
            rollup_write_noop_truncated: rollup_write_noop.with_label_values(&["truncated"]),
        }
    }
}

#[derive(Debug)]
pub struct ShardsMetrics {
    // Unlike all the other metrics in here, ShardsMetrics intentionally uses
    // the DeleteOnDrop wrappers. A process might stop using a shard (drop all
    // handles to it) but e.g. the set of commands never changes.
    _count: ComputedIntGauge,
    since: mz_ore::metrics::IntGaugeVec,
    upper: mz_ore::metrics::IntGaugeVec,
    encoded_rollup_size: mz_ore::metrics::UIntGaugeVec,
    encoded_diff_size: mz_ore::metrics::IntCounterVec,
    hollow_batch_count: mz_ore::metrics::UIntGaugeVec,
    spine_batch_count: mz_ore::metrics::UIntGaugeVec,
    batch_part_count: mz_ore::metrics::UIntGaugeVec,
    update_count: mz_ore::metrics::UIntGaugeVec,
    rollup_count: mz_ore::metrics::UIntGaugeVec,
    largest_batch_size: mz_ore::metrics::UIntGaugeVec,
    seqnos_held: mz_ore::metrics::UIntGaugeVec,
    seqnos_since_last_rollup: mz_ore::metrics::UIntGaugeVec,
    gc_seqno_held_parts: mz_ore::metrics::UIntGaugeVec,
    gc_live_diffs: mz_ore::metrics::UIntGaugeVec,
    gc_finished: mz_ore::metrics::IntCounterVec,
    compaction_applied: mz_ore::metrics::IntCounterVec,
    cmd_succeeded: mz_ore::metrics::IntCounterVec,
    usage_current_state_batches_bytes: mz_ore::metrics::UIntGaugeVec,
    usage_current_state_rollups_bytes: mz_ore::metrics::UIntGaugeVec,
    usage_referenced_not_current_state_bytes: mz_ore::metrics::UIntGaugeVec,
    usage_not_leaked_not_referenced_bytes: mz_ore::metrics::UIntGaugeVec,
    usage_leaked_bytes: mz_ore::metrics::UIntGaugeVec,
    pubsub_push_diff_applied: mz_ore::metrics::IntCounterVec,
    pubsub_push_diff_not_applied_stale: mz_ore::metrics::IntCounterVec,
    pubsub_push_diff_not_applied_out_of_order: mz_ore::metrics::IntCounterVec,
    blob_gets: mz_ore::metrics::IntCounterVec,
    blob_sets: mz_ore::metrics::IntCounterVec,
    live_writers: mz_ore::metrics::UIntGaugeVec,
    unconsolidated_snapshot: mz_ore::metrics::IntCounterVec,
    backpressure_emitted_bytes: IntCounterVec,
    backpressure_last_backpressured_bytes: UIntGaugeVec,
    backpressure_retired_bytes: IntCounterVec,
    rewrite_part_count: mz_ore::metrics::UIntGaugeVec,
    // We hand out `Arc<ShardMetrics>` to read and write handles, but store it
    // here as `Weak`. This allows us to discover if it's no longer in use and
    // so we can remove it from the map.
    shards: Arc<Mutex<BTreeMap<ShardId, Weak<ShardMetrics>>>>,
}

impl ShardsMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        let shards = Arc::new(Mutex::new(BTreeMap::new()));
        let shards_count = Arc::clone(&shards);
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
            since: registry.register(metric!(
                name: "mz_persist_shard_since",
                help: "since by shard",
                var_labels: ["shard", "name"],
            )),
            upper: registry.register(metric!(
                name: "mz_persist_shard_upper",
                help: "upper by shard",
                var_labels: ["shard", "name"],
            )),
            encoded_rollup_size: registry.register(metric!(
                name: "mz_persist_shard_rollup_size_bytes",
                help: "total encoded rollup size by shard",
                var_labels: ["shard", "name"],
            )),
            encoded_diff_size: registry.register(metric!(
                name: "mz_persist_shard_diff_size_bytes",
                help: "total encoded diff size by shard",
                var_labels: ["shard", "name"],
            )),
            hollow_batch_count: registry.register(metric!(
                name: "mz_persist_shard_hollow_batch_count",
                help: "count of hollow batches by shard",
                var_labels: ["shard", "name"],
            )),
            spine_batch_count: registry.register(metric!(
                name: "mz_persist_shard_spine_batch_count",
                help: "count of spine batches by shard",
                var_labels: ["shard", "name"],
            )),
            batch_part_count: registry.register(metric!(
                name: "mz_persist_shard_batch_part_count",
                help: "count of batch parts by shard",
                var_labels: ["shard", "name"],
            )),
            update_count: registry.register(metric!(
                name: "mz_persist_shard_update_count",
                help: "count of updates by shard",
                var_labels: ["shard", "name"],
            )),
            rollup_count: registry.register(metric!(
                name: "mz_persist_shard_rollup_count",
                help: "count of rollups by shard",
                var_labels: ["shard", "name"],
            )),
            largest_batch_size: registry.register(metric!(
                name: "mz_persist_shard_largest_batch_size",
                help: "largest encoded batch size by shard",
                var_labels: ["shard", "name"],
            )),
            seqnos_held: registry.register(metric!(
                name: "mz_persist_shard_seqnos_held",
                help: "maximum count of gc-ineligible states by shard",
                var_labels: ["shard", "name"],
            )),
            seqnos_since_last_rollup: registry.register(metric!(
                name: "mz_persist_shard_seqnos_since_last_rollup",
                help: "count of seqnos since last rollup",
                var_labels: ["shard", "name"],
            )),
            gc_seqno_held_parts: registry.register(metric!(
                name: "mz_persist_shard_gc_seqno_held_parts",
                help: "count of parts referenced by some live state but not the current state (ie. parts kept only to satisfy seqno holds) at GC time",
                var_labels: ["shard", "name"],
            )),
            gc_live_diffs: registry.register(metric!(
                name: "mz_persist_shard_gc_live_diffs",
                help: "the number of diffs (or, alternatively, the number of seqnos) present in consensus state at GC time",
                var_labels: ["shard", "name"],
            )),
            gc_finished: registry.register(metric!(
                name: "mz_persist_shard_gc_finished",
                help: "count of garbage collections finished by shard",
                var_labels: ["shard", "name"],
            )),
            compaction_applied: registry.register(metric!(
                name: "mz_persist_shard_compaction_applied",
                help: "count of compactions applied to state by shard",
                var_labels: ["shard", "name"],
            )),
            cmd_succeeded: registry.register(metric!(
                name: "mz_persist_shard_cmd_succeeded",
                help: "count of commands succeeded by shard",
                var_labels: ["shard", "name"],
            )),
            usage_current_state_batches_bytes: registry.register(metric!(
                name: "mz_persist_shard_usage_current_state_batches_bytes",
                help: "data in batches/parts referenced by current version of state",
                var_labels: ["shard", "name"],
            )),
            usage_current_state_rollups_bytes: registry.register(metric!(
                name: "mz_persist_shard_usage_current_state_rollups_bytes",
                help: "data in rollups referenced by current version of state",
                var_labels: ["shard", "name"],
            )),
            usage_referenced_not_current_state_bytes: registry.register(metric!(
                name: "mz_persist_shard_usage_referenced_not_current_state_bytes",
                help: "data referenced only by a previous version of state",
                var_labels: ["shard", "name"],
            )),
            usage_not_leaked_not_referenced_bytes: registry.register(metric!(
                name: "mz_persist_shard_usage_not_leaked_not_referenced_bytes",
                help: "data written by an active writer but not referenced by any version of state",
                var_labels: ["shard", "name"],
            )),
            usage_leaked_bytes: registry.register(metric!(
                name: "mz_persist_shard_usage_leaked_bytes",
                help: "data reclaimable by a leaked blob detector",
                var_labels: ["shard", "name"],
            )),
            pubsub_push_diff_applied: registry.register(metric!(
                name: "mz_persist_shard_pubsub_diff_applied",
                help: "number of diffs received via pubsub that applied",
                var_labels: ["shard", "name"],
            )),
            pubsub_push_diff_not_applied_stale: registry.register(metric!(
                name: "mz_persist_shard_pubsub_diff_not_applied_stale",
                help: "number of diffs received via pubsub that did not apply due to staleness",
                var_labels: ["shard", "name"],
            )),
            pubsub_push_diff_not_applied_out_of_order: registry.register(metric!(
                name: "mz_persist_shard_pubsub_diff_not_applied_out_of_order",
                help: "number of diffs received via pubsub that did not apply due to out-of-order delivery",
                var_labels: ["shard", "name"],
            )),
            blob_gets: registry.register(metric!(
                name: "mz_persist_shard_blob_gets",
                help: "number of Blob::get calls for this shard",
                var_labels: ["shard", "name"],
            )),
            blob_sets: registry.register(metric!(
                name: "mz_persist_shard_blob_sets",
                help: "number of Blob::set calls for this shard",
                var_labels: ["shard", "name"],
            )),
            live_writers: registry.register(metric!(
                name: "mz_persist_shard_live_writers",
                help: "number of writers that have recently appended updates to this shard",
                var_labels: ["shard", "name"],
            )),
            unconsolidated_snapshot: registry.register(metric!(
                name: "mz_persist_shard_unconsolidated_snapshot",
                help: "in snapshot_and_read, the number of times consolidating the raw data wasn't enough to produce consolidated output",
                var_labels: ["shard", "name"],
            )),
            backpressure_emitted_bytes: registry.register(metric!(
                name: "mz_persist_backpressure_emitted_bytes",
                help: "A counter with the number of emitted bytes.",
                var_labels: ["shard", "name"],
            )),
            backpressure_last_backpressured_bytes: registry.register(metric!(
                name: "mz_persist_backpressure_last_backpressured_bytes",
                help: "The last count of bytes we are waiting to be retired in \
                    the operator. This cannot be directly compared to \
                    `retired_bytes`, but CAN indicate that backpressure is happening.",
                var_labels: ["shard", "name"],
            )),
            backpressure_retired_bytes: registry.register(metric!(
                name: "mz_persist_backpressure_retired_bytes",
                help:"A counter with the number of bytes retired by downstream processing.",
                var_labels: ["shard", "name"],
            )),
            rewrite_part_count: registry.register(metric!(
                name: "mz_persist_shard_rewrite_part_count",
                help: "count of batch parts with rewrites by shard",
                var_labels: ["shard", "name"],
            )),
            shards,
        }
    }

    pub fn shard(&self, shard_id: &ShardId, name: &str) -> Arc<ShardMetrics> {
        let mut shards = self.shards.lock().expect("mutex poisoned");
        if let Some(shard) = shards.get(shard_id) {
            if let Some(shard) = shard.upgrade() {
                return Arc::clone(&shard);
            } else {
                assert!(shards.remove(shard_id).is_some());
            }
        }
        let shard = Arc::new(ShardMetrics::new(shard_id, name, self));
        assert!(shards
            .insert(shard_id.clone(), Arc::downgrade(&shard))
            .is_none());
        shard
    }

    fn compute<F: FnMut(&ShardMetrics)>(
        shards: &Arc<Mutex<BTreeMap<ShardId, Weak<ShardMetrics>>>>,
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
    pub shard_id: ShardId,
    pub since: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    pub upper: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    pub largest_batch_size: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub latest_rollup_size: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub encoded_diff_size: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub hollow_batch_count: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub spine_batch_count: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub batch_part_count: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub update_count: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub rollup_count: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub seqnos_held: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub seqnos_since_last_rollup: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub gc_seqno_held_parts: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub gc_live_diffs: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub usage_current_state_batches_bytes: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub usage_current_state_rollups_bytes: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub usage_referenced_not_current_state_bytes:
        DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub usage_not_leaked_not_referenced_bytes: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub usage_leaked_bytes: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub gc_finished: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub compaction_applied: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub cmd_succeeded: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub pubsub_push_diff_applied: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub pubsub_push_diff_not_applied_stale: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub pubsub_push_diff_not_applied_out_of_order:
        DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub blob_gets: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub blob_sets: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub live_writers: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    pub unconsolidated_snapshot: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub backpressure_emitted_bytes: Arc<DeleteOnDropCounter<'static, AtomicU64, Vec<String>>>,
    pub backpressure_last_backpressured_bytes:
        Arc<DeleteOnDropGauge<'static, AtomicU64, Vec<String>>>,
    pub backpressure_retired_bytes: Arc<DeleteOnDropCounter<'static, AtomicU64, Vec<String>>>,
    pub rewrite_part_count: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
}

impl ShardMetrics {
    pub fn new(shard_id: &ShardId, name: &str, shards_metrics: &ShardsMetrics) -> Self {
        let shard = shard_id.to_string();
        ShardMetrics {
            shard_id: *shard_id,
            since: shards_metrics
                .since
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            upper: shards_metrics
                .upper
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            latest_rollup_size: shards_metrics
                .encoded_rollup_size
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            encoded_diff_size: shards_metrics
                .encoded_diff_size
                .get_delete_on_drop_counter(vec![shard.clone(), name.to_string()]),
            hollow_batch_count: shards_metrics
                .hollow_batch_count
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            spine_batch_count: shards_metrics
                .spine_batch_count
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            batch_part_count: shards_metrics
                .batch_part_count
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            update_count: shards_metrics
                .update_count
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            rollup_count: shards_metrics
                .rollup_count
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            largest_batch_size: shards_metrics
                .largest_batch_size
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            seqnos_held: shards_metrics
                .seqnos_held
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            seqnos_since_last_rollup: shards_metrics
                .seqnos_since_last_rollup
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            gc_seqno_held_parts: shards_metrics
                .gc_seqno_held_parts
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            gc_live_diffs: shards_metrics
                .gc_live_diffs
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            gc_finished: shards_metrics
                .gc_finished
                .get_delete_on_drop_counter(vec![shard.clone(), name.to_string()]),
            compaction_applied: shards_metrics
                .compaction_applied
                .get_delete_on_drop_counter(vec![shard.clone(), name.to_string()]),
            cmd_succeeded: shards_metrics
                .cmd_succeeded
                .get_delete_on_drop_counter(vec![shard.clone(), name.to_string()]),
            usage_current_state_batches_bytes: shards_metrics
                .usage_current_state_batches_bytes
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            usage_current_state_rollups_bytes: shards_metrics
                .usage_current_state_rollups_bytes
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            usage_referenced_not_current_state_bytes: shards_metrics
                .usage_referenced_not_current_state_bytes
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            usage_not_leaked_not_referenced_bytes: shards_metrics
                .usage_not_leaked_not_referenced_bytes
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            usage_leaked_bytes: shards_metrics
                .usage_leaked_bytes
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            pubsub_push_diff_applied: shards_metrics
                .pubsub_push_diff_applied
                .get_delete_on_drop_counter(vec![shard.clone(), name.to_string()]),
            pubsub_push_diff_not_applied_stale: shards_metrics
                .pubsub_push_diff_not_applied_stale
                .get_delete_on_drop_counter(vec![shard.clone(), name.to_string()]),
            pubsub_push_diff_not_applied_out_of_order: shards_metrics
                .pubsub_push_diff_not_applied_out_of_order
                .get_delete_on_drop_counter(vec![shard.clone(), name.to_string()]),
            blob_gets: shards_metrics
                .blob_gets
                .get_delete_on_drop_counter(vec![shard.clone(), name.to_string()]),
            blob_sets: shards_metrics
                .blob_sets
                .get_delete_on_drop_counter(vec![shard.clone(), name.to_string()]),
            live_writers: shards_metrics
                .live_writers
                .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            unconsolidated_snapshot: shards_metrics
                .unconsolidated_snapshot
                .get_delete_on_drop_counter(vec![shard.clone(), name.to_string()]),
            backpressure_emitted_bytes: Arc::new(
                shards_metrics
                    .backpressure_emitted_bytes
                    .get_delete_on_drop_counter(vec![shard.clone(), name.to_string()]),
            ),
            backpressure_last_backpressured_bytes: Arc::new(
                shards_metrics
                    .backpressure_last_backpressured_bytes
                    .get_delete_on_drop_gauge(vec![shard.clone(), name.to_string()]),
            ),
            backpressure_retired_bytes: Arc::new(
                shards_metrics
                    .backpressure_retired_bytes
                    .get_delete_on_drop_counter(vec![shard.clone(), name.to_string()]),
            ),
            rewrite_part_count: shards_metrics
                .rewrite_part_count
                .get_delete_on_drop_gauge(vec![shard, name.to_string()]),
        }
    }

    pub fn set_since<T: Codec64>(&self, since: &Antichain<T>) {
        self.since.set(encode_ts_metric(since))
    }

    pub fn set_upper<T: Codec64>(&self, upper: &Antichain<T>) {
        self.upper.set(encode_ts_metric(upper))
    }
}

/// Metrics recorded by audits of persist usage
#[derive(Debug)]
pub struct UsageAuditMetrics {
    /// Size of all batch parts stored in Blob
    pub blob_batch_part_bytes: UIntGauge,
    /// Count of batch parts stored in Blob
    pub blob_batch_part_count: UIntGauge,
    /// Size of all state rollups stored in Blob
    pub blob_rollup_bytes: UIntGauge,
    /// Count of state rollups stored in Blob
    pub blob_rollup_count: UIntGauge,
    /// Size of Blob
    pub blob_bytes: UIntGauge,
    /// Count of all blobs
    pub blob_count: UIntGauge,
    /// Time spent fetching blob metadata
    pub step_blob_metadata: Counter,
    /// Time spent fetching state versions
    pub step_state: Counter,
    /// Time spent doing math
    pub step_math: Counter,
}

impl UsageAuditMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        let step_timings: CounterVec = registry.register(metric!(
                name: "mz_persist_audit_step_seconds",
                help: "time spent on individual steps of audit",
                var_labels: ["step"],
        ));
        UsageAuditMetrics {
            blob_batch_part_bytes: registry.register(metric!(
                name: "mz_persist_audit_blob_batch_part_bytes",
                help: "total size of batch parts in blob",
            )),
            blob_batch_part_count: registry.register(metric!(
                name: "mz_persist_audit_blob_batch_part_count",
                help: "count of batch parts in blob",
            )),
            blob_rollup_bytes: registry.register(metric!(
                name: "mz_persist_audit_blob_rollup_bytes",
                help: "total size of state rollups stored in blob",
            )),
            blob_rollup_count: registry.register(metric!(
                name: "mz_persist_audit_blob_rollup_count",
                help: "count of all state rollups in blob",
            )),
            blob_bytes: registry.register(metric!(
                name: "mz_persist_audit_blob_bytes",
                help: "total size of blob",
            )),
            blob_count: registry.register(metric!(
                name: "mz_persist_audit_blob_count",
                help: "count of all blobs",
            )),
            step_blob_metadata: step_timings.with_label_values(&["blob_metadata"]),
            step_state: step_timings.with_label_values(&["state"]),
            step_math: step_timings.with_label_values(&["math"]),
        }
    }
}

/// Represents a change in a number of updates kept in a data structure
/// (e.g., a buffer length or capacity change).
#[derive(Debug)]
pub enum UpdateDelta {
    /// A negative delta in the number of updates.
    Negative(u64),
    /// A non-negative delta in the number of updates.
    NonNegative(u64),
}

impl UpdateDelta {
    /// Creates a new `UpdateDelta` from the difference between a new value
    /// for a number of updates and the corresponding old value.
    pub fn new(new: usize, old: usize) -> Self {
        if new < old {
            UpdateDelta::Negative(CastFrom::cast_from(old - new))
        } else {
            UpdateDelta::NonNegative(CastFrom::cast_from(new - old))
        }
    }
}

/// Metrics for the persist sink. (While this lies slightly outside the usual
/// abstraction boundary of the client, it's convenient to manage them together.
#[derive(Debug)]
pub struct SinkMetrics {
    /// Number of small batches that were forwarded to the central append operator
    pub forwarded_batches: Counter,
    /// Number of updates that were forwarded to the centralized append operator
    pub forwarded_updates: Counter,
    /// Cumulative record insertions made to the correction buffer across workers
    correction_insertions_total: IntCounter,
    /// Cumulative record deletions made to the correction buffer across workers
    correction_deletions_total: IntCounter,
    /// Cumulative capacity increases made to the correction buffer across workers
    correction_capacity_increases_total: IntCounter,
    /// Cumulative capacity decreases made to the correction buffer across workers
    correction_capacity_decreases_total: IntCounter,
    /// Peak length observed for the correction buffer across workers
    correction_peak_len_updates: UIntGauge,
    /// Peak capacity observed for the correction buffer across workers
    correction_peak_capacity_updates: UIntGauge,
    /// Maximum length observed for any one correction buffer per worker
    correction_max_per_sink_worker_len_updates: raw::UIntGaugeVec,
    /// Maximum capacity observed for any one correction buffer per worker
    correction_max_per_sink_worker_capacity_updates: raw::UIntGaugeVec,
}

impl SinkMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        SinkMetrics {
            forwarded_batches: registry.register(metric!(
                name: "mz_persist_sink_forwarded_batches",
                help: "number of batches forwarded to the central append operator",
            )),
            forwarded_updates: registry.register(metric!(
                name: "mz_persist_sink_forwarded_updates",
                help: "number of updates forwarded to the central append operator",
            )),
            correction_insertions_total: registry.register(metric!(
                name: "mz_persist_sink_correction_insertions_total",
                help: "The cumulative insertions observed on the correction buffer across workers and persist sinks.",
            )),
            correction_deletions_total: registry.register(metric!(
                name: "mz_persist_sink_correction_deletions_total",
                help: "The cumulative deletions observed on the correction buffer across workers and persist sinks.",
            )),
            correction_capacity_increases_total: registry.register(metric!(
                name: "mz_persist_sink_correction_capacity_increases_total",
                help: "The cumulative capacity increases observed on the correction buffer across workers and persist sinks.",
            )),
            correction_capacity_decreases_total: registry.register(metric!(
                name: "mz_persist_sink_correction_capacity_decreases_total",
                help: "The cumulative capacity decreases observed on the correction buffer across workers and persist sinks.",
            )),
            correction_peak_len_updates: registry.register(metric!(
                name: "mz_persist_sink_correction_peak_len_updates",
                help: "The peak length observed for the correction buffer across workers and persist sinks.",
            )),
            correction_peak_capacity_updates: registry.register(metric!(
                name: "mz_persist_sink_correction_peak_capacity_updates",
                help: "The peak capacity observed for the correction buffer across workers and persist sinks.",
            )),
            correction_max_per_sink_worker_len_updates: registry.register(metric!(
                name: "mz_persist_sink_correction_max_per_sink_worker_len_updates",
                help: "The maximum length observed for the correction buffer of any single persist sink per worker.",
                var_labels: ["worker_id"],
            )),
            correction_max_per_sink_worker_capacity_updates: registry.register(metric!(
                name: "mz_persist_sink_correction_max_per_sink_worker_capacity_updates",
                help: "The maximum capacity observed for the correction buffer of any single persist sink per worker.",
                var_labels: ["worker_id"],
            )),
        }
    }

    /// Obtains a `SinkWorkerMetrics` instance, which allows for metric reporting
    /// from a specific `persist_sink` instance for a given worker. The reports will
    /// update metrics shared across workers, but provide per-worker contributions
    /// to them.
    pub fn for_worker(&self, worker_id: usize) -> SinkWorkerMetrics {
        let worker = worker_id.to_string();
        let correction_max_per_sink_worker_len_updates = self
            .correction_max_per_sink_worker_len_updates
            .with_label_values(&[&worker]);
        let correction_max_per_sink_worker_capacity_updates = self
            .correction_max_per_sink_worker_capacity_updates
            .with_label_values(&[&worker]);
        SinkWorkerMetrics {
            correction_max_per_sink_worker_len_updates,
            correction_max_per_sink_worker_capacity_updates,
        }
    }

    /// Reports updates to the length and capacity of the correction buffer in the
    /// `write_batches` operator of a `persist_sink`.
    ///
    /// This method updates monotonic metrics based on the deltas and thus can be
    /// called across workers and instances of `persist_sink`.
    pub fn report_correction_update_deltas(
        &self,
        correction_len_delta: UpdateDelta,
        correction_cap_delta: UpdateDelta,
    ) {
        // Report insertions or deletions.
        match correction_len_delta {
            UpdateDelta::NonNegative(delta) => {
                if delta > 0 {
                    self.correction_insertions_total.inc_by(delta)
                }
            }
            UpdateDelta::Negative(delta) => self.correction_deletions_total.inc_by(delta),
        }
        // Report capacity increases or decreases.
        match correction_cap_delta {
            UpdateDelta::NonNegative(delta) => {
                if delta > 0 {
                    self.correction_capacity_increases_total.inc_by(delta)
                }
            }
            UpdateDelta::Negative(delta) => self.correction_capacity_decreases_total.inc_by(delta),
        }
    }

    /// Updates our estimate of the aggregate peak for the correction buffer
    /// length and capacity across workers and persist sink.
    ///
    /// This method assumes temporary quiescence on the correction buffer metrics,
    /// i.e., there are no updates to these metrics taking place when the method
    /// is called. This occurs, e.g., after a `step_or_park` call.
    pub fn update_sink_correction_peak_metrics(&self) {
        // Correctness argument:
        // 1. We always update insertions/increases prior to updating deletions/decreases
        // in each worker. So upon quiescence, the net effect of these must result in a
        // non-negative quantity.
        // 2. Every worker will execute the instructions below and compute the same peak.
        // This is because during quiescence, no changes happen to the previous peak nor
        // other metrics, and the ones we look at are all shared across workers. So at least
        // one worker will update the peak with its new value, if there is an increase, at
        // the quiescence granularity.
        // Note that we may miss the overall peak if between quiescence moments there are
        // lots of insertions followed by lots of deletions to the correction buffer. We
        // find this trade-off more attractive than adding explicity multi-metric synchronization
        // among workers.
        let aggregate_correction_len = self
            .correction_insertions_total
            .get()
            .checked_sub(self.correction_deletions_total.get());
        let Some(aggregate_correction_len) = aggregate_correction_len else {
            error!(
                aggregate_correction_len,
                "Negative aggregate length for persist sink correction"
            );
            return;
        };
        if aggregate_correction_len > self.correction_peak_len_updates.get() {
            self.correction_peak_len_updates
                .set(aggregate_correction_len);
        }
        let aggregate_correction_cap = self
            .correction_capacity_increases_total
            .get()
            .checked_sub(self.correction_capacity_decreases_total.get());
        let Some(aggregate_correction_cap) = aggregate_correction_cap else {
            error!(
                aggregate_correction_cap,
                "Negative aggregate capacity for persist sink correction"
            );
            return;
        };
        if aggregate_correction_cap > self.correction_peak_capacity_updates.get() {
            self.correction_peak_capacity_updates
                .set(aggregate_correction_cap);
        }
    }
}

/// Metrics for the persist sink that are labeled per-worker.
#[derive(Clone, Debug)]
pub struct SinkWorkerMetrics {
    correction_max_per_sink_worker_len_updates: UIntGauge,
    correction_max_per_sink_worker_capacity_updates: UIntGauge,
}

impl SinkWorkerMetrics {
    /// Reports the length and capacity of the correction buffer in the `write_batches`
    /// operator of `persist_sink`.
    ///
    /// This method is used to update metrics that are kept per worker.
    pub fn report_correction_update_totals(&self, correction_len: usize, correction_cap: usize) {
        // Maintain per-worker peaks.
        let correction_len = CastFrom::cast_from(correction_len);
        if correction_len > self.correction_max_per_sink_worker_len_updates.get() {
            self.correction_max_per_sink_worker_len_updates
                .set(correction_len);
        }
        let correction_cap = CastFrom::cast_from(correction_cap);
        if correction_cap > self.correction_max_per_sink_worker_capacity_updates.get() {
            self.correction_max_per_sink_worker_capacity_updates
                .set(correction_cap);
        }
    }
}

/// A minimal set of metrics imported into honeycomb for alerting.
#[derive(Debug)]
pub struct AlertsMetrics {
    pub(crate) blob_failures: IntCounter,
    pub(crate) consensus_failures: IntCounter,
}

impl AlertsMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        AlertsMetrics {
            blob_failures: registry.register(metric!(
                name: "mz_persist_blob_failures",
                help: "count of all blob operation failures",
                const_labels: {"honeycomb" => "import"},
            )),
            consensus_failures: registry.register(metric!(
                name: "mz_persist_consensus_failures",
                help: "count of determinate consensus operation failures",
                const_labels: {"honeycomb" => "import"},
            )),
        }
    }
}

/// Metrics for the PubSubServer implementation.
#[derive(Debug)]
pub struct PubSubServerMetrics {
    pub(crate) active_connections: UIntGauge,
    pub(crate) broadcasted_diff_count: IntCounter,
    pub(crate) broadcasted_diff_bytes: IntCounter,
    pub(crate) broadcasted_diff_dropped_channel_full: IntCounter,

    pub(crate) push_seconds: Counter,
    pub(crate) subscribe_seconds: Counter,
    pub(crate) unsubscribe_seconds: Counter,
    pub(crate) connection_cleanup_seconds: Counter,

    pub(crate) push_call_count: IntCounter,
    pub(crate) subscribe_call_count: IntCounter,
    pub(crate) unsubscribe_call_count: IntCounter,
}

impl PubSubServerMetrics {
    pub(crate) fn new(registry: &MetricsRegistry) -> Self {
        let op_timings: CounterVec = registry.register(metric!(
                name: "mz_persist_pubsub_server_operation_seconds",
                help: "time spent in pubsub server performing each operation",
                var_labels: ["op"],
        ));
        let call_count: IntCounterVec = registry.register(metric!(
                name: "mz_persist_pubsub_server_call_count",
                help: "count of each pubsub server message received",
                var_labels: ["call"],
        ));

        Self {
            active_connections: registry.register(metric!(
                    name: "mz_persist_pubsub_server_active_connections",
                    help: "number of active connections to server",
            )),
            broadcasted_diff_count: registry.register(metric!(
                    name: "mz_persist_pubsub_server_broadcasted_diff_count",
                    help: "count of total broadcast diff messages sent",
            )),
            broadcasted_diff_bytes: registry.register(metric!(
                    name: "mz_persist_pubsub_server_broadcasted_diff_bytes",
                    help: "count of total broadcast diff bytes sent",
            )),
            broadcasted_diff_dropped_channel_full: registry.register(metric!(
                    name: "mz_persist_pubsub_server_broadcasted_diff_dropped_channel_full",
                    help: "count of diffs dropped due to full connection channel",
            )),

            push_seconds: op_timings.with_label_values(&["push"]),
            subscribe_seconds: op_timings.with_label_values(&["subscribe"]),
            unsubscribe_seconds: op_timings.with_label_values(&["unsubscribe"]),
            connection_cleanup_seconds: op_timings.with_label_values(&["cleanup"]),

            push_call_count: call_count.with_label_values(&["push"]),
            subscribe_call_count: call_count.with_label_values(&["subscribe"]),
            unsubscribe_call_count: call_count.with_label_values(&["unsubscribe"]),
        }
    }
}

/// Metrics for the PubSubClient implementation.
#[derive(Debug)]
pub struct PubSubClientMetrics {
    pub sender: PubSubClientSenderMetrics,
    pub receiver: PubSubClientReceiverMetrics,
    pub grpc_connection: PubSubGrpcClientConnectionMetrics,
}

impl PubSubClientMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        PubSubClientMetrics {
            sender: PubSubClientSenderMetrics::new(registry),
            receiver: PubSubClientReceiverMetrics::new(registry),
            grpc_connection: PubSubGrpcClientConnectionMetrics::new(registry),
        }
    }
}

#[derive(Debug)]
pub struct PubSubGrpcClientConnectionMetrics {
    pub(crate) connected: UIntGauge,
    pub(crate) connection_established_count: IntCounter,
    pub(crate) connect_call_attempt_count: IntCounter,
    pub(crate) broadcast_recv_lagged_count: IntCounter,
    pub(crate) grpc_error_count: IntCounter,
}

impl PubSubGrpcClientConnectionMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        Self {
            connected: registry.register(metric!(
                    name: "mz_persist_pubsub_client_grpc_connected",
                    help: "whether the grpc client is currently connected",
            )),
            connection_established_count: registry.register(metric!(
                    name: "mz_persist_pubsub_client_grpc_connection_established_count",
                    help: "count of grpc connection establishments to pubsub server",
            )),
            connect_call_attempt_count: registry.register(metric!(
                    name: "mz_persist_pubsub_client_grpc_connect_call_attempt_count",
                    help: "count of connection call attempts (including retries) to pubsub server",
            )),
            broadcast_recv_lagged_count: registry.register(metric!(
                    name: "mz_persist_pubsub_client_grpc_broadcast_recv_lagged_count",
                    help: "times a message was missed by broadcast receiver due to lag",
            )),
            grpc_error_count: registry.register(metric!(
                    name: "mz_persist_pubsub_client_grpc_error_count",
                    help: "count of grpc errors received",
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PubSubClientReceiverMetrics {
    pub(crate) push_received: IntCounter,
    pub(crate) unknown_message_received: IntCounter,
    pub(crate) approx_diff_latency_seconds: Histogram,

    pub(crate) state_pushed_diff_fast_path: IntCounter,
    pub(crate) state_pushed_diff_slow_path_succeeded: IntCounter,
    pub(crate) state_pushed_diff_slow_path_failed: IntCounter,
}

impl PubSubClientReceiverMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        let call_received: IntCounterVec = registry.register(metric!(
                name: "mz_persist_pubsub_client_call_received",
                help: "times a pubsub client call was received",
                var_labels: ["call"],
        ));

        Self {
            push_received: call_received.with_label_values(&["push"]),
            unknown_message_received: call_received.with_label_values(&["unknown"]),
            approx_diff_latency_seconds: registry.register(metric!(
                name: "mz_persist_pubsub_client_approx_diff_apply_latency_seconds",
                help: "histogram of (approximate) latency between sending a diff and applying it",
                buckets: prometheus::exponential_buckets(0.001, 2.0, 13).expect("buckets"),
            )),

            state_pushed_diff_fast_path: registry.register(metric!(
                name: "mz_persist_pubsub_client_receiver_state_push_diff_fast_path",
                help: "count fast-path state push_diff calls",
            )),
            state_pushed_diff_slow_path_succeeded: registry.register(metric!(
                name: "mz_persist_pubsub_client_receiver_state_push_diff_slow_path_succeeded",
                help: "count of successful slow-path state push_diff calls",
            )),
            state_pushed_diff_slow_path_failed: registry.register(metric!(
                name: "mz_persist_pubsub_client_receiver_state_push_diff_slow_path_failed",
                help: "count of unsuccessful slow-path state push_diff calls",
            )),
        }
    }
}

#[derive(Debug)]
pub struct PubSubClientSenderMetrics {
    pub push: PubSubClientCallMetrics,
    pub subscribe: PubSubClientCallMetrics,
    pub unsubscribe: PubSubClientCallMetrics,
}

#[derive(Debug)]
pub struct PubSubClientCallMetrics {
    pub(crate) succeeded: IntCounter,
    pub(crate) bytes_sent: IntCounter,
    pub(crate) failed: IntCounter,
}

impl PubSubClientSenderMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        let call_bytes_sent: IntCounterVec = registry.register(metric!(
                name: "mz_persist_pubsub_client_call_bytes_sent",
                help: "number of bytes sent for a given pubsub client call",
                var_labels: ["call"],
        ));
        let call_succeeded: IntCounterVec = registry.register(metric!(
                name: "mz_persist_pubsub_client_call_succeeded",
                help: "times a pubsub client call succeeded",
                var_labels: ["call"],
        ));
        let call_failed: IntCounterVec = registry.register(metric!(
                name: "mz_persist_pubsub_client_call_failed",
                help: "times a pubsub client call failed",
                var_labels: ["call"],
        ));

        Self {
            push: PubSubClientCallMetrics {
                succeeded: call_succeeded.with_label_values(&["push"]),
                failed: call_failed.with_label_values(&["push"]),
                bytes_sent: call_bytes_sent.with_label_values(&["push"]),
            },
            subscribe: PubSubClientCallMetrics {
                succeeded: call_succeeded.with_label_values(&["subscribe"]),
                failed: call_failed.with_label_values(&["subscribe"]),
                bytes_sent: call_bytes_sent.with_label_values(&["subscribe"]),
            },
            unsubscribe: PubSubClientCallMetrics {
                succeeded: call_succeeded.with_label_values(&["unsubscribe"]),
                failed: call_failed.with_label_values(&["unsubscribe"]),
                bytes_sent: call_bytes_sent.with_label_values(&["unsubscribe"]),
            },
        }
    }
}

#[derive(Debug)]
pub struct LocksMetrics {
    pub(crate) applier_read_cacheable: LockMetrics,
    pub(crate) applier_read_noncacheable: LockMetrics,
    pub(crate) applier_write: LockMetrics,
    pub(crate) watch: LockMetrics,
}

#[derive(Debug, Clone)]
pub struct LockMetrics {
    pub(crate) acquire_count: IntCounter,
    pub(crate) blocking_acquire_count: IntCounter,
    pub(crate) blocking_seconds: Counter,
}

#[derive(Debug)]
pub struct WatchMetrics {
    pub(crate) listen_woken_via_watch: IntCounter,
    pub(crate) listen_woken_via_sleep: IntCounter,
    pub(crate) listen_resolved_via_watch: IntCounter,
    pub(crate) listen_resolved_via_sleep: IntCounter,
    pub(crate) snapshot_woken_via_watch: IntCounter,
    pub(crate) snapshot_woken_via_sleep: IntCounter,
    pub(crate) notify_sent: IntCounter,
    pub(crate) notify_noop: IntCounter,
    pub(crate) notify_recv: IntCounter,
    pub(crate) notify_lagged: IntCounter,
    pub(crate) notify_wait_started: IntCounter,
    pub(crate) notify_wait_finished: IntCounter,
}

impl WatchMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        WatchMetrics {
            listen_woken_via_watch: registry.register(metric!(
                name: "mz_persist_listen_woken_via_watch",
                help: "count of listen next batches wakes via watch notify",
            )),
            listen_woken_via_sleep: registry.register(metric!(
                name: "mz_persist_listen_woken_via_sleep",
                help: "count of listen next batches wakes via sleep",
            )),
            listen_resolved_via_watch: registry.register(metric!(
                name: "mz_persist_listen_resolved_via_watch",
                help: "count of listen next batches resolved via watch notify",
            )),
            listen_resolved_via_sleep: registry.register(metric!(
                name: "mz_persist_listen_resolved_via_sleep",
                help: "count of listen next batches resolved via sleep",
            )),
            snapshot_woken_via_watch: registry.register(metric!(
                name: "mz_persist_snapshot_woken_via_watch",
                help: "count of snapshot wakes via watch notify",
            )),
            snapshot_woken_via_sleep: registry.register(metric!(
                name: "mz_persist_snapshot_woken_via_sleep",
                help: "count of snapshot wakes via sleep",
            )),
            notify_sent: registry.register(metric!(
                name: "mz_persist_watch_notify_sent",
                help: "count of watch notifications sent to a non-empty broadcast channel",
            )),
            notify_noop: registry.register(metric!(
                name: "mz_persist_watch_notify_noop",
                help: "count of watch notifications sent to an broadcast channel",
            )),
            notify_recv: registry.register(metric!(
                name: "mz_persist_watch_notify_recv",
                help: "count of watch notifications received from the broadcast channel",
            )),
            notify_lagged: registry.register(metric!(
                name: "mz_persist_watch_notify_lagged",
                help: "count of lagged events in the watch notification broadcast channel",
            )),
            notify_wait_started: registry.register(metric!(
                name: "mz_persist_watch_notify_wait_started",
                help: "count of watch wait calls started",
            )),
            notify_wait_finished: registry.register(metric!(
                name: "mz_persist_watch_notify_wait_finished",
                help: "count of watch wait calls resolved",
            )),
        }
    }
}

#[derive(Debug)]
pub struct PushdownMetrics {
    pub(crate) parts_filtered_count: IntCounter,
    pub(crate) parts_filtered_bytes: IntCounter,
    pub(crate) parts_fetched_count: IntCounter,
    pub(crate) parts_fetched_bytes: IntCounter,
    pub(crate) parts_audited_count: IntCounter,
    pub(crate) parts_audited_bytes: IntCounter,
    pub(crate) parts_stats_trimmed_count: IntCounter,
    pub(crate) parts_stats_trimmed_bytes: IntCounter,
    pub part_stats: PartStatsMetrics,
}

impl PushdownMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        PushdownMetrics {
            parts_filtered_count: registry.register(metric!(
                name: "mz_persist_pushdown_parts_filtered_count",
                help: "count of parts filtered by pushdown",
            )),
            parts_filtered_bytes: registry.register(metric!(
                name: "mz_persist_pushdown_parts_filtered_bytes",
                help: "total size of parts filtered by pushdown in bytes",
            )),
            parts_fetched_count: registry.register(metric!(
                name: "mz_persist_pushdown_parts_fetched_count",
                help: "count of parts not filtered by pushdown",
            )),
            parts_fetched_bytes: registry.register(metric!(
                name: "mz_persist_pushdown_parts_fetched_bytes",
                help: "total size of parts not filtered by pushdown in bytes",
            )),
            parts_audited_count: registry.register(metric!(
                name: "mz_persist_pushdown_parts_audited_count",
                help: "count of parts fetched only for pushdown audit",
            )),
            parts_audited_bytes: registry.register(metric!(
                name: "mz_persist_pushdown_parts_audited_bytes",
                help: "total size of parts fetched only for pushdown audit",
            )),
            parts_stats_trimmed_count: registry.register(metric!(
                name: "mz_persist_pushdown_parts_stats_trimmed_count",
                help: "count of trimmed part stats",
            )),
            parts_stats_trimmed_bytes: registry.register(metric!(
                name: "mz_persist_pushdown_parts_stats_trimmed_bytes",
                help: "total bytes trimmed from part stats",
            )),
            part_stats: PartStatsMetrics::new(registry),
        }
    }
}

#[derive(Debug)]
pub struct ConsolidationMetrics {
    pub(crate) parts_fetched: IntCounter,
    pub(crate) parts_skipped: IntCounter,
    pub(crate) parts_wasted: IntCounter,
}

impl ConsolidationMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        ConsolidationMetrics {
            parts_fetched: registry.register(metric!(
                name: "mz_persist_consolidation_parts_fetched_count",
                help: "count of parts that were fetched and used during consolidation",
            )),
            parts_skipped: registry.register(metric!(
                name: "mz_persist_consolidation_parts_skipped_count",
                help: "count of parts that were never needed during consolidation",
            )),
            parts_wasted: registry.register(metric!(
                name: "mz_persist_consolidation_parts_wasted_count",
                help: "count of parts that were fetched but not needed during consolidation",
            )),
        }
    }
}

#[derive(Debug)]
pub struct BlobMemCache {
    pub(crate) size_blobs: UIntGauge,
    pub(crate) size_bytes: UIntGauge,
    pub(crate) hits_blobs: IntCounter,
    pub(crate) hits_bytes: IntCounter,
    pub(crate) evictions: IntCounter,
}

impl BlobMemCache {
    fn new(registry: &MetricsRegistry) -> Self {
        BlobMemCache {
            size_blobs: registry.register(metric!(
                name: "mz_persist_blob_cache_size_blobs",
                help: "count of blobs in the cache",
                const_labels: {"cache" => "mem"},
            )),
            size_bytes: registry.register(metric!(
                name: "mz_persist_blob_cache_size_bytes",
                help: "total size of blobs in the cache",
                const_labels: {"cache" => "mem"},
            )),
            hits_blobs: registry.register(metric!(
                name: "mz_persist_blob_cache_hits_blobs",
                help: "count of blobs served via cache instead of s3",
                const_labels: {"cache" => "mem"},
            )),
            hits_bytes: registry.register(metric!(
                name: "mz_persist_blob_cache_hits_bytes",
                help: "total size of blobs served via cache instead of s3",
                const_labels: {"cache" => "mem"},
            )),
            evictions: registry.register(metric!(
                name: "mz_persist_blob_cache_evictions",
                help: "count of capacity-based cache evictions",
                const_labels: {"cache" => "mem"},
            )),
        }
    }
}

#[derive(Debug)]
pub struct ExternalOpMetrics {
    started: IntCounter,
    succeeded: IntCounter,
    failed: IntCounter,
    bytes: IntCounter,
    seconds: Counter,
    seconds_histogram: Option<Histogram>,
    alerts_metrics: Arc<AlertsMetrics>,
}

impl ExternalOpMetrics {
    async fn run_op<R, F, OpFn, ErrFn>(
        &self,
        op_fn: OpFn,
        on_err_fn: ErrFn,
    ) -> Result<R, ExternalError>
    where
        F: std::future::Future<Output = Result<R, ExternalError>>,
        OpFn: FnOnce() -> F,
        ErrFn: FnOnce(&AlertsMetrics, &ExternalError),
    {
        self.started.inc();
        let start = Instant::now();
        let res = op_fn().await;
        let elapsed_seconds = start.elapsed().as_secs_f64();
        self.seconds.inc_by(elapsed_seconds);
        if let Some(h) = &self.seconds_histogram {
            h.observe(elapsed_seconds);
        }
        match res.as_ref() {
            Ok(_) => self.succeeded.inc(),
            Err(err) => {
                self.failed.inc();
                on_err_fn(&self.alerts_metrics, err);
            }
        };
        res
    }

    fn run_stream<'a, R: 'a, S, OpFn, ErrFn>(
        &'a self,
        op_fn: OpFn,
        mut on_err_fn: ErrFn,
    ) -> impl futures::Stream<Item = Result<R, ExternalError>> + 'a
    where
        S: futures::Stream<Item = Result<R, ExternalError>> + Unpin + 'a,
        OpFn: FnOnce() -> S,
        ErrFn: FnMut(&AlertsMetrics, &ExternalError) + 'a,
    {
        self.started.inc();
        let start = Instant::now();
        let mut stream = op_fn();
        stream! {
            let mut succeeded = true;
            while let Some(res) = stream.next().await {
                if let Err(err) = res.as_ref() {
                    on_err_fn(&self.alerts_metrics, err);
                    succeeded = false;
                }
                yield res;
            }
            if succeeded {
                self.succeeded.inc()
            } else {
                self.failed.inc()
            }
            let elapsed_seconds = start.elapsed().as_secs_f64();
            self.seconds.inc_by(elapsed_seconds);
            if let Some(h) = &self.seconds_histogram {
                h.observe(elapsed_seconds);
            }
        }
    }
}

#[derive(Debug)]
pub struct BlobMetrics {
    set: ExternalOpMetrics,
    get: ExternalOpMetrics,
    list_keys: ExternalOpMetrics,
    delete: ExternalOpMetrics,
    restore: ExternalOpMetrics,
    delete_noop: IntCounter,
    blob_sizes: Histogram,
    pub rtt_latency: Gauge,
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

    fn on_err(alerts_metrics: &AlertsMetrics, _err: &ExternalError) {
        alerts_metrics.blob_failures.inc()
    }
}

#[async_trait]
impl Blob for MetricsBlob {
    #[instrument(name = "blob::get", fields(shard=blob_key_shard_id(key)))]
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        let res = self
            .metrics
            .blob
            .get
            .run_op(|| self.blob.get(key), Self::on_err)
            .await;
        if let Ok(Some(value)) = res.as_ref() {
            self.metrics
                .blob
                .get
                .bytes
                .inc_by(u64::cast_from(value.len()));
        }
        res
    }

    #[instrument(name = "blob::list_keys_and_metadata", fields(shard=blob_key_shard_id(key_prefix)))]
    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        let mut byte_total = 0;
        let mut instrumented = |blob_metadata: BlobMetadata| {
            // Track the size of the _keys_, not the blobs, so that we get a
            // sense for how much network bandwidth these calls are using.
            byte_total += blob_metadata.key.len();
            f(blob_metadata)
        };

        let res = self
            .metrics
            .blob
            .list_keys
            .run_op(
                || {
                    self.blob
                        .list_keys_and_metadata(key_prefix, &mut instrumented)
                },
                Self::on_err,
            )
            .await;

        self.metrics
            .blob
            .list_keys
            .bytes
            .inc_by(u64::cast_from(byte_total));

        res
    }

    #[instrument(name = "blob::set", fields(shard=blob_key_shard_id(key),size_bytes=value.len()))]
    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        let bytes = value.len();
        let res = self
            .metrics
            .blob
            .set
            .run_op(|| self.blob.set(key, value), Self::on_err)
            .await;
        if res.is_ok() {
            self.metrics.blob.set.bytes.inc_by(u64::cast_from(bytes));
            self.metrics.blob.blob_sizes.observe(f64::cast_lossy(bytes));
        }
        res
    }

    #[instrument(name = "blob::delete", fields(shard=blob_key_shard_id(key)))]
    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        let bytes = self
            .metrics
            .blob
            .delete
            .run_op(|| self.blob.delete(key), Self::on_err)
            .await?;
        if let Some(bytes) = bytes {
            self.metrics.blob.delete.bytes.inc_by(u64::cast_from(bytes));
        } else {
            self.metrics.blob.delete_noop.inc();
        }
        Ok(bytes)
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        self.metrics
            .blob
            .restore
            .run_op(|| self.blob.restore(key), Self::on_err)
            .await
    }
}

#[derive(Debug)]
pub struct ConsensusMetrics {
    list_keys: ExternalOpMetrics,
    head: ExternalOpMetrics,
    compare_and_set: ExternalOpMetrics,
    scan: ExternalOpMetrics,
    truncate: ExternalOpMetrics,
    truncated_count: IntCounter,
    pub rtt_latency: Gauge,
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

    fn on_err(alerts_metrics: &AlertsMetrics, err: &ExternalError) {
        // As of 2022-09-06, regular determinate errors are expected in
        // Consensus (i.e. "txn conflict, please retry"), so only count the
        // indeterminate ones.
        if let ExternalError::Indeterminate(_) = err {
            alerts_metrics.consensus_failures.inc()
        }
    }
}

#[async_trait]
impl Consensus for MetricsConsensus {
    fn list_keys(&self) -> ResultStream<String> {
        Box::pin(
            self.metrics
                .consensus
                .list_keys
                .run_stream(|| self.consensus.list_keys(), Self::on_err),
        )
    }

    #[instrument(name = "consensus::head", fields(shard=key))]
    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let res = self
            .metrics
            .consensus
            .head
            .run_op(|| self.consensus.head(key), Self::on_err)
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

    #[instrument(name = "consensus::compare_and_set", fields(shard=key,size_bytes=new.data.len()))]
    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        let bytes = new.data.len();
        let res = self
            .metrics
            .consensus
            .compare_and_set
            .run_op(
                || self.consensus.compare_and_set(key, expected, new),
                Self::on_err,
            )
            .await;
        match res.as_ref() {
            Ok(CaSResult::Committed) => self
                .metrics
                .consensus
                .compare_and_set
                .bytes
                .inc_by(u64::cast_from(bytes)),
            Ok(CaSResult::ExpectationMismatch) | Err(_) => {}
        }
        res
    }

    #[instrument(name = "consensus::scan", fields(shard=key))]
    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let res = self
            .metrics
            .consensus
            .scan
            .run_op(|| self.consensus.scan(key, from, limit), Self::on_err)
            .await;
        if let Ok(dataz) = res.as_ref() {
            let bytes: usize = dataz.iter().map(|x| x.data.len()).sum();
            self.metrics
                .consensus
                .scan
                .bytes
                .inc_by(u64::cast_from(bytes));
        }
        res
    }

    #[instrument(name = "consensus::truncate", fields(shard=key))]
    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<usize, ExternalError> {
        let deleted = self
            .metrics
            .consensus
            .truncate
            .run_op(|| self.consensus.truncate(key, seqno), Self::on_err)
            .await?;
        self.metrics
            .consensus
            .truncated_count
            .inc_by(u64::cast_from(deleted));
        Ok(deleted)
    }
}

/// A standard set of metrics for an async task. Call [TaskMetrics::instrument_task] to instrument
/// a future and report its metrics for this task type.
#[derive(Debug, Clone)]
pub struct TaskMetrics {
    f64_gauges: Vec<(Gauge, fn(&tokio_metrics::TaskMetrics) -> f64)>,
    u64_gauges: Vec<(
        GenericGauge<AtomicU64>,
        fn(&tokio_metrics::TaskMetrics) -> u64,
    )>,
    monitor: TaskMonitor,
}

impl TaskMetrics {
    pub fn new(name: &str) -> Self {
        let monitor = TaskMonitor::new();
        Self {
            f64_gauges: vec![
                (
                    Gauge::make_collector(metric!(
                        name: "mz_persist_task_total_idle_duration",
                        help: "Seconds of time spent idling, ie. waiting for a task to be woken up.",
                        const_labels: {"name" => name}
                    )),
                    |m| m.total_idle_duration.as_secs_f64(),
                ),
                (
                    Gauge::make_collector(metric!(
                        name: "mz_persist_task_total_scheduled_duration",
                        help: "Seconds of time spent scheduled, ie. ready to poll but not yet polled.",
                        const_labels: {"name" => name}
                    )),
                    |m| m.total_scheduled_duration.as_secs_f64(),
                ),
            ],
            u64_gauges: vec![
                (
                    MakeCollector::make_collector(metric!(
                        name: "mz_persist_task_total_scheduled_count",
                        help: "The total number of task schedules. Useful for computing the average scheduled time.",
                        const_labels: {"name" => name}
                    )),
                    |m| m.total_scheduled_count,
                ),
                (
                    MakeCollector::make_collector(metric!(
                        name: "mz_persist_task_total_idled_count",
                        help: "The total number of task idles. Useful for computing the average idle time.",
                        const_labels: {"name" => name}
                    ,
                    )),
                    |m| m.total_idled_count,
                ),
            ],
            monitor,
        }
    }

    /// Instrument the provided future. The expectation is that the result will be executed
    /// as a task. (See [TaskMonitor::instrument] for more context.)
    pub fn instrument_task<F>(&self, task: F) -> tokio_metrics::Instrumented<F> {
        self.monitor.instrument(task)
    }
}

impl Collector for TaskMetrics {
    fn desc(&self) -> Vec<&Desc> {
        let mut descs = Vec::with_capacity(self.f64_gauges.len() + self.u64_gauges.len());
        for (g, _) in &self.f64_gauges {
            descs.extend(g.desc());
        }
        for (g, _) in &self.u64_gauges {
            descs.extend(g.desc());
        }
        descs
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut families = Vec::with_capacity(self.f64_gauges.len() + self.u64_gauges.len());
        let metrics = self.monitor.cumulative();
        for (g, metrics_fn) in &self.f64_gauges {
            g.set(metrics_fn(&metrics));
            families.extend(g.collect());
        }
        for (g, metrics_fn) in &self.u64_gauges {
            g.set(metrics_fn(&metrics));
            families.extend(g.collect());
        }
        families
    }
}

#[derive(Debug)]
pub struct TasksMetrics {
    pub heartbeat_read: TaskMetrics,
}

impl TasksMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        let heartbeat_read = TaskMetrics::new("heartbeat_read");
        registry.register_collector(heartbeat_read.clone());
        TasksMetrics { heartbeat_read }
    }
}

fn blob_key_shard_id(key: &str) -> Option<String> {
    let (shard_id, _) = BlobKey::parse_ids(key).ok()?;
    Some(shard_id.to_string())
}

/// Encode a frontier into an i64 acceptable for use in metrics.
pub fn encode_ts_metric<T: Codec64>(ts: &Antichain<T>) -> i64 {
    // We have two problems in mapping a persist frontier into a metric.
    // First is that we only have a `T: Timestamp+Codec64`. Second, is
    // mapping an antichain to a single counter value. We solve both by
    // taking advantage of the fact that in practice, timestamps in mz are
    // currently always a u64 (and if we switch them, it will be to an i64).
    // This means that for all values that mz would actually produce,
    // interpreting the encoded bytes as a little-endian i64 will work.
    // Both of them impl PartialOrder, so in practice, there will always be
    // zero or one elements in the antichain.
    match ts.elements().first() {
        Some(ts) => i64::from_le_bytes(Codec64::encode(ts)),
        None => i64::MAX,
    }
}
