// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus monitoring metrics.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use mz_ore::cast::CastFrom;
use mz_ore::metric;
use mz_ore::metrics::{Counter, IntCounter, MetricsRegistry};
use mz_persist::location::{Atomicity, Blob, Consensus, ExternalError, SeqNo, VersionedData};
use mz_persist::retry::RetryStream;
use prometheus::{CounterVec, IntCounterVec};

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
    /// Metrics for various encodings and decodings.
    pub codecs: CodecsMetrics,
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
            expire_reader: self.cmd_metrics("expire_reader"),
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
                apply_unbatched_cmd_truncate: self.retry_metrics("apply_unbatched_cmd::truncate"),
                batch_set: self.retry_metrics("batch::set"),
                blob_open: self.retry_metrics("blob::open"),
                consensus_open: self.retry_metrics("consensus::open"),
                fetch_and_update_state_head: self.retry_metrics("fetch_and_update_state::head"),
                fetch_batch_get: self.retry_metrics("fetch_batch::get"),
                maybe_init_state_cas: self.retry_metrics("maybe_init_state::cas"),
                maybe_init_state_head: self.retry_metrics("maybe_init_state::head"),
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
        }
    }

    fn consensus_metrics(&self) -> ConsensusMetrics {
        ConsensusMetrics {
            head: self.external_op_metrics("consensus_head"),
            compare_and_set: self.external_op_metrics("consensus_cas"),
            scan: self.external_op_metrics("consensus_scan"),
            truncate: self.external_op_metrics("consensus_truncate"),
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
    pub(crate) expire_reader: CmdMetrics,
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
    pub(crate) apply_unbatched_cmd_truncate: RetryMetrics,
    pub(crate) batch_set: RetryMetrics,
    pub(crate) blob_open: RetryMetrics,
    pub(crate) consensus_open: RetryMetrics,
    pub(crate) fetch_and_update_state_head: RetryMetrics,
    pub(crate) fetch_batch_get: RetryMetrics,
    pub(crate) maybe_init_state_cas: RetryMetrics,
    pub(crate) maybe_init_state_head: RetryMetrics,
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
    async fn get(&self, deadline: Instant, key: &str) -> Result<Option<Vec<u8>>, ExternalError> {
        let res = self
            .metrics
            .blob
            .get
            .run_op(|| self.blob.get(deadline, key))
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

    async fn list_keys(&self, deadline: Instant) -> Result<Vec<String>, ExternalError> {
        let res = self
            .metrics
            .blob
            .list_keys
            .run_op(|| self.blob.list_keys(deadline))
            .await;
        if let Ok(keys) = res.as_ref() {
            let bytes = keys.iter().map(|x| x.len()).sum();
            self.metrics
                .blob
                .list_keys
                .bytes
                .inc_by(u64::cast_from(bytes));
        }
        res
    }

    async fn set(
        &self,
        deadline: Instant,
        key: &str,
        value: Bytes,
        atomic: Atomicity,
    ) -> Result<(), ExternalError> {
        let bytes = value.len();
        let res = self
            .metrics
            .blob
            .set
            .run_op(|| self.blob.set(deadline, key, value, atomic))
            .await;
        if res.is_ok() {
            self.metrics.blob.set.bytes.inc_by(u64::cast_from(bytes));
        }
        res
    }

    async fn delete(&self, deadline: Instant, key: &str) -> Result<(), ExternalError> {
        // It'd be nice if this could also track bytes somehow.
        self.metrics
            .blob
            .delete
            .run_op(|| self.blob.delete(deadline, key))
            .await
    }
}

#[derive(Debug)]
pub struct ConsensusMetrics {
    head: ExternalOpMetrics,
    compare_and_set: ExternalOpMetrics,
    scan: ExternalOpMetrics,
    truncate: ExternalOpMetrics,
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
    async fn head(
        &self,
        deadline: Instant,
        key: &str,
    ) -> Result<Option<VersionedData>, ExternalError> {
        let res = self
            .metrics
            .consensus
            .head
            .run_op(|| self.consensus.head(deadline, key))
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
        deadline: Instant,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<Result<(), Option<VersionedData>>, ExternalError> {
        let bytes = new.data.len();
        let res = self
            .metrics
            .consensus
            .compare_and_set
            .run_op(|| self.consensus.compare_and_set(deadline, key, expected, new))
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

    async fn scan(
        &self,
        deadline: Instant,
        key: &str,
        from: SeqNo,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let res = self
            .metrics
            .consensus
            .scan
            .run_op(|| self.consensus.scan(deadline, key, from))
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

    async fn truncate(
        &self,
        deadline: Instant,
        key: &str,
        seqno: SeqNo,
    ) -> Result<(), ExternalError> {
        self.metrics
            .consensus
            .truncate
            .run_op(|| self.consensus.truncate(deadline, key, seqno))
            .await
    }
}
