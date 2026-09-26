// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Process-wide optimizer admission for an isolated QPS diagnostic.
//!
//! `MZ_QPS_OPTIMIZER_CONCURRENCY` is read once. Unset or `0` preserves direct
//! blocking offload, `unbounded` measures the instrumented wrapper without a
//! limit, and a positive integer caps admitted optimizer closures. This does
//! not limit other blocking work or optimizer work performed inline.

use std::sync::{Arc, LazyLock};
use std::time::Instant;

use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::stats::histogram_seconds_buckets;
use mz_ore::task::{self, JoinHandle};
use prometheus::{Histogram, IntCounter, IntGauge};
use tokio::sync::Semaphore;

static EXECUTOR: LazyLock<Executor> = LazyLock::new(|| {
    let value = std::env::var("MZ_QPS_OPTIMIZER_CONCURRENCY");
    let mode = match value {
        Ok(value) => Mode::parse(&value),
        Err(std::env::VarError::NotPresent) => Ok(Mode::Disabled),
        Err(err) => Err(err.to_string()),
    }
    .expect("invalid MZ_QPS_OPTIMIZER_CONCURRENCY");
    Executor::new(mode)
});

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Disabled,
    Unbounded,
    Bounded(usize),
}

impl Mode {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "0" => Ok(Self::Disabled),
            "unbounded" => Ok(Self::Unbounded),
            _ => match value.parse::<usize>() {
                Ok(n) if n > 0 && n <= Semaphore::MAX_PERMITS => Ok(Self::Bounded(n)),
                _ => Err("expected 0, unbounded, or a positive semaphore capacity".into()),
            },
        }
    }
}

/// Registers diagnostic counters and validates the process-wide startup mode.
pub(crate) fn register_metrics(registry: &MetricsRegistry) {
    let metrics = &EXECUTOR.metrics;
    registry.register_collector(metrics.limit.clone());
    registry.register_collector(metrics.submitted.clone());
    registry.register_collector(metrics.started.clone());
    registry.register_collector(metrics.finished.clone());
    registry.register_collector(metrics.queued.clone());
    registry.register_collector(metrics.admitted.clone());
    registry.register_collector(metrics.running.clone());
    registry.register_collector(metrics.admission_seconds.clone());
    registry.register_collector(metrics.dispatch_seconds.clone());
    registry.register_collector(metrics.execution_seconds.clone());
}

/// Spawns optimizer work without waiting for admission on the caller's task.
///
/// Dropping the handle detaches work, as with `spawn_blocking`. Explicitly
/// aborting the handle can cancel admission, but cannot stop an executing
/// closure. Closures must not wait on other jobs admitted through this gate.
pub(crate) fn spawn<F, T, N, NC>(name: NC, function: F) -> JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
    N: AsRef<str>,
    NC: FnOnce() -> N + Send + 'static,
{
    EXECUTOR.spawn(name, function)
}

struct Executor {
    mode: Mode,
    permits: Option<Arc<Semaphore>>,
    metrics: Metrics,
}

impl Executor {
    fn new(mode: Mode) -> Self {
        Self {
            mode,
            permits: match mode {
                Mode::Bounded(n) => Some(Arc::new(Semaphore::new(n))),
                Mode::Disabled | Mode::Unbounded => None,
            },
            metrics: Metrics::new(mode),
        }
    }

    fn spawn<F, T, N, NC>(&self, name: NC, function: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
        N: AsRef<str>,
        NC: FnOnce() -> N + Send + 'static,
    {
        if self.mode == Mode::Disabled {
            return task::spawn_blocking(name, function);
        }
        let submitted_at = Instant::now();
        self.metrics.submitted.inc();
        let queued = GaugeGuard::new(self.metrics.queued.clone());
        let permits = self.permits.clone();
        let metrics = self.metrics.clone();
        let dispatch = tracing::dispatcher::get_default(Clone::clone);
        task::spawn(|| "optimizer admission", async move {
            let permit = match permits {
                Some(permits) => Some(permits.acquire_owned().await.expect("gate never closes")),
                None => None,
            };
            drop(queued);
            metrics
                .admission_seconds
                .observe(submitted_at.elapsed().as_secs_f64());
            let admitted = GaugeGuard::new(metrics.admitted.clone());
            let admitted_at = Instant::now();
            task::spawn_blocking(name, move || {
                // The closure owns admission even when its async waiter is
                // canceled. Releasing here prevents oversubscription by
                // work that Tokio can no longer abort.
                let _permit = permit;
                let _admitted = admitted;
                metrics
                    .dispatch_seconds
                    .observe(admitted_at.elapsed().as_secs_f64());
                let _running = GaugeGuard::new(metrics.running.clone());
                metrics.started.inc();
                let _execution = ExecutionGuard {
                    started_at: Instant::now(),
                    seconds: metrics.execution_seconds,
                    finished: metrics.finished,
                };
                tracing::dispatcher::with_default(&dispatch, function)
            })
            .await
        })
    }
}

struct GaugeGuard(IntGauge);

impl GaugeGuard {
    fn new(gauge: IntGauge) -> Self {
        gauge.inc();
        Self(gauge)
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

struct ExecutionGuard {
    started_at: Instant,
    seconds: Histogram,
    finished: IntCounter,
}

impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        self.seconds
            .observe(self.started_at.elapsed().as_secs_f64());
        self.finished.inc();
    }
}

#[derive(Clone)]
struct Metrics {
    limit: IntGauge,
    submitted: IntCounter,
    started: IntCounter,
    finished: IntCounter,
    queued: IntGauge,
    admitted: IntGauge,
    running: IntGauge,
    admission_seconds: Histogram,
    dispatch_seconds: Histogram,
    execution_seconds: Histogram,
}

impl Metrics {
    fn new(mode: Mode) -> Self {
        let registry = MetricsRegistry::new();
        let limit: IntGauge = registry.register(metric!(
            name: "mz_optimizer_offload_limit",
            help: "Diagnostic optimizer admission limit. Zero is disabled, minus one is unbounded.",
        ));
        limit.set(match mode {
            Mode::Disabled => 0,
            Mode::Unbounded => -1,
            Mode::Bounded(n) => i64::try_from(n).expect("semaphore capacity fits i64"),
        });
        Self {
            limit,
            submitted: registry.register(metric!(
                name: "mz_optimizer_offload_submitted_total",
                help: "Optimizer jobs submitted to the instrumented wrapper.",
            )),
            started: registry.register(metric!(
                name: "mz_optimizer_offload_started_total",
                help: "Optimizer closures started on blocking threads.",
            )),
            finished: registry.register(metric!(
                name: "mz_optimizer_offload_finished_total",
                help: "Optimizer closures exited, including errors and panics.",
            )),
            queued: registry.register(metric!(
                name: "mz_optimizer_offload_queued",
                help: "Optimizer jobs awaiting async admission, including unscheduled admission tasks.",
            )),
            admitted: registry.register(metric!(
                name: "mz_optimizer_offload_admitted",
                help: "Admitted optimizer jobs, including blocking-pool dispatch wait.",
            )),
            running: registry.register(metric!(
                name: "mz_optimizer_offload_running",
                help: "Optimizer closures executing on blocking threads.",
            )),
            admission_seconds: registry.register(metric!(
                name: "mz_optimizer_offload_admission_seconds",
                help: "Submission to admission wall time for admitted jobs.",
                buckets: histogram_seconds_buckets(0.000_001, 8.0),
            )),
            dispatch_seconds: registry.register(metric!(
                name: "mz_optimizer_offload_dispatch_seconds",
                help: "Admission to closure start wall time for started jobs.",
                buckets: histogram_seconds_buckets(0.000_001, 8.0),
            )),
            execution_seconds: registry.register(metric!(
                name: "mz_optimizer_offload_execution_seconds",
                help: "Optimizer closure wall time, including errors and panics.",
                buckets: histogram_seconds_buckets(0.000_001, 8.0),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::*;

    async fn finish<T>(handle: JoinHandle<T>) -> T {
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("job did not complete")
    }

    async fn hold_permit(executor: &Executor) -> tokio::sync::OwnedSemaphorePermit {
        Arc::clone(executor.permits.as_ref().expect("bounded executor"))
            .acquire_owned()
            .await
            .expect("gate is open")
    }

    fn assert_idle(executor: &Executor) {
        assert_eq!(executor.metrics.queued.get(), 0);
        assert_eq!(executor.metrics.admitted.get(), 0);
        assert_eq!(executor.metrics.running.get(), 0);
    }

    #[mz_ore::test]
    fn configuration() {
        assert_eq!(Mode::parse("0"), Ok(Mode::Disabled));
        assert_eq!(Mode::parse("unbounded"), Ok(Mode::Unbounded));
        assert_eq!(Mode::parse("1"), Ok(Mode::Bounded(1)));
        assert_eq!(Mode::parse("16"), Ok(Mode::Bounded(16)));
        for invalid in ["", "-1", "off", "1.0", " 2", "18446744073709551615"] {
            assert!(Mode::parse(invalid).is_err(), "accepted {invalid:?}");
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn admission_does_not_block_async_or_unrelated_blocking_work() {
        let executor = Executor::new(Mode::Bounded(1));
        let held = hold_permit(&executor).await;
        let mut job = executor.spawn(|| "queued optimizer", || 42);
        assert_eq!(
            finish(task::spawn_blocking(|| "unrelated I/O", || 7)).await,
            7
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut job)
                .await
                .is_err()
        );
        assert_eq!(executor.metrics.queued.get(), 1);
        assert_eq!(executor.metrics.started.get(), 0);
        drop(held);
        assert_eq!(finish(job).await, 42);
        assert_eq!(executor.metrics.submitted.get(), 1);
        assert_eq!(executor.metrics.started.get(), 1);
        assert_eq!(executor.metrics.finished.get(), 1);
        assert_eq!(executor.metrics.admission_seconds.get_sample_count(), 1);
        assert_eq!(executor.metrics.dispatch_seconds.get_sample_count(), 1);
        assert_eq!(executor.metrics.execution_seconds.get_sample_count(), 1);
        assert_idle(&executor);
    }

    #[mz_ore::test(tokio::test)]
    async fn abort_queued_work_releases_queue_and_does_not_execute() {
        let executor = Executor::new(Mode::Bounded(1));
        let held = hold_permit(&executor).await;
        let executed = Arc::new(AtomicBool::new(false));
        let executed_in_task = Arc::clone(&executed);
        let job = executor.spawn(
            || "canceled optimizer",
            move || {
                executed_in_task.store(true, Ordering::SeqCst);
            },
        );
        tokio::task::yield_now().await;
        job.abort_and_wait().await;
        assert_idle(&executor);
        drop(held);
        finish(executor.spawn(|| "after cancellation", || ())).await;
        assert!(!executed.load(Ordering::SeqCst));
        assert_eq!(executor.metrics.submitted.get(), 2);
        assert_eq!(executor.metrics.started.get(), 1);
        assert_eq!(executor.metrics.finished.get(), 1);
        assert_idle(&executor);
    }

    #[mz_ore::test(tokio::test)]
    async fn abort_running_waiter_keeps_permit_until_closure_exits() {
        let executor = Executor::new(Mode::Bounded(1));
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let running = executor.spawn(
            || "running optimizer",
            move || {
                started_tx.send(()).expect("start receiver alive");
                release_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("release signal");
            },
        );
        tokio::time::timeout(Duration::from_secs(5), started_rx)
            .await
            .expect("signal arrived before timeout")
            .expect("signal sender alive");
        running.abort_and_wait().await;
        assert_eq!(executor.metrics.running.get(), 1);
        assert_eq!(executor.metrics.admitted.get(), 1);
        let mut next = executor.spawn(|| "next optimizer", || 12);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut next)
                .await
                .is_err()
        );
        release_tx.send(()).expect("blocking job still alive");
        assert_eq!(finish(next).await, 12);
        assert_eq!(executor.metrics.finished.get(), 2);
        assert_idle(&executor);
    }

    #[mz_ore::test(tokio::test)]
    async fn dropping_handle_preserves_detached_completion() {
        let executor = Executor::new(Mode::Bounded(1));
        let held = hold_permit(&executor).await;
        let (done_tx, done_rx) = oneshot::channel();
        drop(executor.spawn(
            || "detached optimizer",
            move || {
                done_tx.send(()).expect("completion receiver alive");
            },
        ));
        drop(held);
        tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .expect("signal arrived before timeout")
            .expect("signal sender alive");
        finish(executor.spawn(|| "drain optimizer", || ())).await;
        assert_eq!(executor.metrics.finished.get(), 2);
        assert_idle(&executor);
    }

    #[mz_ore::test(tokio::test)]
    async fn panic_and_error_release_permits_and_preserve_results() {
        let executor = Executor::new(Mode::Bounded(1));
        let panic = executor.spawn(|| "panic optimizer", || panic!("offload test panic"));
        let panic = tokio::time::timeout(Duration::from_secs(5), panic.into_tokio_handle())
            .await
            .expect("panic observed before timeout")
            .unwrap_err();
        assert_eq!(
            panic.into_panic().downcast_ref::<&str>(),
            Some(&"offload test panic")
        );
        assert_idle(&executor);
        let result =
            finish(executor.spawn(|| "error optimizer", || Err::<(), _>("test error"))).await;
        assert_eq!(result, Err("test error"));
        assert_eq!(executor.metrics.finished.get(), 2);
        assert_idle(&executor);
    }

    #[mz_ore::test(tokio::test)]
    async fn unbounded_and_disabled_controls_allow_parallel_work() {
        for mode in [Mode::Disabled, Mode::Unbounded] {
            let executor = Executor::new(mode);
            let (started_tx, started_rx) = oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let first = executor.spawn(
                || "first optimizer",
                move || {
                    started_tx.send(()).expect("start receiver alive");
                    release_rx
                        .recv_timeout(Duration::from_secs(5))
                        .expect("release signal");
                },
            );
            tokio::time::timeout(Duration::from_secs(5), started_rx)
                .await
                .expect("signal arrived before timeout")
                .expect("signal sender alive");
            assert_eq!(finish(executor.spawn(|| "second optimizer", || 2)).await, 2);
            release_tx.send(()).expect("blocking job still alive");
            finish(first).await;
            let expected = if mode == Mode::Disabled { 0 } else { 2 };
            assert_eq!(executor.metrics.submitted.get(), expected);
            assert_eq!(executor.metrics.finished.get(), expected);
            assert_idle(&executor);
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn multiple_permits_bound_executing_closures() {
        for limit in [1, 2, 4] {
            let executor = Executor::new(Mode::Bounded(limit));
            let running = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let jobs: Vec<_> = (0..32)
                .map(|_| {
                    let running = Arc::clone(&running);
                    let peak = Arc::clone(&peak);
                    executor.spawn(
                        || "concurrency check",
                        move || {
                            let n = running.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(n, Ordering::SeqCst);
                            let until = Instant::now() + Duration::from_millis(1);
                            while Instant::now() < until {
                                std::hint::spin_loop();
                            }
                            running.fetch_sub(1, Ordering::SeqCst);
                        },
                    )
                })
                .collect();
            for job in jobs {
                finish(job).await;
            }
            assert!(peak.load(Ordering::SeqCst) <= limit);
            assert!(peak.load(Ordering::SeqCst) > 0);
            assert_eq!(executor.metrics.finished.get(), 32);
            assert_idle(&executor);
        }
    }

    #[derive(Clone)]
    struct Capture(Arc<Mutex<Vec<Vec<String>>>>);

    impl<S> tracing_subscriber::Layer<S> for Capture
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let scope = ctx
                .event_scope(event)
                .map(|scope| {
                    scope
                        .from_root()
                        .map(|span| span.name().to_owned())
                        .collect()
                })
                .unwrap_or_default();
            self.0
                .lock()
                .expect("capture lock not poisoned")
                .push(scope);
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn queued_jobs_preserve_dispatcher_and_request_parent_without_leakage() {
        use tracing_subscriber::prelude::*;

        let executor = Executor::new(Mode::Bounded(1));
        let held = hold_permit(&executor).await;
        let first = Capture(Arc::new(Mutex::new(Vec::new())));
        let second = Capture(Arc::new(Mutex::new(Vec::new())));
        let first_dispatch =
            tracing::Dispatch::new(tracing_subscriber::registry().with(first.clone()));
        let second_dispatch =
            tracing::Dispatch::new(tracing_subscriber::registry().with(second.clone()));
        let a = tracing::dispatcher::with_default(&first_dispatch, || {
            let parent = tracing::info_span!("request_a");
            executor.spawn(
                || "attributed optimizer a",
                move || {
                    parent.in_scope(|| {
                        tracing::info_span!("optimizer").in_scope(|| tracing::info!("result a"));
                    })
                },
            )
        });
        let b = tracing::dispatcher::with_default(&second_dispatch, || {
            let parent = tracing::info_span!("request_b");
            executor.spawn(
                || "attributed optimizer b",
                move || {
                    parent.in_scope(|| {
                        tracing::info_span!("optimizer").in_scope(|| tracing::info!("result b"));
                    })
                },
            )
        });
        drop(held);
        finish(a).await;
        finish(b).await;
        assert_eq!(
            *first.0.lock().expect("capture lock not poisoned"),
            vec![vec!["request_a", "optimizer"]]
        );
        assert_eq!(
            *second.0.lock().expect("capture lock not poisoned"),
            vec![vec!["request_b", "optimizer"]]
        );
        assert_idle(&executor);
    }
}
