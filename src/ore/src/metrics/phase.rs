// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Opt-in phase diagnostics for the QPS investigation, not a stable metrics API.
//!
//! Poll durations are wall time inside `Future::poll`, NOT thread CPU time.
//! Parent and nested phase measurements overlap. Never add their quantiles.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use pin_project::pin_project;
use prometheus::{Histogram, HistogramVec, IntGauge, IntGaugeVec};

use crate::metrics::MetricsRegistry;
use crate::stats::histogram_seconds_buckets;

/// Diagnostic mode, selected once when a subsystem registers its metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// No timestamps, histogram updates, or per-operation allocations.
    Off,
    /// Wall duration only.
    Wall,
    /// Wall duration and accumulated wall duration within future polls.
    Poll,
}

impl Mode {
    /// Read the diagnostic-only startup setting. Do not silently accept typos.
    pub fn from_env() -> Self {
        match std::env::var("MZ_QPS_PHASE_METRICS").as_deref() {
            Err(std::env::VarError::NotPresent) | Ok("off") => Self::Off,
            Ok("wall") => Self::Wall,
            Ok("poll") => Self::Poll,
            value => panic!("invalid MZ_QPS_PHASE_METRICS: {value:?}"),
        }
    }
}

/// Registration factory. Bind static phase names at startup, not per request.
#[derive(Debug)]
pub struct PhaseRegistry {
    mode: Mode,
    seconds: HistogramVec,
    active: IntGaugeVec,
}

impl PhaseRegistry {
    /// Register a subsystem's diagnostic metrics with an explicit mode.
    pub fn new(registry: &MetricsRegistry, subsystem: &str, mode: Mode) -> Self {
        let enabled: IntGauge = registry.register(crate::metric!(
            name: "qps_phase_mode",
            help: "Diagnostic mode: 0 off, 1 wall, 2 poll.",
            subsystem: subsystem,
        ));
        enabled.set(match mode {
            Mode::Off => 0,
            Mode::Wall => 1,
            Mode::Poll => 2,
        });
        Self {
            mode,
            seconds: registry.register(crate::metric!(
                name: "qps_phase_seconds",
                help: "Diagnostic phase wall or poll-wall seconds. Nested phases overlap. Returned includes errors, dropped includes cancellation. Not CPU time.",
                subsystem: subsystem,
                var_labels: ["phase", "outcome", "kind"],
                buckets: histogram_seconds_buckets(0.000_008, 16.0),
            )),
            active: registry.register(crate::metric!(
                name: "qps_phase_active",
                help: "Started diagnostic phase operations not yet returned or dropped.",
                subsystem: subsystem,
                var_labels: ["phase"],
            )),
        }
    }

    /// Bind a bounded, source-defined label. No runtime query identifiers.
    pub fn phase(&self, name: &'static str) -> Phase {
        Phase((self.mode != Mode::Off).then(|| {
            Arc::new(PhaseInner {
                poll: self.mode == Mode::Poll,
                returned: self.seconds.with_label_values(&[name, "returned", "wall"]),
                dropped: self.seconds.with_label_values(&[name, "dropped", "wall"]),
                poll_returned: self.seconds.with_label_values(&[name, "returned", "poll"]),
                poll_dropped: self.seconds.with_label_values(&[name, "dropped", "poll"]),
                active: self.active.with_label_values(&[name]),
            })
        }))
    }
}

#[derive(Debug)]
struct PhaseInner {
    poll: bool,
    returned: Histogram,
    dropped: Histogram,
    poll_returned: Histogram,
    poll_dropped: Histogram,
    active: IntGauge,
}

/// Cheaply cloneable, pre-bound metric handles. Disabled phases contain nothing.
#[derive(Clone, Default)]
pub struct Phase(Option<Arc<PhaseInner>>);

impl std::fmt::Debug for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Enclosing commands can be traced. Never format Prometheus handles
        // or their contents as part of an otherwise unrelated command span.
        f.debug_struct("Phase")
            .field("enabled", &self.0.is_some())
            .finish()
    }
}

impl Phase {
    /// Begin a wall-time interval, including explicit cross-task queues.
    /// Use [`Self::time`] for futures when poll accounting is also wanted.
    pub fn start(&self) -> PhaseGuard {
        PhaseGuard {
            inner: self.0.as_ref().map(|metrics| {
                metrics.active.inc();
                Running {
                    metrics: Arc::clone(metrics),
                    start: Instant::now(),
                    poll: None,
                    returned: false,
                }
            }),
        }
    }

    /// Time a future from first poll to return or drop, without replacing its waker.
    pub fn time<F: Future>(&self, future: F) -> Timed<'_, F> {
        Timed {
            future,
            phase: self,
            guard: None,
            completed: false,
        }
    }
}

/// A diagnostic wrapper with exactly one inline copy of its future.
///
/// An async function that moves the future into different await branches can
/// retain multiple copies in its layout. Nested phase wrappers then multiply
/// storage and overflow runtime stacks, including when metrics are disabled.
#[derive(Debug)]
#[must_use = "futures do nothing unless polled or awaited"]
#[pin_project]
pub struct Timed<'a, F> {
    #[pin]
    future: F,
    phase: &'a Phase,
    guard: Option<PhaseGuard>,
    completed: bool,
}

impl<F: Future> Future for Timed<'_, F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        assert!(!*this.completed, "timed future polled after completion");
        let guard = this.guard.get_or_insert_with(|| this.phase.start());
        let poll_start = guard.inner.as_mut().and_then(|running| {
            running.metrics.poll.then(|| {
                running.poll.get_or_insert(Duration::ZERO);
                Instant::now()
            })
        });
        let result = this.future.poll(cx);
        if let Some(start) = poll_start {
            *guard
                .inner
                .as_mut()
                .expect("enabled")
                .poll
                .as_mut()
                .expect("poll mode") += start.elapsed();
        }
        if result.is_ready() {
            *this.completed = true;
            this.guard.take().expect("started").finish();
        }
        result
    }
}

#[derive(Debug)]
struct Running {
    metrics: Arc<PhaseInner>,
    start: Instant,
    poll: Option<Duration>,
    returned: bool,
}

/// Owns an in-flight operation. Drop without `finish` records an interrupted
/// scope, including cancellation, panic, or an early return from manual timing.
#[derive(Default)]
pub struct PhaseGuard {
    inner: Option<Running>,
}

impl std::fmt::Debug for PhaseGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhaseGuard")
            .field("enabled", &self.inner.is_some())
            .finish()
    }
}

impl PhaseGuard {
    /// Record normal return (including a returned error), not SQL success.
    pub fn finish(mut self) {
        if let Some(running) = &mut self.inner {
            running.returned = true;
        }
    }
}

impl Drop for PhaseGuard {
    fn drop(&mut self) {
        if let Some(running) = &self.inner {
            let duration = running.start.elapsed();
            let metrics = &running.metrics;
            metrics.active.dec();
            let (wall, poll) = if running.returned {
                (&metrics.returned, &metrics.poll_returned)
            } else {
                (&metrics.dropped, &metrics.poll_dropped)
            };
            wall.observe(duration.as_secs_f64());
            if let Some(duration) = running.poll {
                poll.observe(duration.as_secs_f64());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use super::*;

    fn phase(mode: Mode) -> Phase {
        PhaseRegistry::new(&MetricsRegistry::new(), "test", mode).phase("test")
    }

    #[crate::test]
    fn disabled_does_not_create_timing_state() {
        let phase = phase(Mode::Off);
        assert!(phase.0.is_none());
        assert!(phase.start().inner.is_none());
        let mut future = pin!(phase.time(std::future::ready(7)));
        assert_eq!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(7)
        );
    }

    #[crate::test]
    fn returned_error_is_not_cancellation() {
        let phase = phase(Mode::Poll);
        let mut future = pin!(phase.time(std::future::ready(Err::<(), _>("error"))));
        assert_eq!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err("error"))
        );
        let metrics = phase.0.as_ref().expect("enabled");
        assert_eq!(metrics.returned.get_sample_count(), 1);
        assert_eq!(metrics.poll_returned.get_sample_count(), 1);
        assert_eq!(metrics.dropped.get_sample_count(), 0);
        assert_eq!(metrics.active.get(), 0);
        assert!(metrics.returned.get_sample_sum() >= metrics.poll_returned.get_sample_sum());
    }

    #[crate::test]
    fn pending_drop_balances_active_and_records_interruption() {
        let phase = phase(Mode::Poll);
        let metrics = phase.0.as_ref().expect("enabled");
        {
            let mut future = pin!(phase.time(std::future::pending::<()>()));
            assert_eq!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            );
            assert_eq!(metrics.active.get(), 1);
        }
        assert_eq!(metrics.active.get(), 0);
        assert_eq!(metrics.returned.get_sample_count(), 0);
        assert_eq!(metrics.dropped.get_sample_count(), 1);
        assert_eq!(metrics.poll_dropped.get_sample_count(), 1);
    }

    #[crate::test]
    fn unpolled_future_is_not_a_started_phase() {
        let phase = phase(Mode::Poll);
        drop(phase.time(std::future::pending::<()>()));
        let metrics = phase.0.as_ref().expect("enabled");
        assert_eq!(metrics.active.get(), 0);
        assert_eq!(metrics.dropped.get_sample_count(), 0);
    }

    #[crate::test]
    fn wall_mode_does_not_report_poll_samples() {
        let phase = phase(Mode::Wall);
        let mut future = pin!(phase.time(std::future::ready(())));
        assert_eq!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(())
        );
        let metrics = phase.0.as_ref().expect("enabled");
        assert_eq!(metrics.returned.get_sample_count(), 1);
        assert_eq!(metrics.poll_returned.get_sample_count(), 0);
        assert_eq!(metrics.active.get(), 0);
    }

    #[crate::test]
    fn synchronous_guard_distinguishes_return_and_drop() {
        let phase = phase(Mode::Poll);
        phase.start().finish();
        drop(phase.start());
        let metrics = phase.0.as_ref().expect("enabled");
        assert_eq!(metrics.returned.get_sample_count(), 1);
        assert_eq!(metrics.dropped.get_sample_count(), 1);
        assert_eq!(metrics.poll_returned.get_sample_count(), 0);
        assert_eq!(metrics.active.get(), 0);
    }

    #[crate::test]
    fn multiple_polls_are_one_operation_and_preserve_output() {
        let phase = phase(Mode::Poll);
        let mut polls = 0;
        let inner = poll_fn(|cx| {
            polls += 1;
            if polls == 3 {
                Poll::Ready(19)
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        });
        let mut future = pin!(phase.time(inner));
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(future.as_mut().poll(&mut cx), Poll::Ready(19));
        let metrics = phase.0.as_ref().expect("enabled");
        assert_eq!(metrics.returned.get_sample_count(), 1);
        assert_eq!(metrics.poll_returned.get_sample_count(), 1);
        assert_eq!(metrics.active.get(), 0);
        assert!(metrics.returned.get_sample_sum() >= metrics.poll_returned.get_sample_sum());
    }

    #[crate::test]
    fn nested_timing_has_bounded_future_size() {
        struct LargeFuture([u8; 8192]);
        impl Future for LargeFuture {
            type Output = u8;

            fn poll(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<u8> {
                Poll::Ready(self.0[0])
            }
        }

        for mode in [Mode::Off, Mode::Wall, Mode::Poll] {
            let phase = phase(mode);
            let future = phase.time(phase.time(phase.time(phase.time(LargeFuture([7; 8192])))));
            let size = std::mem::size_of_val(&future);
            assert!(
                size <= std::mem::size_of::<LargeFuture>() + 1024,
                "nested phase wrappers duplicated future storage in {mode:?}: {size} bytes"
            );
            let mut future = pin!(future);
            assert_eq!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(7)
            );
            if let Some(metrics) = &phase.0 {
                assert_eq!(metrics.returned.get_sample_count(), 4);
                assert_eq!(metrics.active.get(), 0);
            }
        }
    }
}
