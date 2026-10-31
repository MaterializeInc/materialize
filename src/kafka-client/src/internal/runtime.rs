// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The process-wide I/O runtime.
//!
//! All client I/O runs on one multi-thread tokio runtime, `kafka-io`, created
//! on first use with [`IO_THREADS_ENV`] worker threads (default
//! `min(4, available_parallelism)`). It lives for the rest of the process.
//!
//! Synchronous client APIs bridge to it with [`block_on`], which waits on a std
//! channel instead of using `Handle::block_on`. That works from any thread that
//! is not a `kafka-io` thread: plain threads, timely workers, `spawn_blocking`
//! threads, and workers of other tokio runtimes (whose worker it blocks, as
//! librdkafka's synchronous calls did).

use std::cell::Cell;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use mz_ore::task::{JoinHandle, RuntimeExt};
use tokio::runtime::{Handle, Runtime};
use tracing::warn;

/// Environment variable that sets the number of `kafka-io` worker threads.
pub(crate) const IO_THREADS_ENV: &str = "MZ_KAFKA_CLIENT_IO_THREADS";

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

thread_local! {
    /// Whether the current thread belongs to the `kafka-io` runtime and may be
    /// running its tasks. Cleared while a `kafka-io` blocking thread runs a
    /// [`run_blocking`] closure, where blocking is allowed.
    static ON_IO_THREAD: Cell<bool> = const { Cell::new(false) };
}

/// The worker count for a value of [`IO_THREADS_ENV`]: a positive integer, or
/// `default` if unset or invalid.
fn io_threads(env_value: Option<&str>, default: usize) -> usize {
    match env_value.map(|v| v.trim().parse::<usize>()) {
        None => default,
        Some(Ok(n)) if n > 0 => n,
        Some(_) => {
            warn!(
                "ignoring invalid {IO_THREADS_ENV}={:?}, using {default}",
                env_value.unwrap_or_default()
            );
            default
        }
    }
}

/// Returns the handle of the `kafka-io` runtime, creating it on first use.
pub(crate) fn handle() -> &'static Handle {
    RUNTIME
        .get_or_init(|| {
            let default = std::thread::available_parallelism()
                .map_or(1, NonZeroUsize::get)
                .min(4);
            let env_value = std::env::var(IO_THREADS_ENV).ok();
            let workers = io_threads(env_value.as_deref(), default);
            let thread_id = AtomicUsize::new(0);
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(workers)
                .thread_name_fn(move || {
                    format!("kafka-io-{}", thread_id.fetch_add(1, Ordering::Relaxed))
                })
                .on_thread_start(|| ON_IO_THREAD.with(|f| f.set(true)))
                .enable_all()
                .build()
                .expect("failed to create the kafka-io runtime")
        })
        .handle()
}

/// Reports whether the current thread is a `kafka-io` thread that must not
/// block.
pub(crate) fn is_io_thread() -> bool {
    ON_IO_THREAD.with(|f| f.get())
}

/// Spawns a task on the `kafka-io` runtime.
///
/// Dropping the returned handle detaches the task; it keeps running.
pub(crate) fn spawn<F>(name: &str, fut: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    handle().spawn_named(|| name, fut)
}

/// Runs `fut` on the `kafka-io` runtime and blocks the current thread until it
/// completes. Panics in `fut` are resumed on the calling thread.
///
/// Panics if called from a `kafka-io` thread, where waiting could deadlock the
/// runtime.
pub(crate) fn block_on<F>(fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    assert!(
        !is_io_thread(),
        "mz_kafka_client: blocking client call on a kafka-io runtime thread; \
         call it from a thread that may block"
    );
    let (tx, rx) = mpsc::sync_channel(1);
    let task = spawn("kafka-client-block-on", fut).into_tokio_handle();
    // A second task observes the first one's outcome, panics included, which
    // the std channel then carries to this thread.
    spawn("kafka-client-block-on-join", async move {
        let _ = tx.send(task.await);
    });
    match rx.recv() {
        Ok(Ok(output)) => output,
        Ok(Err(e)) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Ok(Err(e)) => panic!("kafka-io task did not complete: {e}"),
        Err(mpsc::RecvError) => panic!("kafka-io runtime dropped a blocking client call"),
    }
}

/// Runs `f` on a blocking thread of the `kafka-io` runtime.
///
/// For user callbacks that may block, such as
/// [`ClientContext::resolve_broker_addr`](crate::ClientContext::resolve_broker_addr).
/// `f` may call [`block_on`]. Panics in `f` are resumed in the awaiting task.
pub(crate) async fn run_blocking<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    handle()
        .spawn_blocking_named(
            || "kafka-client-blocking",
            move || {
                let _guard = BlockingSection::enter();
                f()
            },
        )
        .await
}

/// Waits for `duration` using the `kafka-io` timer, from any executor.
pub(crate) async fn sleep(duration: Duration) {
    spawn("kafka-client-sleep", async move {
        tokio::time::sleep(duration).await
    })
    .await
}

/// Marks the current `kafka-io` blocking thread as allowed to block until
/// dropped.
struct BlockingSection {
    prev: bool,
}

impl BlockingSection {
    fn enter() -> BlockingSection {
        BlockingSection {
            prev: ON_IO_THREAD.with(|f| f.replace(false)),
        }
    }
}

impl Drop for BlockingSection {
    fn drop(&mut self) {
        ON_IO_THREAD.with(|f| f.set(self.prev));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    #[mz_ore::test]
    fn io_threads_parsing() {
        assert_eq!(io_threads(None, 3), 3);
        assert_eq!(io_threads(Some("8"), 3), 8);
        assert_eq!(io_threads(Some(" 2 "), 3), 2);
        assert_eq!(io_threads(Some("0"), 3), 3);
        assert_eq!(io_threads(Some("x"), 3), 3);
    }

    #[mz_ore::test]
    fn block_on_plain_thread() {
        assert!(!is_io_thread());
        let name = block_on(async {
            assert!(is_io_thread());
            std::thread::current().name().map(str::to_string)
        });
        assert!(name.unwrap().starts_with("kafka-io-"));
        let handle = spawn("test", async { 7 });
        assert_eq!(block_on(handle), 7);
    }

    #[mz_ore::test]
    fn block_on_from_other_runtime_worker() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let task = (&rt).spawn_named(|| "other-runtime", async {
            // Runs on the other runtime's worker thread.
            block_on(async { 40 + 2 })
        });
        assert_eq!(rt.block_on(task), 42);

        let current = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert_eq!(current.block_on(async { block_on(async { 5 }) }), 5);
    }

    #[mz_ore::test]
    #[should_panic(expected = "boom")]
    fn block_on_resumes_panics() {
        block_on(async { panic!("boom") });
    }

    #[mz_ore::test]
    #[should_panic(expected = "blocking client call on a kafka-io runtime thread")]
    fn block_on_panics_on_io_thread() {
        block_on(async { block_on(async {}) });
    }

    #[mz_ore::test]
    fn run_blocking_allows_blocking() {
        let out = block_on(run_blocking(|| {
            assert!(!is_io_thread());
            block_on(async { 3 })
        }));
        assert_eq!(out, 3);
        // The flag is restored for later tasks on that thread.
        assert!(block_on(async { is_io_thread() }));
    }

    #[mz_ore::test]
    fn sleep_from_other_runtime() {
        let current = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let start = Instant::now();
        current.block_on(sleep(Duration::from_millis(20)));
        assert!(start.elapsed() >= Duration::from_millis(20));
    }
}
