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

use std::process::Command;

use mz_ore::metrics::MetricsRegistry;
use mz_ore::request_context::{self, RequestContext};
use mz_ore::task::{JoinSetExt, RuntimeExt};
use mz_ore::tracing::{QpsTracingMode, StderrLogConfig, StderrLogFormat, TracingConfig};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

fn disabled_field() -> u64 {
    panic!("disabled field evaluated")
}

// Configure the actual global subscriber in a child process. The usual test
// attribute initializes a different global subscriber before the test starts.
#[test] // allow(test-attribute)
fn configured_request_logs_survive_async_interleaving() {
    let output = Command::new(std::env::current_exe().unwrap())
        .env("MZ_QPS_TRACING_MODE", "request")
        .env("MZ_REQUEST_CONTEXT_TEST_CHILD", "1")
        .args(["--exact", "configured_request_only_child", "--nocapture"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    let events: Vec<serde_json::Value> = stderr
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events.len(), 5, "{stderr}");
    let mut seen = std::collections::BTreeSet::new();
    for event in &events {
        if event["fields"]["message"] == "background" {
            assert!(event.get("request").is_none());
            continue;
        }
        let owner = event["fields"]["owner"].as_u64().unwrap();
        assert_eq!(event["request"]["request_id"], owner);
        assert_eq!(
            event["request"]["session_id"],
            Uuid::from_u128(u128::from(owner)).to_string()
        );
        assert!(seen.insert((owner, event["level"].as_str().unwrap().to_owned())));
    }
    assert_eq!(seen.len(), 4);
}

#[test] // allow(test-attribute)
fn configured_request_only_child() {
    if std::env::var_os("MZ_REQUEST_CONTEXT_TEST_CHILD").is_none() {
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let handle = mz_ore::tracing::configure(TracingConfig {
            service_name: "test",
            stderr_log: StderrLogConfig {
                format: StderrLogFormat::Json,
                filter: EnvFilter::new("info"),
            },
            opentelemetry: None,
            #[cfg(feature = "tokio-console")]
            tokio_console: None,
            #[cfg(feature = "capture")]
            capture: None,
            sentry: None::<
                mz_ore::tracing::SentryConfig<
                    fn(&tracing::Metadata<'_>) -> sentry_tracing::EventFilter,
                >,
            >,
            build_version: "test",
            build_sha: "test",
            registry: MetricsRegistry::new(),
        })
        .await
        .unwrap();
        assert_eq!(QpsTracingMode::current(), QpsTracingMode::RequestOnly);
        let request = |owner| {
            request_context::scope(
                Some(RequestContext {
                    session_id: Uuid::from_u128(u128::from(owner)),
                    request_id: owner,
                }),
                async move {
                    let span = tracing::info_span!("internal", field = disabled_field());
                    assert!(span.is_disabled());
                    tracing::warn!(owner, "before yield");
                    tokio::task::yield_now().await;
                    assert_eq!(request_context::current().unwrap().request_id, owner);
                    let spawned = mz_ore::task::spawn_in_request(|| "request-child", async move {
                        tokio::task::yield_now().await;
                        assert_eq!(request_context::current().unwrap().request_id, owner);
                    });
                    let blocking = mz_ore::task::spawn_blocking_in_request(
                        || "request-blocking",
                        move || {
                            assert_eq!(request_context::current().unwrap().request_id, owner);
                        },
                    );
                    let runtime = tokio::runtime::Handle::current();
                    let named = runtime.spawn_named(
                        || "request-runtime",
                        request_context::scope_if_enabled(request_context::capture(), async move {
                            assert_eq!(request_context::current().unwrap().request_id, owner);
                        }),
                    );
                    let mut set = tokio::task::JoinSet::new();
                    set.spawn_named(
                        || "request-join-set",
                        request_context::scope_if_enabled(request_context::capture(), async move {
                            assert_eq!(request_context::current().unwrap().request_id, owner);
                        }),
                    );
                    spawned.await;
                    blocking.await;
                    named.await;
                    set.join_next().await.unwrap().unwrap();
                    tracing::error!(owner, "after yield");
                },
            )
        };
        tokio::join!(request(1u64), request(2u64));
        request_context::scope(
            Some(RequestContext {
                session_id: Uuid::from_u128(70),
                request_id: 80,
            }),
            async {
                mz_ore::task::spawn(|| "shared-service", async {
                    tokio::task::yield_now().await;
                    assert_eq!(request_context::current(), None);
                })
                .await;
                mz_ore::task::spawn_blocking(
                    || "shared-blocking-service",
                    || {
                        assert_eq!(request_context::current(), None);
                    },
                )
                .await;
            },
        )
        .await;
        assert_eq!(request_context::current(), None);
        assert!(
            handle
                .reload_stderr_log_filter(EnvFilter::new("info,[internal]=trace"), Vec::new())
                .is_err()
        );
        tracing::warn!("background");
        check_unpolled_cancellation().await;
    });
}

struct DropProbe {
    expected: Option<RequestContext>,
    dropped: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        assert_eq!(request_context::current(), self.expected);
        self.dropped
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

async fn check_unpolled_cancellation() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let expected = Some(RequestContext {
        session_id: Uuid::from_u128(50),
        request_id: 60,
    });
    for direct in [true, false] {
        let dropped = Arc::new(AtomicBool::new(false));
        let probe = DropProbe {
            expected,
            dropped: Arc::clone(&dropped),
        };
        let future = async move {
            let _probe = probe;
            std::future::pending::<()>().await;
        };
        if direct {
            let context =
                request_context::in_scope(expected, mz_ore::tracing::InProcessContext::obtain);
            drop(context.scope(future));
        } else {
            drop(request_context::scope_if_enabled(expected, future));
        }
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(request_context::current(), None);
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = DropProbe {
        expected,
        dropped: Arc::clone(&dropped),
    };
    let task = request_context::in_scope(expected, || {
        mz_ore::task::spawn_in_request(|| "cancel-async", async move {
            let _probe = probe;
            std::future::pending::<()>().await;
        })
    })
    .into_tokio_handle();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(dropped.load(Ordering::SeqCst));

    // Hold the runtime's sole blocking worker so the second closure is canceled
    // while still queued. Its captures must be dropped in the submitting context.
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocker = mz_ore::task::spawn_blocking(
        || "blocking-worker-holder",
        move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        },
    );
    started_rx.await.unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = DropProbe {
        expected,
        dropped: Arc::clone(&dropped),
    };
    let queued = request_context::in_scope(expected, || {
        mz_ore::task::spawn_blocking_in_request(
            || "cancel-blocking",
            move || -> () {
                let _probe = probe;
                panic!("canceled blocking closure must not run");
            },
        )
    })
    .into_tokio_handle();
    queued.abort();
    release_tx.send(()).unwrap();
    blocker.await;
    assert!(queued.await.unwrap_err().is_cancelled());
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(request_context::current(), None);
}
