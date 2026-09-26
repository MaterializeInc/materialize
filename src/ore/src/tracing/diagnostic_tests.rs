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

use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tracing::Dispatch;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{EnvFilter, Registry};
use uuid::Uuid;

use super::{QpsTracingMode, Reloader, StderrLogConfig, StderrLogFormat, stderr_logging};
use crate::request_context::{self, RequestContext};

#[derive(Clone, Default)]
struct Output(Arc<Mutex<Vec<u8>>>);

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Output {
    type Writer = Self;
    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}

impl Output {
    fn events(&self) -> Vec<Value> {
        String::from_utf8(self.0.lock().unwrap().clone())
            .unwrap()
            .lines()
            .map(|line| {
                let mut event: Value = serde_json::from_str(line).unwrap();
                assert!(event["timestamp"].is_string());
                event.as_object_mut().unwrap().remove("timestamp");
                event
            })
            .collect()
    }
}

fn build(mode: QpsTracingMode) -> (Dispatch, Reloader, Output) {
    let output = Output::default();
    let (layer, reload) = stderr_logging(
        StderrLogConfig {
            format: StderrLogFormat::Json,
            filter: EnvFilter::new("info"),
        },
        mode,
        output.clone(),
    )
    .unwrap();
    (
        Dispatch::new(Registry::default().with(layer)),
        reload,
        output,
    )
}

#[crate::test]
fn request_logs_retain_fields_and_attribution_without_constructing_spans() {
    let (dispatch, _, output) = build(QpsTracingMode::RequestOnly);
    tracing::dispatcher::with_default(&dispatch, || {
        let context = RequestContext {
            session_id: Uuid::from_u128(17),
            request_id: 29,
        };
        request_context::in_scope(Some(context), || {
            let mut captures = 0;
            let span = tracing::info_span!(target: "request_test", "internal", field = {
                captures += 1;
                7
            });
            assert!(span.is_disabled());
            assert_eq!(captures, 0);
            tracing::warn!(target: "request_test", number = 7u64, flag = true, value = "quote\"\n", "warning");
            tracing::error!(target: "request_test", "error");
            request_context::in_scope(
                None,
                || tracing::warn!(target: "request_test", "background"),
            );
        });
        assert_eq!(request_context::current(), None);
    });
    let events = output.events();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0]["level"], "WARN");
    assert_eq!(events[0]["target"], "request_test");
    assert_eq!(events[0]["fields"]["number"], 7);
    assert_eq!(events[0]["fields"]["flag"], true);
    assert_eq!(events[0]["fields"]["value"], "quote\"\n");
    assert_eq!(
        events[0]["request"]["session_id"],
        Uuid::from_u128(17).to_string()
    );
    assert_eq!(events[0]["request"]["request_id"], 29);
    assert_eq!(events[1]["level"], "ERROR");
    assert_eq!(events[0]["request"], events[1]["request"]);
    assert!(events[0].get("span").is_none());
    assert!(events[0].get("spans").is_none());
    assert!(events[2].get("request").is_none());
}

#[crate::test]
fn unsupported_dynamic_request_filters_are_rejected_without_changing_output() {
    let dynamic = "error,request_test[dispatch{kind=command}]=trace";
    for mode in [QpsTracingMode::RequestOnly, QpsTracingMode::Detailed] {
        assert!(
            stderr_logging(
                StderrLogConfig {
                    format: StderrLogFormat::Json,
                    filter: EnvFilter::new(dynamic),
                },
                mode,
                Output::default(),
            )
            .is_err()
        );
        let (dispatch, reload, output) = build(mode);
        tracing::dispatcher::with_default(&dispatch, || {
            assert!(reload(EnvFilter::new(dynamic), Vec::new()).is_err());
            tracing::warn!(target: "request_test", "still visible");
            reload(EnvFilter::new("error"), Vec::new()).unwrap();
            tracing::warn!(target: "request_test", "filtered out");
            tracing::error!(target: "request_test", "still visible");
        });
        assert_eq!(output.events().len(), 2);
    }
}

fn probe(tag: &str) {
    tracing::trace!(target: "request_test", tag, "trace");
    tracing::info!(target: "request_test", tag, "info");
    tracing::warn!(target: "request_test", tag, "warn");
    tracing::error!(target: "request_test", tag, "error");
}

fn reload_sequence(mode: QpsTracingMode) -> Vec<Value> {
    let (dispatch, reload, output) = build(mode);
    tracing::dispatcher::with_default(&dispatch, || {
        let live = tracing::info_span!(target: "request_test", "dispatch", kind = "command");
        live.in_scope(|| {
            reload(EnvFilter::new("warn"), Vec::new()).unwrap();
            probe("live static");
            reload(
                EnvFilter::new("warn,request_test[dispatch{kind=command}]=trace"),
                Vec::new(),
            )
            .unwrap();
            probe("live dynamic");
        });
        for directive in [
            "warn,request_test[dispatch{kind=command}]=trace",
            "info",
            "off",
            "request_test=trace,warn",
        ] {
            reload(EnvFilter::new(directive), Vec::new()).unwrap();
            let span = tracing::info_span!(target: "request_test", "dispatch", kind = "command");
            span.in_scope(|| {
                probe(directive);
                span.record("kind", "other");
                probe("recorded");
            });
        }
    });
    output.events()
}

#[crate::test]
fn filter_only_matches_baseline_through_live_span_reloads() {
    let baseline = reload_sequence(QpsTracingMode::Baseline);
    assert!(baseline.iter().any(|event| event["level"] == "TRACE"));
    assert_eq!(baseline, reload_sequence(QpsTracingMode::FilterOnly));
}

#[crate::test]
fn concurrent_filter_reload_keeps_all_warnings() {
    for mode in [QpsTracingMode::Baseline, QpsTracingMode::FilterOnly] {
        let (dispatch, reload, output) = build(mode);
        // Interest rebuilding must see the test dispatcher on the reload thread
        // too. Production uses a global dispatcher on every thread.
        let _default = tracing::dispatcher::set_default(&dispatch);
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let dispatch = dispatch.clone();
                scope.spawn(move || tracing::dispatcher::with_default(&dispatch, || {
                    for item in 0..200 {
                        let span = tracing::info_span!(target: "request_test", "dispatch", kind = "command");
                        span.in_scope(|| tracing::warn!(target: "request_test", worker, item, "visible"));
                    }
                }));
            }
            for _ in 0..20 {
                reload(
                    EnvFilter::new("warn,request_test[dispatch{kind=command}]=trace"),
                    Vec::new(),
                )
                .unwrap();
                reload(EnvFilter::new("info"), Vec::new()).unwrap();
            }
        });
        let events = output.events();
        assert_eq!(events.len(), 800);
        let mut seen = std::collections::BTreeSet::new();
        for event in events {
            assert_eq!(event["level"], "WARN");
            assert!(seen.insert((
                event["fields"]["worker"].as_u64().unwrap(),
                event["fields"]["item"].as_u64().unwrap()
            )));
        }
    }
}

#[crate::test]
fn request_logs_keep_bridged_log_target_and_message() {
    let (dispatch, _, output) = build(QpsTracingMode::RequestOnly);
    tracing::dispatcher::with_default(&dispatch, || {
        tracing_log::format_trace(
            &tracing_log::log::Record::builder()
                .level(tracing_log::log::Level::Warn)
                .target("dependency_target")
                .args(format_args!("dependency warning {}", 3))
                .build(),
        )
        .unwrap();
    });
    let events = output.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["target"], "dependency_target");
    assert_eq!(events[0]["level"], "WARN");
    assert_eq!(events[0]["fields"]["message"], "dependency warning 3");
}

#[crate::test]
fn request_diagnostic_rejects_incompatible_configuration() {
    let mut config = super::TracingConfig {
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
            super::SentryConfig<fn(&tracing::Metadata<'_>) -> sentry_tracing::EventFilter>,
        >,
        build_version: "test",
        build_sha: "test",
        registry: crate::metrics::MetricsRegistry::new(),
    };
    assert!(QpsTracingMode::RequestOnly.validate(&config).is_ok());
    assert!(QpsTracingMode::Detailed.validate(&config).is_err());
    config.stderr_log.format = StderrLogFormat::Text { prefix: None };
    assert!(QpsTracingMode::RequestOnly.validate(&config).is_err());
    config.stderr_log.format = StderrLogFormat::Json;
    config.sentry = Some(super::SentryConfig {
        dsn: "unused".to_string(),
        environment: None,
        tags: Default::default(),
        event_filter: |_| sentry_tracing::EventFilter::Ignore,
    });
    assert!(QpsTracingMode::RequestOnly.validate(&config).is_err());
    assert!(QpsTracingMode::Baseline.validate(&config).is_ok());
    assert!(QpsTracingMode::FilterOnly.validate(&config).is_ok());
    config.sentry = None;
    config.opentelemetry = Some(super::OpenTelemetryConfig {
        endpoint: "http://unused.invalid".to_string(),
        headers: Default::default(),
        filter: EnvFilter::new("info"),
        max_batch_queue_size: 10,
        max_export_batch_size: 10,
        max_concurrent_exports: 1,
        batch_scheduled_delay: std::time::Duration::from_secs(1),
        max_export_timeout: std::time::Duration::from_secs(1),
        resource: opentelemetry_sdk::Resource::builder_empty().build(),
    });
    assert!(QpsTracingMode::RequestOnly.validate(&config).is_err());
    assert!(QpsTracingMode::Detailed.validate(&config).is_ok());
}
