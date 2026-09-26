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

use std::future::Future;

use tracing::Span;

use super::{OpenTelemetryContext, QpsTracingMode};
use crate::request_context::{self, RequestContext};

/// Request and tracing context for an in-process ownership handoff.
///
/// This type is deliberately not serializable. RPCs must continue to use
/// [`OpenTelemetryContext`]. The legacy carrier remains for baseline comparison.
#[derive(Clone, Debug)]
pub struct InProcessContext {
    request: Option<RequestContext>,
    parent: Parent,
}

#[derive(Clone, Debug)]
enum Parent {
    Legacy(OpenTelemetryContext),
    Local(Span),
    Disabled,
}

impl InProcessContext {
    /// Captures the current context using the configured diagnostic mode.
    pub fn obtain() -> Self {
        Self::obtain_for(QpsTracingMode::current())
    }

    fn obtain_for(mode: QpsTracingMode) -> Self {
        let request = if mode.request_context() {
            request_context::current()
        } else {
            None
        };
        let parent = match mode {
            QpsTracingMode::Baseline | QpsTracingMode::FilterOnly => {
                Parent::Legacy(OpenTelemetryContext::obtain())
            }
            QpsTracingMode::RequestOnly => Parent::Disabled,
            QpsTracingMode::Detailed => Parent::Local(Span::current()),
        };
        Self { request, parent }
    }

    /// Returns the identity captured when the work was submitted.
    pub fn request(&self) -> Option<RequestContext> {
        self.request
    }

    /// Preserves legacy response-parent attachment for baseline comparisons.
    ///
    /// Direct mode carries its parent to the next receiver instead of changing
    /// the parent of an already-running caller span.
    pub fn attach_legacy_parent(&self) {
        if let Parent::Legacy(context) = &self.parent {
            context.attach_as_parent();
        }
    }

    /// Creates a receiver span with the captured parent.
    ///
    /// The callback must use an explicit parent for `Some`, including disabled
    /// parents. `None` requests the caller's original contextual-parent behavior
    /// for the baseline path, before attaching the legacy OTEL parent.
    pub fn span(&self, create: impl FnOnce(Option<&Span>) -> Span) -> Span {
        match &self.parent {
            Parent::Legacy(context) => {
                let span = create(None);
                context.attach_as_parent_to(&span);
                span
            }
            Parent::Local(parent) => create(Some(parent)),
            Parent::Disabled => create(Some(&Span::none())),
        }
    }

    /// Runs received work with its captured identity, restoring the receiver.
    pub fn scope<F: Future>(self, future: F) -> impl Future<Output = F::Output> {
        // Construct the scope before the async block captures the future.
        let future = if matches!(&self.parent, Parent::Legacy(_)) {
            either::Either::Left(future)
        } else {
            either::Either::Right(request_context::scope(self.request, future))
        };
        async move {
            let result = future.await;
            // Keep the direct parent alive until received work has finished.
            drop(self);
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider,
    };
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
    use tracing::{Dispatch, Instrument};
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_subscriber::layer::{Layer, SubscriberExt};
    use uuid::Uuid;

    use super::*;

    #[derive(Clone, Debug)]
    struct Capture(Arc<Mutex<Vec<SpanData>>>);
    impl SpanExporter for Capture {
        async fn export(&self, spans: Vec<SpanData>) -> OTelSdkResult {
            self.0.lock().unwrap().extend(spans);
            Ok(())
        }
    }

    #[crate::test]
    fn detailed_local_handoff_preserves_remote_and_local_parentage() {
        let spans = Arc::new(Mutex::new(Vec::new()));
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(Capture(Arc::clone(&spans)))
            .build();
        let dispatch = Dispatch::new(
            tracing_subscriber::registry().with(
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer("test"))
                    .with_filter(tracing_subscriber::filter::LevelFilter::INFO),
            ),
        );
        let remote = SpanContext::new(
            TraceId::from(1234u128),
            SpanId::from(5678u64),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let request = Some(RequestContext {
            session_id: Uuid::from_u128(12),
            request_id: 34,
        });
        let context = tracing::dispatcher::with_default(&dispatch, || {
            let root = tracing::info_span!(parent: None, "request_root");
            root.set_parent(opentelemetry::Context::new().with_remote_span_context(remote.clone()))
                .unwrap();
            root.in_scope(|| {
                request_context::in_scope(request, || {
                    InProcessContext::obtain_for(QpsTracingMode::Detailed)
                })
            })
        });
        assert_eq!(context.request(), request);
        std::thread::spawn(move || tracing::dispatcher::with_default(&dispatch, || {
            let receiver = context.span(|parent| tracing::info_span!(parent: parent.expect("direct parent"), "receiver", detail = "preserved"));
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            runtime.block_on(context.scope(async {
                assert_eq!(request_context::current(), request);
                tokio::task::yield_now().await;
                assert_eq!(request_context::current(), request);
            }.instrument(receiver)));
            assert_eq!(request_context::current(), None);
        })).join().unwrap();
        provider.force_flush().unwrap();
        let spans = spans.lock().unwrap();
        assert_eq!(
            spans.len(),
            2,
            "{:?}",
            spans.iter().map(|span| &span.name).collect::<Vec<_>>()
        );
        let root = spans.iter().find(|s| s.name == "request_root").unwrap();
        let receiver = spans.iter().find(|s| s.name == "receiver").unwrap();
        assert_eq!(root.parent_span_id, remote.span_id());
        assert!(root.parent_span_is_remote);
        assert_eq!(receiver.parent_span_id, root.span_context.span_id());
        assert_eq!(receiver.span_context.trace_id(), remote.trace_id());
        assert!(!receiver.parent_span_is_remote);
        assert!(
            receiver
                .attributes
                .iter()
                .any(|a| a.key.as_str() == "detail" && a.value.as_str() == "preserved")
        );
    }

    #[crate::test]
    fn request_only_carrier_contains_no_span_or_text_context() {
        let request = Some(RequestContext {
            session_id: Uuid::from_u128(1),
            request_id: 2,
        });
        let context = request_context::in_scope(request, || {
            InProcessContext::obtain_for(QpsTracingMode::RequestOnly)
        });
        assert_eq!(context.request(), request);
        assert!(matches!(context.parent, Parent::Disabled));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(context.scope(async {
            assert_eq!(request_context::current(), request);
        }));
        assert_eq!(request_context::current(), None);
    }
}
