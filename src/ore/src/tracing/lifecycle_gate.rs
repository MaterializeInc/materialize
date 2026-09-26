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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::subscriber::Interest;
use tracing::{Metadata, Subscriber, span};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::{Context, Filter};
use tracing_subscriber::{EnvFilter, reload};

pub(super) struct LifecycleGate<S> {
    inner: reload::Layer<EnvFilter, S>,
    dynamic_ever: Arc<AtomicBool>,
}

pub(super) struct Handle<S> {
    inner: reload::Handle<EnvFilter, S>,
    dynamic_ever: Arc<AtomicBool>,
}

pub(super) fn needs_lifecycle(filter: &EnvFilter) -> bool {
    // Canonical span/field directives contain brackets. False positives use the
    // original implementation. Targets parsing cannot distinguish these cases.
    filter.to_string().contains('[')
}

pub(super) fn gated<S>(filter: EnvFilter) -> (LifecycleGate<S>, Handle<S>) {
    let dynamic_ever = Arc::new(AtomicBool::new(needs_lifecycle(&filter)));
    let (inner, handle) = reload::Layer::new(filter);
    (
        LifecycleGate {
            inner,
            dynamic_ever: Arc::clone(&dynamic_ever),
        },
        Handle {
            inner: handle,
            dynamic_ever,
        },
    )
}

impl<S> Handle<S> {
    pub(super) fn reload(&self, filter: EnvFilter) -> Result<(), reload::Error> {
        if needs_lifecycle(&filter) {
            // Publish before installing the filter. Never clear the gate: old
            // spans may still require callbacks after a later static reload.
            self.dynamic_ever.store(true, Ordering::SeqCst);
        }
        self.inner.reload(filter)
    }
}

impl<S: Subscriber + 'static> Filter<S> for LifecycleGate<S> {
    fn enabled(&self, metadata: &Metadata<'_>, ctx: &Context<'_, S>) -> bool {
        Filter::enabled(&self.inner, metadata, ctx)
    }

    fn callsite_enabled(&self, metadata: &'static Metadata<'static>) -> Interest {
        Filter::callsite_enabled(&self.inner, metadata)
    }

    fn max_level_hint(&self) -> Option<LevelFilter> {
        Filter::max_level_hint(&self.inner)
    }

    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        if self.dynamic_ever.load(Ordering::SeqCst) {
            Filter::on_new_span(&self.inner, attrs, id, ctx);
        }
    }

    fn on_record(&self, id: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        if self.dynamic_ever.load(Ordering::SeqCst) {
            Filter::on_record(&self.inner, id, values, ctx);
        }
    }

    fn on_enter(&self, id: &span::Id, ctx: Context<'_, S>) {
        if self.dynamic_ever.load(Ordering::SeqCst) {
            Filter::on_enter(&self.inner, id, ctx);
        }
    }

    fn on_exit(&self, id: &span::Id, ctx: Context<'_, S>) {
        if self.dynamic_ever.load(Ordering::SeqCst) {
            Filter::on_exit(&self.inner, id, ctx);
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        if self.dynamic_ever.load(Ordering::SeqCst) {
            Filter::on_close(&self.inner, id, ctx);
        }
    }
}

pub(super) struct EventsOnly;

impl<S: Subscriber> Filter<S> for EventsOnly {
    fn enabled(&self, metadata: &Metadata<'_>, _: &Context<'_, S>) -> bool {
        metadata.is_event()
    }

    fn callsite_enabled(&self, metadata: &'static Metadata<'static>) -> Interest {
        if metadata.is_event() {
            Interest::always()
        } else {
            Interest::never()
        }
    }
}
