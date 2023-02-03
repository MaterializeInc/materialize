// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tracing utilities for explainable plans.

#![cfg(feature = "tracing_")]

use std::fmt::Debug;
use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::Mutex;

use tracing::{span, subscriber};
use tracing_subscriber::{field, layer};

/// A tracing layer used to accumulate a sequence of explainable plans.
#[allow(missing_debug_implementations)]
pub struct PlanTrace<T> {
    /// A specific concrete path to find in this trace. If present,
    /// [`PlanTrace::push`] will only collect traces if the current path is a
    /// prefix of find.
    find: Option<&'static str>,
    /// A path of segments identifying the spans in the current ancestor-or-self
    /// chain. The current path is used when accumulating new `entries`.
    path: Mutex<String>,
    /// A path of times at which the spans in the current ancestor-or-self chain
    /// were started. The duration since the last time is used when accumulating
    /// new `entries`.
    times: Mutex<Vec<std::time::Instant>>,
    /// A sequence of entries associating for a specific plan type `T`.
    entries: Mutex<Vec<TraceEntry<T>>>,
}

/// A struct created as a reflection of a [`trace_plan`] call.
#[allow(missing_debug_implementations)]
pub struct TraceEntry<T> {
    /// The instant at which an entry was created.
    ///
    /// Used to impose global sorting when merging multiple `TraceEntry`
    /// arrays in a single array.
    pub instant: std::time::Instant,
    /// The time it took to run this optimization step.
    pub duration: std::time::Duration,
    /// Ancestor chain of span names (root is first, parent is last).
    pub path: String,
    /// The plan produced this step.
    pub plan: T,
}

/// Trace a fragment of type `T` to be emitted as part of an `EXPLAIN OPTIMIZER
/// TRACE` output.
///
/// For best compatibility with the existing UI (which at the moment is the only
/// sane way to look at such `EXPLAIN` traces), code instrumentation should
/// adhere to the following constraints:
///
/// 1.  The plan type should be listed in the layers created in the
///     `OptimizerTrace` constructor.
/// 2.  Each `trace_plan` should be unique within it's enclosing span and should
///     represent the result of the stage idenified by that span. In particular,
///     this means that functions that call `trace_plan` more than once need to
///     construct ad-hoc spans (see the iteration spans in the `Fixpoint`
///     transform for example).
///
/// As a consequence of the second constraint, a sequence of paths such as
/// ```text
/// optimizer.foo.bar
/// optimizer.foo.baz
/// ```
/// is not well-formed as it is missing the results of the prefix paths at the
/// end:
/// ```text
/// optimizer.foo.bar
/// optimizer.foo.baz
/// optimizer.foo
/// optimizer
/// ```
///
/// Also, note that full paths can be repeated within a pipeline, but adjacent
/// duplicates are interpreted as separete invocations. For example, the
/// sub-sequence
/// ```text
/// ... // preceding stages
/// optimizer.foo.bar // 1st call
/// optimizer.foo.bar // 2nd call
/// ... // following stages
/// ```
/// will be rendered by the UI as the following tree structure.
/// ```text
/// optimizer
///   ... // following stages
///   foo
///     bar // 2nd call
///     bar // 1st call
///   ... // preceding stages
/// ```
pub fn trace_plan<T: Clone + 'static>(plan: &T) {
    tracing::Span::current().with_subscriber(|(_id, subscriber)| {
        if let Some(trace) = subscriber.downcast_ref::<PlanTrace<T>>() {
            trace.push(plan)
        }
    });
}

/// A [`layer::Layer`] implementation for [`PlanTrace`].
///
/// Populates the `data` wrapped by the [`PlanTrace`] instance with
/// [`TraceEntry`] values, one for each span with attached plan in its
/// extensions map.
impl<S, T> layer::Layer<S> for PlanTrace<T>
where
    S: subscriber::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
    T: 'static,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: layer::Context<'_, S>) {
        // add segment to path
        let mut path = self.path.lock().expect("path shouldn't be poisoned");
        let path = path.deref_mut();
        let segment = attrs
            .get_str("path.segment")
            .unwrap_or_else(|| ctx.span(id).expect("span").name().to_string());
        if !path.is_empty() {
            path.push('/');
        }
        path.push_str(segment.as_str());
    }

    fn on_enter(&self, _id: &span::Id, _ctx: layer::Context<'_, S>) {
        // push to time stack
        let mut times = self.times.lock().expect("times shouldn't be poisoned");
        times.deref_mut().push(std::time::Instant::now());
    }

    fn on_exit(&self, _id: &span::Id, _ctx: layer::Context<'_, S>) {
        // truncate last segment from path
        let mut path = self.path.lock().expect("path shouldn't be poisoned");
        let path = path.deref_mut();
        path.truncate(path.rfind('/').unwrap_or(path.len()));
        // pop from time stack
        let mut times = self.times.lock().expect("times shouldn't be poisoned");
        times.deref_mut().pop();
    }
}

impl<T: Clone + 'static> PlanTrace<T> {
    /// Create a new trace for plans of type `T`.
    pub fn new() -> Self {
        Self {
            find: None,
            path: Mutex::new(String::with_capacity(256)),
            times: Mutex::new(vec![]),
            entries: Mutex::new(vec![]),
        }
    }

    /// Create a new trace for plans of type `T` that will only accumulate
    /// [`TraceEntry`] instances along the prefix of the given `path`.
    pub fn find(path: &'static str) -> Self {
        Self {
            find: Some(path),
            path: Mutex::new(String::with_capacity(256)),
            times: Mutex::new(vec![]),
            entries: Mutex::new(vec![]),
        }
    }

    /// Drain the trace data collected so far.
    ///
    /// Note that this method will mutate the internal state of the enclosing
    /// [`PlanTrace`] even though its receiver is not `&mut self`. This quirk is
    /// required because the tracing `Dispatch` does not have `downcast_mut` method.
    pub fn drain_as_vec(&self) -> Vec<TraceEntry<T>>
    where
        T: Debug,
    {
        let mut entries = self.entries.lock().expect("entries shouldn't be poisoned");
        entries.split_off(0)
    }

    /// Push a trace entry for the given `plan` to the current trace.
    ///
    /// This is a noop if (1) the call is within a context without an enclosing
    /// span, or if (2) [`PlanTrace::find`] is set and the current path is not a
    /// prefix of its value.
    fn push(&self, plan: &T) {
        let times = self.times.lock().expect("times shouldn't be poisoned");
        if let Some(span_start) = times.last() {
            if let Some(current_path) = self.current_path() {
                let mut entries = self.entries.lock().expect("entries shouldn't be poisoned");
                let time = std::time::Instant::now();
                entries.push(TraceEntry {
                    instant: time,
                    duration: time.duration_since(*span_start),
                    path: current_path,
                    plan: plan.clone(),
                });
            }
        }
    }

    /// Helper method: get a copy of the current path.
    ///
    /// If [`PlanTrace::find`] is set, this will also check the current path
    /// against the `find` entry and return `None` if the former is not a prefix
    /// of the latter.
    fn current_path(&self) -> Option<String> {
        let path = self.path.lock().expect("path shouldn't be poisoned");
        let path = path.deref();
        match self.find {
            Some(find) => {
                if find.starts_with(path.as_str()) {
                    Some(path.clone())
                } else {
                    None
                }
            }
            None => Some(path.clone()),
        }
    }
}

/// Helper trait used to extract attributes of type `&'static str`.
trait GetStr {
    fn get_str(&self, key: &'static str) -> Option<String>;
}

impl<'a> GetStr for span::Attributes<'a> {
    fn get_str(&self, key: &'static str) -> Option<String> {
        let mut extract_str = ExtractStr::new(key);
        self.record(&mut extract_str);
        extract_str.val()
    }
}

/// Helper struct that implements `field::Visit` and is used in the
/// `GetStr::get_str` implementation for `span::Attributes`.
struct ExtractStr {
    key: &'static str,
    val: Option<String>,
}

impl ExtractStr {
    fn new(key: &'static str) -> Self {
        Self { key, val: None }
    }

    fn val(self) -> Option<String> {
        self.val
    }
}

impl field::Visit for ExtractStr {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == self.key {
            self.val = Some(value.to_string())
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

#[cfg(test)]
mod test {
    use tracing::dispatcher;
    use tracing::instrument;
    use tracing_subscriber::prelude::*;

    use super::trace_plan;
    use super::PlanTrace;

    #[test]
    fn test_optimizer_trace() {
        let subscriber = tracing_subscriber::registry().with(Some(PlanTrace::<String>::new()));
        let dispatch = dispatcher::Dispatch::new(subscriber);

        dispatcher::with_default(&dispatch, || {
            optimize();
        });

        if let Some(trace) = dispatch.downcast_ref::<PlanTrace<String>>() {
            let trace = trace.drain_as_vec();
            assert_eq!(trace.len(), 5);
            for (i, entry) in trace.into_iter().enumerate() {
                let path = entry.path;
                match i {
                    0 => {
                        assert_eq!(path, "optimize");
                    }
                    1 => {
                        assert_eq!(path, "optimize/logical/my_optimization");
                    }
                    2 => {
                        assert_eq!(path, "optimize/logical");
                    }
                    3 => {
                        assert_eq!(path, "optimize/physical");
                    }
                    4 => {
                        assert_eq!(path, "optimize");
                    }
                    _ => (),
                }
            }
        }
    }

    #[instrument(level = "info", skip_all)]
    fn optimize() {
        let mut plan = constant_plan(42);
        trace_plan(&plan);
        logical_optimizer(&mut plan);
        physical_optimizer(&mut plan);
        trace_plan(&plan);
    }

    #[instrument(level = "info", name = "logical", skip_all)]
    fn logical_optimizer(plan: &mut String) {
        some_optimization(plan);
        let _ = plan.replace("RawPlan", "LogicalPlan");
        trace_plan(plan);
    }

    #[instrument(level = "info", name = "physical", skip_all)]
    fn physical_optimizer(plan: &mut String) {
        let _ = plan.replace("LogicalPlan", "PhysicalPlan");
        trace_plan(plan);
    }

    #[tracing::instrument(level = "debug", skip_all, fields(path.segment ="my_optimization"))]
    fn some_optimization(plan: &mut String) {
        let _ = plan.replace("42", "47");
        trace_plan(plan);
    }

    fn constant_plan(i: usize) -> String {
        format!("RawPlan(#{})", i)
    }
}
