// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tracing utilities for explainable plans.

use std::fmt::{Debug, Display};
use std::sync::Mutex;

use mz_sql_parser::ast::NamedPlan;
use tracing::{span, subscriber, Level};
use tracing_core::{Interest, Metadata};
use tracing_subscriber::{field, layer};

use crate::explain::UsedIndexes;
use smallvec::SmallVec;

/// A tracing layer used to accumulate a sequence of explainable plans.
#[allow(missing_debug_implementations)]
pub struct PlanTrace<T> {
    /// A specific concrete path to find in this trace. If present,
    /// [`PlanTrace::push`] will only collect traces if the current path is a
    /// prefix of find.
    filter: Option<SmallVec<[NamedPlan; 4]>>,
    /// A path of segments identifying the spans in the current ancestor-or-self
    /// chain. The current path is used when accumulating new `entries`.
    path: Mutex<String>,
    /// The first time when entering a span (None no span was entered yet).
    start: Mutex<Option<std::time::Instant>>,
    /// A path of times at which the spans in the current ancestor-or-self chain
    /// were started. The duration since the last time is used when accumulating
    /// new `entries`.
    times: Mutex<Vec<std::time::Instant>>,
    /// A sequence of entries associating for a specific plan type `T`.
    entries: Mutex<Vec<TraceEntry<T>>>,
}

/// A struct created as a reflection of a [`trace_plan`] call.
#[derive(Clone, Debug)]
pub struct TraceEntry<T> {
    /// The instant at which an entry was created.
    ///
    /// Used to impose global sorting when merging multiple `TraceEntry`
    /// arrays in a single array.
    pub instant: std::time::Instant,
    /// The duration since the start of the enclosing span.
    pub span_duration: std::time::Duration,
    /// The duration since the start of the top-level span seen by the `PlanTrace`.
    pub full_duration: std::time::Duration,
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

/// Create a span identified by `segment` and trace `plan` in it.
///
/// This primitive is useful for instrumentic code, see this commit[^example]
/// for an example.
///
/// [^example]: <https://github.com/MaterializeInc/materialize/commit/2ce93229>
pub fn dbg_plan<S: Display, T: Clone + 'static>(segment: S, plan: &T) {
    span!(target: "optimizer", Level::DEBUG, "segment", path.segment = %segment).in_scope(|| {
        trace_plan(plan);
    });
}

/// Create a span identified by `segment` and trace `misc` in it.
///
/// This primitive is useful for instrumentic code, see this commit[^example]
/// for an example.
///
/// [^example]: <https://github.com/MaterializeInc/materialize/commit/2ce93229>
pub fn dbg_misc<S: Display, T: Display>(segment: S, misc: T) {
    span!(target: "optimizer", Level::DEBUG, "segment", path.segment = %segment).in_scope(|| {
        trace_plan(&misc.to_string());
    });
}

/// A helper struct for wrapping entries that represent the invocation context
/// of a function or method call into an object that renders as their hash.
///
/// Useful when constructing path segments when instrumenting a function trace
/// with additional debugging information.
#[allow(missing_debug_implementations)]
pub struct ContextHash(u64);

impl ContextHash {
    pub fn of<T: std::hash::Hash>(t: T) -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let mut h = DefaultHasher::new();
        t.hash(&mut h);
        ContextHash(h.finish())
    }
}

impl Display for ContextHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}", self.0 & 0xFFFFFFFu64) // show last 28 bits
    }
}

/// A [`layer::Layer`] implementation for [`PlanTrace`].
///
/// Populates the `data` wrapped by the [`PlanTrace`] instance with
/// [`TraceEntry`] values, one for each span with attached plan in its
/// extensions map.
impl<S, T> layer::Layer<S> for PlanTrace<T>
where
    S: subscriber::Subscriber,
    T: 'static,
{
    fn on_new_span(
        &self,
        attrs: &span::Attributes<'_>,
        _id: &span::Id,
        _ctx: layer::Context<'_, S>,
    ) {
        // add segment to path
        let mut path = self.path.lock().expect("path shouldn't be poisoned");
        let segment = attrs.get_str("path.segment");
        let segment = segment.unwrap_or_else(|| attrs.metadata().name().to_string());
        if !path.is_empty() {
            path.push('/');
        }
        path.push_str(segment.as_str());
    }

    fn on_enter(&self, _id: &span::Id, _ctx: layer::Context<'_, S>) {
        let now = std::time::Instant::now();
        // set start value on first ever on_enter
        let mut start = self.start.lock().expect("start shouldn't be poisoned");
        start.get_or_insert(now);
        // push to time stack
        let mut times = self.times.lock().expect("times shouldn't be poisoned");
        times.push(now);
    }

    fn on_exit(&self, _id: &span::Id, _ctx: layer::Context<'_, S>) {
        // truncate last segment from path
        let mut path = self.path.lock().expect("path shouldn't be poisoned");
        let new_len = path.rfind('/').unwrap_or(0);
        path.truncate(new_len);
        // pop from time stack
        let mut times = self.times.lock().expect("times shouldn't be poisoned");
        times.pop();
    }
}

impl<S, T> layer::Filter<S> for PlanTrace<T>
where
    S: subscriber::Subscriber,
    T: 'static + Clone,
{
    fn enabled(&self, meta: &Metadata<'_>, _cx: &layer::Context<'_, S>) -> bool {
        self.is_enabled(meta)
    }

    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> Interest {
        if self.is_enabled(meta) {
            Interest::always()
        } else {
            Interest::never()
        }
    }
}

impl<T: 'static + Clone> PlanTrace<T> {
    /// Create a new trace for plans of type `T` that will only accumulate
    /// [`TraceEntry`] instances along the prefix of the given `path`.
    pub fn new(filter: Option<SmallVec<[NamedPlan; 4]>>) -> Self {
        Self {
            filter,
            path: Mutex::new(String::with_capacity(256)),
            start: Mutex::new(None),
            times: Mutex::new(Default::default()),
            entries: Mutex::new(Default::default()),
        }
    }

    /// Check if a subscriber layer of this kind will be interested in tracing
    /// spans and events with the given metadata.
    fn is_enabled(&self, meta: &Metadata<'_>) -> bool {
        meta.is_span() && meta.target() == "optimizer"
    }

    /// Drain the trace data collected so far.
    ///
    /// Note that this method will mutate the internal state of the enclosing
    /// [`PlanTrace`] even though its receiver is not `&mut self`. This quirk is
    /// required because the tracing `Dispatch` does not have `downcast_mut` method.
    pub fn drain_as_vec(&self) -> Vec<TraceEntry<T>> {
        let mut entries = self.entries.lock().expect("entries shouldn't be poisoned");
        entries.split_off(0)
    }

    /// Retrieve the trace data collected so far while leaving it in place.
    pub fn collect_as_vec(&self) -> Vec<TraceEntry<T>> {
        let entries = self.entries.lock().expect("entries shouldn't be poisoned");
        (*entries).clone()
    }

    /// Find and return a clone of the [`TraceEntry`] for the given `path`.
    pub fn find(&self, path: &str) -> Option<TraceEntry<T>>
    where
        T: Clone,
    {
        let entries = self.entries.lock().expect("entries shouldn't be poisoned");
        entries.iter().find(|entry| entry.path == path).cloned()
    }

    /// Push a trace entry for the given `plan` to the current trace.
    ///
    /// This is a noop if
    /// 1. the call is within a context without an enclosing span, or if
    /// 2. [`PlanTrace::filter`] is set not equal to [`PlanTrace::current_path`].
    fn push(&self, plan: &T)
    where
        T: Clone,
    {
        if let Some(current_path) = self.current_path() {
            let times = self.times.lock().expect("times shouldn't be poisoned");
            let start = self.start.lock().expect("start shouldn't is poisoned");
            if let (Some(full_start), Some(span_start)) = (start.as_ref(), times.last()) {
                let mut entries = self.entries.lock().expect("entries shouldn't be poisoned");
                let time = std::time::Instant::now();
                entries.push(TraceEntry {
                    instant: time,
                    span_duration: time.duration_since(*span_start),
                    full_duration: time.duration_since(*full_start),
                    path: current_path,
                    plan: plan.clone(),
                });
            }
        }
    }

    /// Helper method: get a copy of the current path.
    ///
    /// If [`PlanTrace::filter`] is set, this will also check the current path
    /// against the `find` entry and return `None` if the two differ.
    fn current_path(&self) -> Option<String> {
        let path = self.path.lock().expect("path shouldn't be poisoned");
        let path = path.as_str();
        match self.filter.as_ref() {
            Some(named_paths) => {
                if named_paths.iter().any(|named| path == named.path()) {
                    Some(path.to_owned())
                } else {
                    None
                }
            }
            None => Some(path.to_owned()),
        }
    }
}

impl PlanTrace<UsedIndexes> {
    /// Get the [`UsedIndexes`] corresponding to the given `plan_path`.
    ///
    /// Note that the path under which a `UsedIndexes` entry is traced might
    /// differ from the path of the `plan_path` of the plan that needs it.
    pub fn used_indexes_for(&self, plan_path: &str) -> UsedIndexes {
        // Compute the path from which we are going to lookup the `UsedIndexes`
        // instance from the requested path.
        let path = match NamedPlan::of_path(plan_path) {
            Some(NamedPlan::Global) => Some(NamedPlan::Global),
            Some(NamedPlan::Physical) => Some(NamedPlan::Global),
            Some(NamedPlan::FastPath) => Some(NamedPlan::FastPath),
            _ => None,
        };
        // Find the `TraceEntry` wrapping the `UsedIndexes` instance.
        let entry = match path {
            Some(path) => self.find(path.path()),
            None => None,
        };
        // Either return the `UsedIndexes` wrapped by the found entry or a
        // default `UsedIndexes` instance if such entry was not found.
        entry.map_or(Default::default(), |e| e.plan)
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

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == self.key {
            self.val = Some(format!("{value:?}"))
        }
    }
}

#[cfg(test)]
mod test {
    use mz_ore::instrument;
    use tracing::dispatcher;
    use tracing_subscriber::prelude::*;

    use super::{trace_plan, PlanTrace};

    #[mz_ore::test]
    fn test_optimizer_trace() {
        let subscriber = tracing_subscriber::registry().with(Some(PlanTrace::<String>::new(None)));
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

    #[instrument(level = "info")]
    fn optimize() {
        let mut plan = constant_plan(42);
        trace_plan(&plan);
        logical_optimizer(&mut plan);
        physical_optimizer(&mut plan);
        trace_plan(&plan);
    }

    #[instrument(level = "info", name = "logical")]
    fn logical_optimizer(plan: &mut String) {
        some_optimization(plan);
        let _ = plan.replace("RawPlan", "LogicalPlan");
        trace_plan(plan);
    }

    #[instrument(level = "info", name = "physical")]
    fn physical_optimizer(plan: &mut String) {
        let _ = plan.replace("LogicalPlan", "PhysicalPlan");
        trace_plan(plan);
    }

    #[mz_ore::instrument(level = "debug", fields(path.segment ="my_optimization"))]
    fn some_optimization(plan: &mut String) {
        let _ = plan.replace("42", "47");
        trace_plan(plan);
    }

    fn constant_plan(i: usize) -> String {
        format!("RawPlan(#{})", i)
    }
}
