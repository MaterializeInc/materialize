// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Observability for the reconciliation loop, shared by every controller in
//! this process.
//!
//! [`Observed`] wraps a controller's [`k8s_controller::Context`] so that each
//! reconciliation pass is timed and counted, and so that a failed pass is
//! published as a Kubernetes warning event on the resource being reconciled.
//! The event is what makes a stuck object explain itself: `kubectl describe`
//! shows why reconciliation is not progressing to someone who cannot read the
//! operator's logs.
//!
//! Reconcilers additionally report their own [steps](Metrics::step). A step is
//! the only level at which "there was nothing to do" is distinguishable from
//! "the desired state was reached", since the reconciler interface reports both
//! as success.

use std::sync::Arc;
use std::time::Instant;

use k8s_controller::TraceMetadata;
use kube::runtime::controller::Action;
use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::{Client, Resource};
use tracing::warn;

use mz_ore::error::ErrorExt;
use mz_ore::metric;
use mz_ore::metrics::{
    MetricsRegistry,
    raw::{HistogramVec, IntCounterVec},
};
use mz_ore::stats::histogram_seconds_buckets;

/// The `reportingController` that events published by this process carry.
const EVENT_REPORTER: &str = "orchestratord.materialize.cloud";

/// The `reason` of the event published when a reconciliation fails.
const RECONCILIATION_FAILED: &str = "ReconciliationFailed";

/// The Kubernetes API server rejects an event whose note exceeds 1kB, so a
/// long error is truncated to fit rather than costing us the event entirely.
const MAX_NOTE_BYTES: usize = 1024;

/// What a reconciliation pass, or one step of one, did.
///
/// The variants are the label values of the `outcome` label, so a call site
/// picking between them decides what a dashboard reports.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    /// Brought the resources it manages to the desired state, or found them
    /// already there.
    Applied,
    /// Made progress, but the desired state is not reached yet, so
    /// reconciliation must run again. A rollout spends most of its passes here
    /// while it waits for the new generation's pods to become ready.
    Waiting,
    /// Had nothing to act on: what it manages is disabled by configuration, or
    /// absent from the resource's spec.
    Skipped,
    /// Returned an error, observed as one.
    Failed,
    /// Did not reach a conclusion. Either an error propagated out of it, or the
    /// reconciliation running it was cancelled: this process aborts in-flight
    /// reconciliations when it loses the leadership lease, and again on
    /// shutdown.
    ///
    /// A [`Step`] dropped without being finished records this, because a `Drop`
    /// cannot tell those two apart. It is therefore not a failure signal:
    /// `orchestratord_reconciliations_total` is, since it is recorded from the
    /// reconciler's actual `Result` and a cancelled pass never reaches it. What
    /// this answers is which step a pass was in when it stopped.
    Abandoned,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Outcome::Applied => "applied",
            Outcome::Waiting => "waiting",
            Outcome::Skipped => "skipped",
            Outcome::Failed => "failed",
            Outcome::Abandoned => "abandoned",
        }
    }

    /// Classifies what a reconciler returned. `Ok(None)` means the reconciler
    /// is done with this resource until something changes, and `Ok(Some(_))`
    /// means it asked to be run again.
    fn of_result<E>(result: &Result<Option<Action>, E>) -> Self {
        match result {
            Ok(None) => Outcome::Applied,
            Ok(Some(_)) => Outcome::Waiting,
            Err(_) => Outcome::Failed,
        }
    }
}

/// Which of a [`k8s_controller::Context`]'s two entry points ran, as the
/// `event_type` label.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EntryPoint {
    Apply,
    Cleanup,
}

impl EntryPoint {
    fn as_str(self) -> &'static str {
        match self {
            EntryPoint::Apply => "apply",
            EntryPoint::Cleanup => "cleanup",
        }
    }

    /// The event `action`, which names what was being attempted rather than
    /// its outcome.
    fn action(self) -> &'static str {
        match self {
            EntryPoint::Apply => "Reconcile",
            EntryPoint::Cleanup => "Cleanup",
        }
    }
}

/// Metrics covering the reconciliation loop of every controller.
#[derive(Debug)]
pub struct Metrics {
    reconciliations: IntCounterVec,
    reconciliation_duration_seconds: HistogramVec,
    steps: IntCounterVec,
    step_duration_seconds: HistogramVec,
}

impl Metrics {
    pub fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            reconciliations: registry.register(metric! {
                name: "orchestratord_reconciliations_total",
                help: "Count of reconciliation passes, by controller, by which of the controller's entry points ran, and by what the pass concluded. An `outcome` of `waiting` means the pass asked to be retried, which is normal during a rollout; `failed` means it returned an error, and is also reported as a Kubernetes event on the resource.",
                var_labels: ["controller", "event_type", "outcome"],
            }),
            reconciliation_duration_seconds: registry.register(metric! {
                name: "orchestratord_reconciliation_duration_seconds",
                help: "Time spent in one reconciliation pass. This covers only the reconciler itself, not the finalizer bookkeeping around it, and a pass that waits on a rollout returns promptly rather than blocking, so this measures work done and not the wall-clock length of a rollout.",
                var_labels: ["controller", "event_type"],
                buckets: histogram_seconds_buckets(0.001, 512.0),
            }),
            steps: registry.register(metric! {
                name: "orchestratord_reconciliation_steps_total",
                help: "Count of reconciliation steps, by controller, by step, and by what the step concluded. Steps are the named phases a reconciliation pass moves through, so this is where a pass that fails or stalls identifies which phase it was in. An `outcome` of `abandoned` means the step did not conclude, which covers both an error propagating out of it and the pass being cancelled by a leadership handoff, so alert on orchestratord_reconciliations_total instead and read this to locate the step.",
                var_labels: ["controller", "step", "outcome"],
            }),
            step_duration_seconds: registry.register(metric! {
                name: "orchestratord_reconciliation_step_duration_seconds",
                help: "Time spent in one reconciliation step.",
                var_labels: ["controller", "step"],
                buckets: histogram_seconds_buckets(0.001, 512.0),
            }),
        }
    }

    fn record_reconciliation(
        &self,
        controller: &str,
        entry_point: EntryPoint,
        outcome: Outcome,
        duration: f64,
    ) {
        self.reconciliations
            .with_label_values(&[controller, entry_point.as_str(), outcome.as_str()])
            .inc();
        self.reconciliation_duration_seconds
            .with_label_values(&[controller, entry_point.as_str()])
            .observe(duration);
    }

    /// Starts timing the step named `step` of `controller`'s reconciliation.
    ///
    /// The step is recorded when the returned [`Step`] is finished or dropped.
    pub fn step<'a>(&'a self, controller: &'static str, step: &'static str) -> Step<'a> {
        Step {
            metrics: self,
            controller,
            step,
            outcome: None,
            start: Instant::now(),
        }
    }
}

/// Times one named step of a reconciliation, recording its duration and
/// outcome once dropped.
///
/// A step dropped without [`Step::finish`] records [`Outcome::Abandoned`], so
/// the `?` that leaves a step reports where the pass stopped with no
/// bookkeeping at the point of the early return. The contract that buys is
/// that every path out of a step's scope which reaches a conclusion must
/// finish the step explicitly.
#[must_use = "a step that is never finished records itself as abandoned"]
pub struct Step<'a> {
    metrics: &'a Metrics,
    controller: &'static str,
    step: &'static str,
    outcome: Option<Outcome>,
    start: Instant,
}

impl Step<'_> {
    /// Records the step as having concluded with `outcome`.
    pub fn finish(mut self, outcome: Outcome) {
        self.outcome = Some(outcome);
    }

    /// Records the step as having concluded with what a reconciler returned:
    /// [`Outcome::Waiting`] if it asked to be run again, [`Outcome::Applied`]
    /// if it is done, and [`Outcome::Failed`] if it errored.
    ///
    /// A step whose caller holds the `Result` should prefer this over letting
    /// the error propagate past the guard, since observing the error is what
    /// separates a failure from an [`Outcome::Abandoned`] pass.
    pub fn finish_with<E>(self, result: &Result<Option<Action>, E>) {
        self.finish(Outcome::of_result(result));
    }
}

impl Drop for Step<'_> {
    fn drop(&mut self) {
        let outcome = self.outcome.unwrap_or(Outcome::Abandoned);
        self.metrics
            .steps
            .with_label_values(&[self.controller, self.step, outcome.as_str()])
            .inc();
        self.metrics
            .step_duration_seconds
            .with_label_values(&[self.controller, self.step])
            .observe(self.start.elapsed().as_secs_f64());
    }
}

/// Wraps a controller [`Context`](k8s_controller::Context) so that every
/// reconciliation pass it runs is measured, and so that a failed pass is
/// published as a Kubernetes warning event on the resource it was reconciling.
///
/// This decorates the context rather than sitting beside the controller
/// because the reconciler's error and the object it was reconciling are only
/// both in hand here.
///
/// NOTE: failures of the finalizer bookkeeping the controller performs around
/// the reconciler, as opposed to failures of the reconciler itself, are not
/// covered. They never reach these methods.
pub struct Observed<Ctx> {
    inner: Ctx,
    controller: &'static str,
    metrics: Arc<Metrics>,
    recorder: Recorder,
}

impl<Ctx> Observed<Ctx> {
    /// Wraps `inner`, labelling its metrics and events with `controller`.
    ///
    /// `controller` must match the name the reconciler passes to
    /// [`Metrics::step`], otherwise a pass and its steps land under different
    /// labels.
    pub fn new(
        inner: Ctx,
        controller: &'static str,
        metrics: Arc<Metrics>,
        recorder: Recorder,
    ) -> Self {
        Self {
            inner,
            controller,
            metrics,
            recorder,
        }
    }
}

impl<Ctx> Observed<Ctx>
where
    Ctx: k8s_controller::Context,
    <Ctx::Resource as Resource>::DynamicType: Default,
{
    /// Publishes a warning event describing `error` on `resource`.
    ///
    /// Failing to publish is only logged: the caller is already returning the
    /// reconciliation's own error, and replacing it with this one would hide
    /// the problem the event was meant to report. A missing `events.k8s.io`
    /// permission therefore costs visibility, not reconciliation.
    async fn publish_failure(
        &self,
        entry_point: EntryPoint,
        resource: &Ctx::Resource,
        error: &Ctx::Error,
    ) {
        let event = Event {
            type_: EventType::Warning,
            reason: RECONCILIATION_FAILED.into(),
            action: entry_point.action().into(),
            // The cause chain, not just the outermost message: an error like
            // "invalid environment id in license key" is only actionable
            // alongside what it was that failed to parse.
            note: Some(truncate_note(&error.to_string_with_causes())),
            secondary: None,
        };
        if let Err(e) = self
            .recorder
            .publish(&event, &resource.object_ref(&Default::default()))
            .await
        {
            warn!(
                error = %e,
                controller = self.controller,
                "failed to publish reconciliation failure event",
            );
        }
    }

    /// Records what one reconciliation pass did, and reports a failure on the
    /// resource itself.
    async fn record(
        &self,
        entry_point: EntryPoint,
        resource: &Ctx::Resource,
        result: &Result<Option<Action>, Ctx::Error>,
        start: Instant,
    ) {
        self.metrics.record_reconciliation(
            self.controller,
            entry_point,
            Outcome::of_result(result),
            start.elapsed().as_secs_f64(),
        );
        if let Err(error) = result {
            self.publish_failure(entry_point, resource, error).await;
        }
    }
}

#[async_trait::async_trait]
impl<Ctx> k8s_controller::Context for Observed<Ctx>
where
    Ctx: k8s_controller::Context + Send + Sync + 'static,
    Ctx::Resource: Send + Sync,
    Ctx::Error: Send + Sync,
    <Ctx::Resource as Resource>::DynamicType: Default,
{
    type Resource = Ctx::Resource;
    type Error = Ctx::Error;

    const FINALIZER_NAME: Option<&'static str> = Ctx::FINALIZER_NAME;

    async fn apply(
        &self,
        client: Client,
        resource: &Self::Resource,
        metadata: &mut TraceMetadata,
    ) -> Result<Option<Action>, Self::Error> {
        let start = Instant::now();
        let result = self.inner.apply(client, resource, metadata).await;
        self.record(EntryPoint::Apply, resource, &result, start)
            .await;
        result
    }

    async fn cleanup(
        &self,
        client: Client,
        resource: &Self::Resource,
        metadata: &mut TraceMetadata,
    ) -> Result<Option<Action>, Self::Error> {
        let start = Instant::now();
        let result = self.inner.cleanup(client, resource, metadata).await;
        self.record(EntryPoint::Cleanup, resource, &result, start)
            .await;
        result
    }

    fn success_action(&self, resource: &Self::Resource) -> Action {
        self.inner.success_action(resource)
    }

    // NOTE: `error_action` is deliberately not delegated. Its signature names
    // `k8s_controller`'s error type, which that crate does not export, so no
    // context outside the crate can override it. Every context, wrapped or
    // not, gets the crate's default backoff.
}

/// Builds the event recorder shared by every controller in this process.
///
/// One recorder is shared so that its deduplication cache is too: a
/// reconciliation that keeps failing collapses into a single event with a
/// count, rather than one event per attempt.
///
/// `instance` disambiguates which replica published an event, and should be the
/// pod name.
pub fn event_recorder(client: Client, instance: String) -> Recorder {
    Recorder::new(
        client,
        Reporter {
            controller: EVENT_REPORTER.to_owned(),
            instance: Some(instance),
        },
    )
}

/// Shortens `note` to fit an event's note, on a character boundary.
fn truncate_note(note: &str) -> String {
    if note.len() <= MAX_NOTE_BYTES {
        return note.to_owned();
    }
    const ELLIPSIS: &str = "...";
    let mut end = MAX_NOTE_BYTES - ELLIPSIS.len();
    while !note.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{ELLIPSIS}", &note[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_truncate_note() {
        assert_eq!(truncate_note("short"), "short");

        let exact = "a".repeat(MAX_NOTE_BYTES);
        assert_eq!(truncate_note(&exact), exact);

        let long = "a".repeat(MAX_NOTE_BYTES + 1);
        let truncated = truncate_note(&long);
        assert_eq!(truncated.len(), MAX_NOTE_BYTES);
        assert!(truncated.ends_with("..."));

        // A multi-byte character straddling the cut must not be split.
        let multibyte = "é".repeat(MAX_NOTE_BYTES);
        let truncated = truncate_note(&multibyte);
        assert!(truncated.len() <= MAX_NOTE_BYTES);
        assert!(truncated.ends_with("..."));
    }
}
