// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the inline index-peek driver, and the fixtures a peek scan is tested over.

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::{Batcher, Builder, Trace};
use mz_expr::RowSetFinishing;
use mz_ore::cast::CastFrom;
use mz_repr::{Datum, Diff, RelationDesc, SqlScalarType};
use mz_row_spine::{RowRowBatcher, RowRowBuilder, RowRowSpine};
use mz_timely_util::columnation::ColumnationStack;
use timely::container::PushInto;
use timely::dataflow::operators::generic::OperatorInfo;

use crate::metrics::ComputeMetrics;
use crate::server::ComputeRuntimeRole;
use crate::typedefs::{ErrAgent, ErrSpine, RowRowAgent};

use super::error_scan::tests::{
    ErrorUpdates, PEEK_TIMESTAMP, cancelling, error, error_batch, holding,
};
use super::*;
use crate::arrangement::manager::TraceBundle;

/// The collection the peeks in these tests read.
pub(crate) const TARGET_ID: GlobalId = GlobalId::User(1);

/// A peek that may not use the stash.
const NO_STASH: StashBounds = StashBounds {
    eligible: false,
    threshold_bytes: usize::MAX,
    batch_bytes: 0,
};

/// A peek whose every row is bound for the stash.
const STASH_EVERYTHING: StashBounds = StashBounds {
    eligible: true,
    threshold_bytes: 0,
    batch_bytes: 0,
};

/// A trace agent over a trace holding exactly `batch`.
///
/// The writer is dropped here, which seals the trace to the empty frontier. That is what lets
/// `TraceReader::cursor` hand out a cursor covering every batch, which is the only way a peek
/// reads a trace.
fn agent<Tr>(batch: Tr::Batch) -> TraceAgent<Tr>
where
    Tr: Trace<Time = Timestamp> + 'static,
{
    let info = OperatorInfo::new(0, 0, [].into());
    let (agent, mut writer) = TraceAgent::new(Tr::new(info.clone(), None, None), info, None);
    writer.insert(batch, Some(Timestamp::MIN));
    agent
}

/// A row holding `value` as its only datum, as the ok trace's keys hold it.
///
/// The datum type matches the column [`index_peek`] declares, because that description is the
/// schema a peek taken to the stash writes its rows under.
pub(crate) fn ok_row(value: u64) -> Row {
    Row::pack_slice(&[Datum::UInt64(value)])
}

/// A finishing that asks for every row, in the order the walk produced them.
pub(crate) fn trivial_finishing() -> RowSetFinishing {
    RowSetFinishing::trivial(1)
}

/// The ok trace of an index holding one update per key in `keys`, all at [`Timestamp::MIN`] so
/// that every one of them is visible at [`PEEK_TIMESTAMP`]. The values are empty.
fn oks_trace(keys: &[Row]) -> RowRowAgent<Timestamp, Diff> {
    let mut chunk = ColumnationStack::with_capacity(keys.len());
    for key in keys {
        chunk.push_into(((key.clone(), Row::default()), Timestamp::MIN, Diff::ONE));
    }
    let mut batcher = RowRowBatcher::<Timestamp, Diff>::new(None, 0);
    batcher.push_into(chunk);
    let (mut chain, description) = batcher.seal(Antichain::from_elem(Timestamp::MAX));
    let batch = RowRowBuilder::<Timestamp, Diff>::seal(&mut chain, description);
    agent::<RowRowSpine<Timestamp, Diff>>(batch)
}

/// The error trace of an index holding `updates`.
fn errs_trace(updates: ErrorUpdates) -> ErrAgent<Timestamp, Diff> {
    agent::<ErrSpine<Timestamp, Diff>>(error_batch(updates))
}

/// The traces of an index whose ok side holds `keys` and whose error side holds `errors`.
pub(crate) fn trace_bundle(keys: &[Row], errors: ErrorUpdates) -> TraceBundle {
    TraceBundle::new(oks_trace(keys), errs_trace(errors))
}

/// Errors that a walk over them examines one by one and finds none of, because each cancels to
/// zero at [`PEEK_TIMESTAMP`].
pub(crate) fn cancelling_errors(count: usize) -> ErrorUpdates {
    let errors: Vec<_> = (0..count).map(error).collect();
    errors.iter().flat_map(cancelling).collect()
}

/// `count` errors that cancel to zero at [`PEEK_TIMESTAMP`] plus one that does not, and the
/// answer a walk that reaches the latter gives.
///
/// The serialized order of an error does not follow its index, so the errors are sorted and
/// the answering one taken from the end. A walk that visits keys in order therefore reaches it
/// last, having examined all `count` of the others.
pub(crate) fn answering_errors(count: usize) -> (ErrorUpdates, PeekError) {
    let mut errors: Vec<_> = (0..count + 1).map(error).collect();
    errors.sort();
    let (answering, cancelled) = errors.split_last().expect("non-empty");
    let mut updates: ErrorUpdates = cancelled.iter().flat_map(cancelling).collect();
    updates.extend(holding(answering));
    (updates, PeekError::from(answering.deserialize()))
}

/// A peek of [`TARGET_ID`] at [`PEEK_TIMESTAMP`].
///
/// The projection drops every column but the first, so a peek with a literal constraint and
/// one without return rows of the same shape: the cursor's key is one datum, the values are
/// empty, and a literal constraint contributes a second datum that a join in a dataflow would
/// have added.
///
/// The result description carries that one column, because its arity is what decides whether
/// the peek's finishing streams, and a finishing that streams is what makes a peek eligible
/// for the peek stash.
pub(crate) fn index_peek(
    finishing: RowSetFinishing,
    literal_constraints: Option<Vec<Row>>,
) -> Peek {
    let arity = if literal_constraints.is_some() { 2 } else { 1 };
    let map_filter_project = mz_expr::MapFilterProject::new(arity)
        .project([0])
        .into_plan()
        .expect("valid plan")
        .into_nontemporal()
        .expect("non-temporal plan");
    let result_desc = RelationDesc::builder()
        .with_column("value", SqlScalarType::UInt64.nullable(false))
        .finish();
    Peek {
        target: PeekTarget::Index { id: TARGET_ID },
        result_desc,
        literal_constraints,
        uuid: Uuid::nil(),
        timestamp: PEEK_TIMESTAMP,
        finishing,
        map_filter_project,
        otel_ctx: OpenTelemetryContext::empty(),
    }
}

/// A peek of [`TARGET_ID`] carrying `uuid`, so that a test with several pending peeks can say
/// which one a response answered.
pub(crate) fn index_peek_with_uuid(uuid: Uuid, literal_constraints: Option<Vec<Row>>) -> Peek {
    let mut peek = index_peek(trivial_finishing(), literal_constraints);
    peek.uuid = uuid;
    peek
}

/// The keys of an index holding `count` distinct rows, in the order the ok walk visits them.
///
/// Sorted here rather than assumed, because the trace holds its keys in [`Row`] order and the
/// answer a full walk gives is that order. Used where an index needs more positions than the
/// production inline budget lets a peek walk on the worker.
pub(crate) fn wide_ok_rows(count: u64) -> Vec<Row> {
    let mut keys: Vec<Row> = (0..count).map(ok_row).collect();
    keys.sort();
    keys
}

/// The metrics an index peek observes into, registered into a registry the test owns so it
/// can read them back.
struct TestMetrics {
    metrics: WorkerMetrics,
    walk: PeekWalkMetrics,
}

impl TestMetrics {
    fn new() -> Self {
        let metrics =
            ComputeMetrics::register_with(&MetricsRegistry::new(), ComputeRuntimeRole::Solo)
                .for_worker(0);
        let walk = PeekWalkMetrics::new(&metrics);
        Self { metrics, walk }
    }

    fn as_metrics(&self) -> IndexPeekMetrics<'_> {
        IndexPeekMetrics {
            seek_fulfillment_seconds: &self.metrics.index_peek_seek_fulfillment_seconds,
            frontier_check_seconds: &self.metrics.index_peek_frontier_check_seconds,
            walk: &self.walk,
        }
    }

    /// How often each metric that a walk can observe into was observed.
    ///
    /// The two histograms the enclosing `seek_fulfillment` owns are left out, because the
    /// tests that read this call [`collect`] directly.
    fn observations(&self) -> BTreeMap<&'static str, u64> {
        let metrics = &self.metrics;
        BTreeMap::from([
            ("walks_inline", metrics.index_peek_walks_inline.get()),
            ("walks_offloaded", metrics.index_peek_walks_offloaded.get()),
            ("walks_stashed", metrics.index_peek_stashed_total.get()),
            (
                "error_scan_seconds",
                metrics.index_peek_error_scan_seconds.get_sample_count(),
            ),
            (
                "cursor_setup_seconds",
                metrics.index_peek_cursor_setup_seconds.get_sample_count(),
            ),
            (
                "row_iteration_seconds",
                metrics.index_peek_row_iteration_seconds.get_sample_count(),
            ),
            (
                "row_iteration_rows",
                metrics.index_peek_row_iteration_rows.get_sample_count(),
            ),
            (
                "result_sort_seconds",
                metrics.index_peek_result_sort_seconds.get_sample_count(),
            ),
            (
                "result_sort_rows",
                metrics.index_peek_result_sort_rows.get_sample_count(),
            ),
            (
                "row_collection_seconds",
                metrics.index_peek_row_collection_seconds.get_sample_count(),
            ),
        ])
    }
}

/// The fuel the inline driver spends with the offload off, which is the amount that makes a
/// walk run to an outcome rather than suspend.
fn unbounded_fuel() -> usize {
    usize::MAX
}

/// The observation counts a driver call is expected to leave behind, named so that a failure
/// says which metric moved.
///
/// `walks_offloaded` is zero throughout, because these tests exercise the inline driver and a
/// walk it offloads is counted by the task that finishes it.
fn expected_observations(
    walks_inline: u64,
    error_scan: u64,
    cursor_setup: u64,
    rows: u64,
) -> BTreeMap<&'static str, u64> {
    BTreeMap::from([
        ("walks_inline", walks_inline),
        ("walks_offloaded", 0),
        ("walks_stashed", 0),
        ("error_scan_seconds", error_scan),
        ("cursor_setup_seconds", cursor_setup),
        ("row_iteration_seconds", rows),
        ("row_iteration_rows", rows),
        ("result_sort_seconds", rows),
        ("result_sort_rows", rows),
        ("row_collection_seconds", rows),
    ])
}

/// What a driver call answered with, in a form a test can compare whole.
///
/// Mirrors [`PeekStatus`], which carries no comparison of its own because nothing on the peek
/// path compares one.
#[derive(Debug, PartialEq)]
enum Answer {
    NotReady,
    Offload,
    Ready(PeekResponse),
}

impl From<PeekStatus<IndexPeekScan>> for Answer {
    fn from(status: PeekStatus<IndexPeekScan>) -> Self {
        match status {
            PeekStatus::NotReady => Answer::NotReady,
            // The scan an offload carries has no comparison of its own. What is comparable
            // is that the walk left this driver rather than answering here.
            PeekStatus::Offload(_) => Answer::Offload,
            PeekStatus::Ready(response) => Answer::Ready(response),
        }
    }
}

/// An index peek of `peek` over an index holding `keys` and `errors`.
/// Walks `subject` without the frontier gate, so the observations are the walk's alone.
fn collect(
    subject: &mut IndexPeek,
    max_result_size: u64,
    stash: StashBounds,
    row_iteration_limit: Option<usize>,
    fuel: &mut usize,
    metrics: &IndexPeekMetrics<'_>,
) -> PeekStatus<IndexPeekScan> {
    let (oks, errs) = subject
        .traces
        .resolve(subject.peek.target.id())
        .expect("local traces resolve");
    IndexPeek::walk_traces(
        &subject.peek,
        oks,
        errs,
        max_result_size,
        stash,
        row_iteration_limit,
        fuel,
        metrics,
    )
}

fn index_peek_over(peek: Peek, keys: &[Row], errors: ErrorUpdates) -> IndexPeek {
    IndexPeek {
        peek,
        traces: IndexTraces::Local(trace_bundle(keys, errors)),
        span: tracing::Span::none(),
    }
}

/// The rows a completed peek answers with, each at a multiplicity of one and in the order the
/// walk produced them.
pub(crate) fn row_collection(rows: impl IntoIterator<Item = Row>) -> RowCollection {
    let rows = rows
        .into_iter()
        .map(|row| (row, NonZeroUsize::new(1).expect("non-zero")))
        .collect();
    RowCollection::new(rows, &[])
}

/// The answer a peek gives when its walk completes over `rows`.
pub(crate) fn rows_answer(rows: impl IntoIterator<Item = Row>) -> PeekResponse {
    PeekResponse::Rows(vec![row_collection(rows)])
}

/// A peek whose scan runs to completion is answered with the rows it accumulated, and reports
/// every phase the walk passed through.
#[mz_ore::test]
fn a_completed_scan_answers_with_rows_and_reports_every_phase() {
    let keys: Vec<Row> = (0..6).map(ok_row).collect();
    let mut subject = index_peek_over(
        index_peek(trivial_finishing(), None),
        &keys,
        cancelling_errors(4),
    );
    let metrics = TestMetrics::new();

    let answer = collect(
        &mut subject,
        u64::MAX,
        NO_STASH,
        None,
        &mut unbounded_fuel(),
        &metrics.as_metrics(),
    );

    assert_eq!(
        Answer::from(answer),
        Answer::Ready(rows_answer((0..6).map(ok_row)))
    );
    assert_eq!(metrics.observations(), expected_observations(1, 1, 1, 1));
}

/// A peek its error trace answers reports no phase timer at all, because it reached none of
/// the phases they measure: the error walk stopped short of the trace's end, and the ok walk
/// never ran.
#[mz_ore::test]
fn an_error_answered_peek_reports_no_phase_timers() {
    let keys: Vec<Row> = (0..6).map(ok_row).collect();
    let (errors, expected) = answering_errors(3);

    let mut subject = index_peek_over(index_peek(trivial_finishing(), None), &keys, errors);
    let metrics = TestMetrics::new();

    let answer = collect(
        &mut subject,
        u64::MAX,
        NO_STASH,
        None,
        &mut unbounded_fuel(),
        &metrics.as_metrics(),
    );

    assert_eq!(
        Answer::from(answer),
        Answer::Ready(PeekResponse::Error(expected))
    );
    assert_eq!(metrics.observations(), expected_observations(1, 0, 0, 0));
}

/// A peek whose accumulated rows fill a batch leaves the worker, however much fuel the slice
/// still had, and the walk it leaves with reports nothing.
///
/// A batch-ready suspension is an offload because the driver that finishes a walk is also the
/// one that writes to the peek stash. Withholding it here would strand the peek: the scan
/// makes no progress until its batch is taken, and this driver never takes one.
#[mz_ore::test]
fn a_scan_that_fills_a_batch_leaves_the_worker_with_fuel_to_spare() {
    let keys: Vec<Row> = (0..6).map(ok_row).collect();
    let mut subject = index_peek_over(
        index_peek(trivial_finishing(), None),
        &keys,
        cancelling_errors(4),
    );
    let metrics = TestMetrics::new();

    // A threshold of zero bytes is crossed by the first row, so the scan fills a batch well
    // before the trace runs out and well before unbounded fuel could run out.
    let mut fuel = unbounded_fuel();
    let answer = collect(
        &mut subject,
        u64::MAX,
        STASH_EVERYTHING,
        None,
        &mut fuel,
        &metrics.as_metrics(),
    );

    assert_eq!(Answer::from(answer), Answer::Offload);
    assert!(fuel > 0, "the slice stopped for the batch, not for fuel");
    assert_eq!(metrics.observations(), expected_observations(0, 0, 0, 0));
}

/// A peek whose walk both fills a batch and runs out of fuel leaves the worker exactly as one
/// that did only the first, so the two causes of a suspension need no telling apart.
#[mz_ore::test]
fn a_batch_ready_suspension_out_of_fuel_is_offloaded_too() {
    let keys: Vec<Row> = (0..6).map(ok_row).collect();
    let mut subject = index_peek_over(
        index_peek(trivial_finishing(), None),
        &keys,
        cancelling_errors(0),
    );
    let metrics = TestMetrics::new();

    // A threshold of zero bytes is crossed by the first row the ok walk produces. The empty
    // error trace costs no position, so that one row is all the budget buys, and the scan
    // suspends holding a full batch and out of fuel, with both causes of a suspension in force
    // at once.
    let mut fuel = 1;
    let answer = collect(
        &mut subject,
        u64::MAX,
        STASH_EVERYTHING,
        None,
        &mut fuel,
        &metrics.as_metrics(),
    );
    assert_eq!(Answer::from(answer), Answer::Offload);
    assert_eq!(fuel, 0, "the slice spent every position it was given");
    assert_eq!(metrics.observations(), expected_observations(0, 0, 0, 0));
}

/// A peek whose walk outruns the fuel it was granted leaves the worker rather than being
/// answered, and the walk it leaves with reports nothing.
///
/// Reporting here as well as in the driver that finishes the walk would count one walk twice,
/// on both substrates and in every phase histogram, and the numbers a scan carries are
/// cumulative precisely so that the driver which ends it can report all of them.
#[mz_ore::test]
fn a_scan_that_outruns_its_fuel_leaves_the_worker_reporting_nothing() {
    let keys: Vec<Row> = (0..6).map(ok_row).collect();
    let mut subject = index_peek_over(
        index_peek(trivial_finishing(), None),
        &keys,
        cancelling_errors(0),
    );
    let metrics = TestMetrics::new();

    // An empty error trace is walked out within a position or two, so this fuel is spent
    // inside the ok walk with most of the six keys still ahead of it.
    let mut fuel = 2;
    let answer = collect(
        &mut subject,
        u64::MAX,
        NO_STASH,
        None,
        &mut fuel,
        &metrics.as_metrics(),
    );

    assert_eq!(Answer::from(answer), Answer::Offload);
    assert_eq!(
        fuel, 0,
        "an offloaded slice spent every position it was given"
    );
    assert_eq!(metrics.observations(), expected_observations(0, 0, 0, 0));
}

/// A peek its ok walk fails is answered with that error, and reports the phases that walk
/// reached: the error scan and the cursor setup, both of which precede it, and none of the
/// histograms an inline answer observes into.
///
/// The sqllogictest sweeps compare answers, so they say nothing about which histogram a
/// failing peek moved. This is what says that the two timers a clean error walk earns are
/// observed on the way to the failure rather than after it.
#[mz_ore::test]
fn an_ok_phase_failure_reports_the_phases_the_walk_reached() {
    let keys: Vec<Row> = (0..6).map(ok_row).collect();
    let mut subject = index_peek_over(
        index_peek(trivial_finishing(), None),
        &keys,
        cancelling_errors(4),
    );
    let metrics = TestMetrics::new();

    // A ceiling of one byte is crossed by the first row the ok walk produces, so the peek
    // fails inside that walk rather than in the error walk before it.
    let max_result_size = 1;
    let answer = collect(
        &mut subject,
        max_result_size,
        NO_STASH,
        None,
        &mut unbounded_fuel(),
        &metrics.as_metrics(),
    );

    assert_eq!(
        Answer::from(answer),
        Answer::Ready(PeekResponse::Error(PeekError::ResultExceedsMaxSize {
            max_result_size: usize::cast_from(max_result_size),
        }))
    );
    assert_eq!(metrics.observations(), expected_observations(1, 1, 1, 0));
}
