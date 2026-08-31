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
use mz_repr::{Datum, Diff, RelationDesc};
use mz_row_spine::{RowRowBatcher, RowRowBuilder, RowRowSpine};
use mz_timely_util::columnation::ColumnationStack;
use timely::container::PushInto;
use timely::dataflow::operators::generic::OperatorInfo;

use crate::typedefs::{ErrAgent, ErrSpine, RowRowAgent};

use super::error_scan::tests::{
    ErrorUpdates, PEEK_TIMESTAMP, cancelling, error, error_batch, holding,
};
use super::*;

/// The collection the peeks in these tests read.
pub(crate) const TARGET_ID: GlobalId = GlobalId::User(1);

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
pub(crate) fn ok_row(value: u8) -> Row {
    Row::pack_slice(&[Datum::UInt8(value)])
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
    Peek {
        target: PeekTarget::Index { id: TARGET_ID },
        result_desc: RelationDesc::empty(),
        literal_constraints,
        uuid: Uuid::nil(),
        timestamp: PEEK_TIMESTAMP,
        finishing,
        map_filter_project,
        otel_ctx: OpenTelemetryContext::empty(),
    }
}

/// The histograms an index peek observes into, owned by the test that reads them back.
struct TestMetrics {
    seek_fulfillment_seconds: prometheus::Histogram,
    frontier_check_seconds: prometheus::Histogram,
    error_scan_seconds: prometheus::Histogram,
    cursor_setup_seconds: prometheus::Histogram,
    row_iteration_seconds: prometheus::Histogram,
    row_iteration_rows: prometheus::Histogram,
    result_sort_seconds: prometheus::Histogram,
    result_sort_rows: prometheus::Histogram,
    row_collection_seconds: prometheus::Histogram,
}

fn histogram(name: &str) -> prometheus::Histogram {
    prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(name, name))
        .expect("valid histogram")
}

impl TestMetrics {
    fn new() -> Self {
        Self {
            seek_fulfillment_seconds: histogram("seek_fulfillment_seconds"),
            frontier_check_seconds: histogram("frontier_check_seconds"),
            error_scan_seconds: histogram("error_scan_seconds"),
            cursor_setup_seconds: histogram("cursor_setup_seconds"),
            row_iteration_seconds: histogram("row_iteration_seconds"),
            row_iteration_rows: histogram("row_iteration_rows"),
            result_sort_seconds: histogram("result_sort_seconds"),
            result_sort_rows: histogram("result_sort_rows"),
            row_collection_seconds: histogram("row_collection_seconds"),
        }
    }

    fn as_metrics(&self) -> IndexPeekMetrics<'_> {
        IndexPeekMetrics {
            seek_fulfillment_seconds: &self.seek_fulfillment_seconds,
            frontier_check_seconds: &self.frontier_check_seconds,
            error_scan_seconds: &self.error_scan_seconds,
            cursor_setup_seconds: &self.cursor_setup_seconds,
            row_iteration_seconds: &self.row_iteration_seconds,
            row_iteration_rows: &self.row_iteration_rows,
            result_sort_seconds: &self.result_sort_seconds,
            result_sort_rows: &self.result_sort_rows,
            row_collection_seconds: &self.row_collection_seconds,
        }
    }

    /// How often each histogram that `collect_finished_data` can observe into was observed.
    ///
    /// The two histograms the enclosing `seek_fulfillment` owns are left out, because the
    /// tests that read this call `collect_finished_data` directly.
    fn observations(&self) -> BTreeMap<&'static str, u64> {
        BTreeMap::from([
            (
                "error_scan_seconds",
                self.error_scan_seconds.get_sample_count(),
            ),
            (
                "cursor_setup_seconds",
                self.cursor_setup_seconds.get_sample_count(),
            ),
            (
                "row_iteration_seconds",
                self.row_iteration_seconds.get_sample_count(),
            ),
            (
                "row_iteration_rows",
                self.row_iteration_rows.get_sample_count(),
            ),
            (
                "result_sort_seconds",
                self.result_sort_seconds.get_sample_count(),
            ),
            ("result_sort_rows", self.result_sort_rows.get_sample_count()),
            (
                "row_collection_seconds",
                self.row_collection_seconds.get_sample_count(),
            ),
        ])
    }
}

/// The observation counts a driver call is expected to leave behind, named so that a failure
/// says which histogram moved.
fn expected_observations(
    error_scan: u64,
    cursor_setup: u64,
    rows: u64,
) -> BTreeMap<&'static str, u64> {
    BTreeMap::from([
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
    UsePeekStash,
    Ready(PeekResponse),
}

impl From<PeekStatus> for Answer {
    fn from(status: PeekStatus) -> Self {
        match status {
            PeekStatus::NotReady => Answer::NotReady,
            PeekStatus::UsePeekStash => Answer::UsePeekStash,
            PeekStatus::Ready(response) => Answer::Ready(response),
        }
    }
}

/// An index peek of `peek` over an index holding `keys` and `errors`.
fn index_peek_over(peek: Peek, keys: &[Row], errors: ErrorUpdates) -> IndexPeek {
    IndexPeek {
        peek,
        trace_bundle: trace_bundle(keys, errors),
        span: tracing::Span::none(),
    }
}

/// The rows a completed peek over `values` answers with.
fn row_collection(values: impl IntoIterator<Item = u8>) -> RowCollection {
    let rows = values
        .into_iter()
        .map(|value| (ok_row(value), NonZeroUsize::new(1).expect("non-zero")))
        .collect();
    RowCollection::new(rows, &[])
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

    let answer =
        subject.collect_finished_data(u64::MAX, false, usize::MAX, None, &metrics.as_metrics());

    assert_eq!(
        Answer::from(answer),
        Answer::Ready(PeekResponse::Rows(vec![row_collection(0..6)]))
    );
    assert_eq!(metrics.observations(), expected_observations(1, 1, 1));
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

    let answer =
        subject.collect_finished_data(u64::MAX, false, usize::MAX, None, &metrics.as_metrics());

    assert_eq!(
        Answer::from(answer),
        Answer::Ready(PeekResponse::Error(expected))
    );
    assert_eq!(metrics.observations(), expected_observations(0, 0, 0));
}

/// A peek whose accumulated rows fill a batch is diverted to the stash rather than answered
/// inline. The phases the walk did pass through are reported, and those only a peek answered
/// inline reaches are not.
#[mz_ore::test]
fn a_scan_that_fills_a_batch_diverts_the_peek_to_the_stash() {
    let keys: Vec<Row> = (0..6).map(ok_row).collect();
    let mut subject = index_peek_over(
        index_peek(trivial_finishing(), None),
        &keys,
        cancelling_errors(4),
    );
    let metrics = TestMetrics::new();

    // A threshold of zero bytes is crossed by the first row, so the scan fills a batch well
    // before the trace runs out.
    let answer = subject.collect_finished_data(u64::MAX, true, 0, None, &metrics.as_metrics());

    assert_eq!(Answer::from(answer), Answer::UsePeekStash);
    assert_eq!(metrics.observations(), expected_observations(1, 1, 0));
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
    let answer = subject.collect_finished_data(
        max_result_size,
        false,
        usize::MAX,
        None,
        &metrics.as_metrics(),
    );

    assert_eq!(
        Answer::from(answer),
        Answer::Ready(PeekResponse::Error(PeekError::ResultExceedsMaxSize {
            max_result_size: usize::cast_from(max_result_size),
        }))
    );
    assert_eq!(metrics.observations(), expected_observations(1, 1, 0));
}
