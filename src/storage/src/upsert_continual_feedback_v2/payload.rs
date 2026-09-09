// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Upsert state with row bytes stored separately from mergeable metadata.
//!
//! Source commands keep rows inline until offset consolidation chooses winners.
//! The source batcher then writes those rows to the shared payload store. Persist
//! feedback stores fingerprints alongside independent row locators. Logical
//! consolidation compares payload bytes when matching fingerprints need resolution.
//!
//! Handles do not own rows. Manifests carry that ownership alongside metadata.
//! Draining probes feedback in key windows, resolves logical equality, and decodes
//! surviving rows for output.
//! See [`super::upsert_inner`] for the frontier and eligibility protocol.

use std::collections::{BTreeMap, VecDeque};
use std::marker::PhantomData;
use std::rc::Rc;

use columnar::{Borrow, Columnar, Index, Len};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::Logger;
use differential_dataflow::operators::arrange::agent::TraceAgent;
use differential_dataflow::trace::chunk::{Chunk, ChunkBatcher, ChunkSpine};
use differential_dataflow::trace::{Batcher, Description, TraceReader};
use futures::StreamExt;
use mz_ore::pool::Pool;
use mz_repr::{Diff, GlobalId, Row};
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::payload::{
    PayloadChunk, PayloadChunker, PayloadLayout, PayloadStaging,
};
use mz_timely_util::columnar::unload::UnloadBatch;
use mz_timely_util::out_of_core::{
    Manifest, PayloadComparator, PayloadRef, ReadLease, RowHandle, Store,
};
use timely::container::{CapacityContainerBuilder, PushInto};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely::dataflow::{Stream, StreamVec};
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use timely::progress::frontier::{Antichain, AntichainRef};

use super::{
    DrainStats, TimeClass, UpsertDiff, UpsertOutputHandle, UpsertStashArm, UpsertUpdate,
    classify_time, decode_upsert_value, upsert_value_byte_len,
};
use crate::upsert::{UpsertKey, UpsertValue};

const BLOCK_BYTES: usize = 2 << 20;
// A drain retains two decoded blocks.
// Eight blocks leave room to acquire the next read without exhausting admission.
const READ_BUDGET_BYTES: usize = 8 * BLOCK_BYTES;
const READ_CONCURRENCY: usize = 2;
// Each key produces at most one retraction and one insertion per probe window.
const PROBE_WINDOW_KEYS: usize = 1024;
const DECODE_CACHE_BLOCKS: usize = 2;

/// A tagged upsert row, either awaiting encoding or stored in a payload block.
///
/// Inline rows also cover values too large for a block.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, columnar::Columnar)]
#[columnar(derive(PartialEq, Eq, PartialOrd, Ord))]
pub(super) enum Value {
    External(RowHandle),
    Inline(Row),
}

impl Default for Value {
    fn default() -> Self {
        Self::Inline(Row::default())
    }
}

fn visit(value: columnar::Ref<'_, Value>, rows: &mut Vec<RowHandle>) {
    if let ValueReference::External(row) = value {
        rows.push(RowHandle::into_owned(row));
    }
}

/// Feedback stores the payload handle alongside the key, with an additive diff.
pub(super) struct FeedbackLayout;
impl PayloadLayout for FeedbackLayout {
    type Data = (UpsertKey, (u64, Value));
    type Diff = Diff;
    const COMPARE_PAYLOADS: bool = true;

    fn compare<'a>(
        (ka, (ha, a)): columnar::Ref<'a, Self::Data>,
        (kb, (hb, b)): columnar::Ref<'a, Self::Data>,
        owners: &[&Manifest],
        cache: &mut PayloadComparator,
    ) -> std::cmp::Ordering {
        ka.cmp(kb).then_with(|| ha.cmp(hb)).then_with(|| {
            cache
                .compare(payload_ref(a), payload_ref(b), owners)
                .expect("feedback payloads remain owned")
        })
    }

    fn visit(
        (_, (_, value)): columnar::Ref<'_, Self::Data>,
        _: columnar::Ref<'_, Diff>,
        rows: &mut Vec<RowHandle>,
    ) {
        visit(value, rows);
    }
}

fn payload_ref(value: columnar::Ref<'_, Value>) -> PayloadRef<'_> {
    match value {
        ValueReference::External(row) => PayloadRef::External(RowHandle::into_owned(row)),
        ValueReference::Inline(row) => PayloadRef::Inline(row.data()),
    }
}

fn fingerprint(row: &Row) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    row.data().hash(&mut hasher);
    hasher.finish()
}

/// Source commands carry their payload in the maximum-offset difference.
pub(super) struct SourceLayout<O>(PhantomData<O>);
impl<O: Columnar> PayloadLayout for SourceLayout<O> {
    type Data = UpsertKey;
    type Diff = UpsertDiff<O, Value>;
    fn visit(
        _: columnar::Ref<'_, UpsertKey>,
        diff: columnar::Ref<'_, Self::Diff>,
        rows: &mut Vec<RowHandle>,
    ) {
        if let Some(value) = diff.value {
            visit(value, rows);
        }
    }
}

type SourceChunk<T, O> = PayloadChunk<SourceLayout<O>, T>;
pub(super) type FeedbackChunk<T> = PayloadChunk<FeedbackLayout, T>;
pub(super) type FeedbackSpine<T> = ChunkSpine<FeedbackChunk<T>>;
type FeedbackBatch<T> = <FeedbackSpine<T> as TraceReader>::Batch;

/// Create the store shared by source commands and persist feedback.
pub(super) fn store(pool: Pool) -> Store {
    Store::new(
        pool,
        BLOCK_BYTES,
        READ_BUDGET_BYTES,
        READ_CONCURRENCY,
        &mz_timely_util::columnar::chunk::LZ4_CODEC,
    )
    .expect("valid payload store configuration")
}

/// Consolidates source metadata and owns payloads awaiting restashing.
pub(super) struct SourceBatcher<C: Chunk + Default + 'static> {
    inner: ChunkBatcher<C>,
    store: Store,
    // Draining puts ineligible updates in a separate buffer. These owners keep
    // their handles valid until flush has attached them to replacement chunks.
    restash_owners: Vec<Manifest>,
}

impl<C: Chunk + Default + 'static> Batcher for SourceBatcher<C> {
    type Output = C;
    type Time = C::Time;
    fn new(_: Option<Logger>, _: usize) -> Self {
        panic!("construct with the shared payload store")
    }
    fn seal(&mut self, upper: Antichain<C::Time>) -> (Vec<Self::Output>, Description<C::Time>) {
        self.inner.seal(upper)
    }
    fn frontier(&mut self) -> AntichainRef<'_, C::Time> {
        self.inner.frontier()
    }
}

impl<C: Chunk + Default + 'static> PushInto<C> for SourceBatcher<C> {
    fn push_into(&mut self, chunk: C) {
        self.inner.push_into(chunk);
    }
}

/// Connects payload-backed source and feedback batches to the shared upsert loop.
pub(super) struct PayloadArm;
impl<T, O> UpsertStashArm<T, O> for PayloadArm
where
    T: Timestamp + TotalOrder + Lattice + Sync + columnation::Columnation + Columnar + Default,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    O: Columnar + Default + Ord + Clone + Send + Sync + 'static,
    for<'a> columnar::Ref<'a, O>: Copy + Ord,
{
    type Value = Value;
    type Spine = FeedbackSpine<T>;
    type Batcher = SourceBatcher<SourceChunk<T, O>>;
    type Sealed = Vec<SourceChunk<T, O>>;
    async fn seal(batcher: &mut Self::Batcher, upper: Antichain<T>) -> Self::Sealed {
        batcher.seal(upper).0
    }
    fn frontier(batcher: &mut Self::Batcher) -> Antichain<T> {
        batcher.frontier().to_owned()
    }

    fn new_batcher(store: Option<Store>) -> Self::Batcher {
        let store = store.expect("payload arm shares its store with feedback");
        Self::Batcher {
            inner: Batcher::new(None, 0),
            store,
            restash_owners: Vec::new(),
        }
    }

    fn encode(value: Option<Row>, _batcher: &mut Self::Batcher) -> Option<Value> {
        value.map(Value::Inline)
    }

    fn end_flush(batcher: &mut Self::Batcher) {
        batcher.restash_owners.clear();
    }

    async fn push_chunk(batcher: &mut Self::Batcher, chunk: Column<UpsertUpdate<T, O, Value>>) {
        // Publish only the initial chunker's winners, packed in metadata order.
        let mut builder = batcher.store.builder();
        let mut metadata = Column::default();
        let view = chunk.borrow();
        for index in 0..view.len() {
            let (key, time, diff) = view.get(index);
            let value = diff.value.map(|value| match value {
                ValueReference::Inline(row) if batcher.store.can_store(row.byte_len()) => {
                    Value::External(builder.push(row.data()).expect("row fits payload block"))
                }
                value => Value::into_owned(value),
            });
            metadata.push_into(&(
                *key,
                T::into_owned(time),
                UpsertDiff {
                    from_time: O::into_owned(diff.from_time),
                    value,
                },
            ));
        }
        let owner = builder.finish();
        let mut owners: Vec<_> = batcher.restash_owners.iter().collect();
        owners.push(&owner);
        batcher
            .inner
            .push_into(PayloadChunk::new(metadata, &owners));
    }

    async fn drain(
        sealed: Vec<SourceChunk<T, O>>,
        ineligible: &mut Vec<UpsertUpdate<T, O, Value>>,
        output_handle: &UpsertOutputHandle<T>,
        output_cap: &Capability<T>,
        persist_upper: &Antichain<T>,
        trace: &mut TraceAgent<Self::Spine>,
        _worker_id: usize,
        _source_id: GlobalId,
        _async_reads: bool,
        batcher: &mut Self::Batcher,
    ) -> DrainStats {
        let batches = trace
            .batches_through(Antichain::new().borrow())
            .expect("complete feedback batches");
        let mut stats = DrainStats {
            eligible: 0,
            result_count: 0,
            inserts: 0,
            updates: 0,
            deletes: 0,
            output_count: 0,
        };
        let mut decoder = ValueDecoder::default();
        for chunk in sealed {
            let metadata = chunk.metadata_async().await;
            let view = metadata.borrow();
            let mut retained = false;
            let source_owner = Rc::new(chunk.manifest().clone());
            for start in (0..view.len()).step_by(PROBE_WINDOW_KEYS) {
                let end = (start + PROBE_WINDOW_KEYS).min(view.len());
                let keys: Vec<_> = (start..end)
                    .filter_map(|i| {
                        let (key, time, _) = view.get(i);
                        matches!(
                            classify_time(persist_upper, &T::into_owned(time)),
                            TimeClass::Eligible
                        )
                        .then_some(*key)
                    })
                    .collect();
                let (mut previous_values, feedback_owner) = lookup_feedback(&keys, &batches).await;
                let mut pending = Vec::with_capacity(2 * (end - start));
                for index in start..end {
                    let (key, time, diff) = view.get(index);
                    let time = T::into_owned(time);
                    match classify_time(persist_upper, &time) {
                        TimeClass::AlreadyPersisted => continue,
                        TimeClass::Ineligible => {
                            ineligible.push((*key, time, UpsertDiff::<O, Value>::into_owned(diff)));
                            retained = true;
                            continue;
                        }
                        TimeClass::Eligible => {}
                    }
                    stats.eligible += 1;
                    let previous = previous_values.remove(key);
                    let value = diff.value.map(Value::into_owned);
                    if let Some(previous) = previous {
                        stats.result_count += 1;
                        if value.is_some() {
                            stats.updates += 1;
                        } else {
                            stats.deletes += 1;
                        }
                        pending.push(PendingOutput {
                            value: previous,
                            owner: Rc::clone(&feedback_owner),
                            time: time.clone(),
                            diff: Diff::MINUS_ONE,
                        });
                    } else if value.is_some() {
                        stats.inserts += 1;
                    }
                    if let Some(value) = value {
                        pending.push(PendingOutput {
                            value,
                            owner: Rc::clone(&source_owner),
                            time,
                            diff: Diff::ONE,
                        });
                    }
                }
                // Differential updates at a completed time can be emitted in any
                // order. Group reads by physical block within this metadata window.
                pending.sort_by_key(|update| match &update.value {
                    Value::External(row) => Some(*row),
                    Value::Inline(_) => None,
                });
                for PendingOutput {
                    value,
                    owner,
                    time,
                    diff,
                } in pending
                {
                    let row = decoder.decode(&value, &owner, &batcher.store).await;
                    let size = upsert_value_byte_len(&row);
                    output_handle
                        .give_fueled(output_cap, (row, time, diff), size)
                        .await;
                    stats.output_count += 1;
                }
            }
            if retained {
                batcher.restash_owners.push(chunk.manifest().clone());
            }
        }
        stats
    }
}

/// Look up logical feedback values for sorted keys, resolving equal fingerprints.
///
/// The caller must probe only keys eligible at the current persist frontier.
/// At that frontier, each key has at most one value with multiplicity one.
async fn lookup_feedback<T>(
    keys: &[UpsertKey],
    batches: &[FeedbackBatch<T>],
) -> (BTreeMap<UpsertKey, Value>, Rc<Manifest>)
where
    T: Timestamp + Lattice + Columnar + Default,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
{
    let mut staging = PayloadStaging::<((UpsertKey, (u64, Value)), T, Diff)>::default();
    if !keys.is_empty() {
        let probes = UpsertKey::as_columns(keys);
        for batch in batches {
            batch
                .extract_into_async(probes.borrow(), &mut staging)
                .await;
        }
    }
    let owners: Vec<_> = staging.owners.iter().map(|owner| &**owner).collect();
    let mut hits = Column::default();
    let rows = staging.updates.borrow();
    for index in 0..rows.len() {
        let ((key, (hash, value)), _, diff) = rows.get(index);
        hits.push_into(&(
            (*key, (*hash, Value::into_owned(value))),
            T::default(),
            Diff::into_owned(diff),
        ));
    }
    // Eligible feedback is read as a collection at its completed frontier.
    // Distinct physical copies must cancel across trace batches as well as within them.
    let hits = FeedbackChunk::<T>::consolidate(hits, &owners);
    let mut previous_values = BTreeMap::new();
    let rows = hits.borrow();
    for index in 0..rows.len() {
        let ((key, (_, value)), _, diff) = rows.get(index);
        if diff.is_positive() {
            assert_eq!(diff, Diff::ONE, "feedback value multiplicity");
            let previous = previous_values.insert(*key, Value::into_owned(value));
            assert!(previous.is_none(), "multiple feedback values for one key");
        }
    }
    let owners = Rc::new(
        Manifest::retain(
            previous_values.values().filter_map(|value| match value {
                Value::External(row) => Some(*row),
                Value::Inline(_) => None,
            }),
            owners,
        )
        .expect("probe results retain payload ownership"),
    );
    (previous_values, owners)
}

/// An output row whose owner remains live until decoding and emission finish.
struct PendingOutput<T> {
    value: Value,
    owner: Rc<Manifest>,
    time: T,
    diff: Diff,
}

/// A bounded cache of decoded blocks shared by one drain's output rows.
#[derive(Default)]
struct ValueDecoder {
    blocks: VecDeque<ReadLease>,
}

impl ValueDecoder {
    async fn decode(&mut self, value: &Value, owner: &Manifest, store: &Store) -> UpsertValue {
        match value {
            Value::Inline(row) => decode_upsert_value(row.iter()),
            Value::External(row) => {
                let hit = self.blocks.iter().position(|lease| lease.get(*row).is_ok());
                if let Some(index) = hit {
                    let lease = self.blocks.remove(index).expect("observed cached block");
                    self.blocks.push_back(lease);
                } else {
                    if self.blocks.len() == DECODE_CACHE_BLOCKS {
                        self.blocks.pop_front();
                    }
                    self.blocks.push_back(
                        store
                            .prepare_read([(owner, *row)])
                            .expect("owned payload fits admission")
                            .read()
                            .await,
                    );
                }
                let bytes = self
                    .blocks
                    .back()
                    .expect("loaded payload")
                    .get(*row)
                    .expect("requested row");
                // SAFETY: bytes came from Row::data and the pool preserves their contents.
                let row = unsafe { Row::from_bytes_unchecked(bytes) };
                decode_upsert_value(row.iter())
            }
        }
    }
}

/// Pack sorted feedback rows into independent payload blocks.
pub(super) fn encode_feedback<'scope, T>(
    input: Stream<'scope, T, Column<((UpsertKey, Row), T, Diff)>>,
    store: Store,
) -> (
    StreamVec<'scope, T, PayloadChunk<FeedbackLayout, T>>,
    PressOnDropButton,
)
where
    T: Timestamp + Lattice + Columnar + Default,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
{
    let mut builder =
        AsyncOperatorBuilder::new("Upsert feedback payloads".to_owned(), input.scope());
    let (output, stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<PayloadChunk<FeedbackLayout, T>>>>();
    let mut input = builder.new_input_for(input, Pipeline, &output);
    let button = builder.build(move |caps| async move {
        drop(caps);
        while let Some(event) = input.next().await {
            if let AsyncEvent::Data(cap, column) = event {
                let view = column.borrow();
                let mut rows: Vec<_> = (0..view.len())
                    .map(|i| {
                        let ((key, row), time, diff) =
                            <((UpsertKey, Row), T, Diff)>::into_owned(view.get(i));
                        ((key, (fingerprint(&row), row)), time, diff)
                    })
                    .collect();
                // Sort while bytes are already resident, and pack in the same
                // order to preserve locality for later exact comparisons.
                rows.sort_by(|((ka, (ha, a)), ta, _), ((kb, (hb, b)), tb, _)| {
                    (ka, ha, a.data(), ta).cmp(&(kb, hb, b.data(), tb))
                });
                rows.dedup_by(|next, previous| {
                    if next.0 == previous.0 && next.1 == previous.1 {
                        previous.2 += next.2;
                        true
                    } else {
                        false
                    }
                });
                let mut builder = store.builder();
                let mut metadata = Column::default();
                for ((key, (hash, row)), time, diff) in rows {
                    if diff == Diff::ZERO {
                        continue;
                    }
                    let value = if store.can_store(row.byte_len()) {
                        Value::External(builder.push(row.data()).expect("feedback row fits block"))
                    } else {
                        Value::Inline(row)
                    };
                    metadata.push_into(&((key, (hash, value)), time, diff));
                }
                let owner = builder.finish();
                if !metadata.is_empty() {
                    output.give(&cap, PayloadChunk::new(metadata, &[&owner]));
                }
                tokio::task::yield_now().await;
            }
        }
    });
    (stream, button.press_on_drop())
}

pub(super) type FeedbackChunker<T> = PayloadChunker<FeedbackLayout, T>;
