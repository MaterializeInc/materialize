// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::rc::Rc;

use columnar::{Columnar, Index, Len};
use futures::StreamExt;
use mz_timely_util::columnar::payload::{PayloadChunk, PayloadChunker, PayloadLayout};
use mz_timely_util::out_of_core::{Manifest, PayloadInterner, ReadLease, RowHandle, Store};
use timely::container::ContainerBuilder;

use super::*;

const BLOCK_BYTES: usize = 2 << 20;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, columnar::Columnar)]
#[columnar(derive(PartialEq, Eq, PartialOrd, Ord))]
pub(super) enum Value {
    External(RowHandle),
    // Oversized rows retain the existing inline path instead of imposing a row limit.
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

pub(super) struct FeedbackLayout;
impl PayloadLayout for FeedbackLayout {
    type Data = (UpsertKey, Value);
    type Diff = Diff;
    fn visit(
        (_, value): columnar::Ref<'_, Self::Data>,
        _: columnar::Ref<'_, Diff>,
        rows: &mut Vec<RowHandle>,
    ) {
        visit(value, rows);
    }
}

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
pub(super) type FeedbackSpine<T> = ChunkSpine<PayloadChunk<FeedbackLayout, T>>;

#[cfg(test)]
thread_local! {
    static TEST_POOL: std::cell::RefCell<Option<mz_ore::pool::Pool>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(super) struct TestPool;
#[cfg(test)]
impl Drop for TestPool {
    fn drop(&mut self) {
        TEST_POOL.with(|pool| *pool.borrow_mut() = None);
        mz_timely_util::columnar::chunk::set_spill_override(None);
    }
}
#[cfg(test)]
pub(super) fn test_pool(pool: mz_ore::pool::Pool) -> TestPool {
    mz_timely_util::columnar::chunk::set_spill_override(Some(pool.clone()));
    TEST_POOL.with(|cell| *cell.borrow_mut() = Some(pool));
    TestPool
}

pub(super) fn store() -> Store {
    #[cfg(test)]
    let pool = TEST_POOL.with(|pool| pool.borrow().clone());
    #[cfg(not(test))]
    let pool: Option<mz_ore::pool::Pool> = None;
    let pool = pool
        .or_else(mz_timely_util::pool_config::active_pool)
        .or_else(mz_timely_util::pool_config::global_pool)
        .expect("payload upsert requires a buffer pool");
    Store::new(
        pool,
        BLOCK_BYTES,
        8 * BLOCK_BYTES,
        2,
        &mz_timely_util::columnar::chunk::LZ4_CODEC,
    )
    .expect("valid payload store configuration")
}

pub(super) struct SourceBatcher<T, O>
where
    T: Timestamp + Lattice + Columnar + Default,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    O: Columnar + Default + Ord + Clone,
    for<'a> columnar::Ref<'a, O>: Copy + Ord,
{
    inner: ChunkBatcher<SourceChunk<T, O>>,
    store: Store,
    owners: Vec<Manifest>,
}

impl<T, O> Batcher for SourceBatcher<T, O>
where
    T: Timestamp + Lattice + Columnar + Default,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    O: Columnar + Default + Ord + Clone,
    for<'a> columnar::Ref<'a, O>: Copy + Ord,
{
    type Output = SourceChunk<T, O>;
    type Time = T;
    fn new(_: Option<Logger>, _: usize) -> Self {
        panic!("construct with the shared payload store")
    }
    fn seal(&mut self, upper: Antichain<T>) -> (Vec<Self::Output>, Description<T>) {
        self.inner.seal(upper)
    }
    fn frontier(&mut self) -> AntichainRef<'_, T> {
        self.inner.frontier()
    }
}

impl<T, O> PushInto<SourceChunk<T, O>> for SourceBatcher<T, O>
where
    T: Timestamp + Lattice + Columnar + Default,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    O: Columnar + Default + Ord + Clone,
    for<'a> columnar::Ref<'a, O>: Copy + Ord,
{
    fn push_into(&mut self, chunk: SourceChunk<T, O>) {
        self.inner.push_into(chunk);
    }
}

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
    type Batcher = SourceBatcher<T, O>;

    fn new_batcher(store: Option<Store>) -> Self::Batcher {
        let store = store.expect("payload arm shares its store with feedback");
        Self::Batcher {
            inner: Batcher::new(None, 0),
            store,
            owners: Vec::new(),
        }
    }

    fn encode(value: Option<Row>, _batcher: &mut Self::Batcher) -> Option<Value> {
        value.map(Value::Inline)
    }

    fn end_flush(batcher: &mut Self::Batcher) {
        batcher.owners.clear();
    }

    fn push_chunk(batcher: &mut Self::Batcher, chunk: Column<UpsertUpdate<T, O, Value>>) {
        // Publish only the initial chunker's winners, packed in metadata order.
        let mut builder = batcher.store.builder();
        let mut metadata = Column::default();
        let view = chunk.borrow();
        for index in 0..view.len() {
            let (key, time, diff) = view.get(index);
            let value = diff.value.map(|value| match value {
                ValueReference::Inline(row) if row.byte_len() + 8 <= BLOCK_BYTES => {
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
        let mut owners: Vec<_> = batcher.owners.iter().collect();
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
        let mut cache = std::collections::VecDeque::new();
        for chunk in sealed {
            let metadata = chunk.metadata_async().await;
            let view = metadata.borrow();
            let mut retained = false;
            let source_owner = Rc::new(chunk.manifest().clone());
            for start in (0..view.len()).step_by(1024) {
                let end = (start + 1024).min(view.len());
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
                let mut hits: BTreeMap<(UpsertKey, Value), Diff> = BTreeMap::new();
                let mut staging = mz_timely_util::columnar::payload::PayloadStaging::<(
                    (UpsertKey, Value),
                    T,
                    Diff,
                )>::default();
                if !keys.is_empty() {
                    use columnar::Borrow;
                    let probes = UpsertKey::as_columns(&keys);
                    for batch in &batches {
                        batch
                            .extract_into_async(probes.borrow(), &mut staging)
                            .await;
                    }
                    let rows = staging.updates.borrow();
                    for index in 0..rows.len() {
                        let ((key, value), _, diff) = rows.get(index);
                        *hits
                            .entry((*key, Value::into_owned(value)))
                            .or_insert(Diff::ZERO) += Diff::into_owned(diff);
                    }
                }
                let owners = Manifest::retain(
                    hits.keys().filter_map(|(_, value)| match value {
                        Value::External(row) => Some(*row),
                        Value::Inline(_) => None,
                    }),
                    staging.owners.iter().map(|owner| &**owner),
                )
                .expect("probe results retain payload ownership");
                let owners = Rc::new(owners);
                let mut old = BTreeMap::new();
                for ((key, value), diff) in hits {
                    if diff.is_positive() {
                        assert_eq!(diff, Diff::ONE, "feedback value multiplicity");
                        let previous = old.insert(key, (value, Rc::clone(&owners)));
                        assert!(previous.is_none(), "multiple feedback values for one key");
                    }
                }
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
                    let previous = old.remove(key);
                    let value = diff.value.map(Value::into_owned);
                    if let Some((previous, owner)) = previous {
                        stats.result_count += 1;
                        if value.is_some() {
                            stats.updates += 1;
                        } else {
                            stats.deletes += 1;
                        }
                        pending.push((previous, owner, time.clone(), Diff::MINUS_ONE));
                    } else if value.is_some() {
                        stats.inserts += 1;
                    }
                    if let Some(value) = value {
                        pending.push((value, Rc::clone(&source_owner), time, Diff::ONE));
                    }
                }
                // Differential updates at a completed time can be emitted in any
                // order. Group reads by physical block within this metadata window.
                pending.sort_by_key(|(value, _, _, _)| match value {
                    Value::External(row) => Some(*row),
                    Value::Inline(_) => None,
                });
                for (value, owner, time, diff) in pending {
                    let row = decode(&value, &owner, &batcher.store, &mut cache).await;
                    let size = upsert_value_byte_len(&row);
                    output_handle
                        .give_fueled(output_cap, (row, time, diff), size)
                        .await;
                    stats.output_count += 1;
                }
            }
            if retained {
                batcher.owners.push(chunk.manifest().clone());
            }
        }
        stats
    }
}

async fn decode(
    value: &Value,
    owner: &Manifest,
    store: &Store,
    cache: &mut std::collections::VecDeque<ReadLease>,
) -> UpsertValue {
    match value {
        Value::Inline(row) => decode_upsert_value(row.iter()),
        Value::External(row) => {
            let hit = cache.iter().position(|lease| lease.get(*row).is_ok());
            if let Some(index) = hit {
                let lease = cache.remove(index).expect("observed cached block");
                cache.push_back(lease);
            } else {
                // Two blocks preserve old/new locality. Together with the feedback
                // interner's one-block cache they fit below the eight-block budget.
                if cache.len() == 2 {
                    cache.pop_front();
                }
                cache.push_back(
                    store
                        .prepare_read([(owner, *row)])
                        .expect("owned payload fits admission")
                        .read()
                        .await,
                );
            }
            let bytes = cache
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
        let mut interner = PayloadInterner::new(store);
        let mut chunker: ColumnChunker<((UpsertKey, Value), T, Diff)> = Default::default();
        while let Some(event) = input.next().await {
            if let AsyncEvent::Data(cap, column) = event {
                let view = column.borrow();
                let rows: Vec<_> = (0..view.len())
                    .map(|i| <((UpsertKey, Row), T, Diff)>::into_owned(view.get(i)))
                    .collect();
                let bytes: Vec<_> = rows
                    .iter()
                    .filter(|((_, row), _, _)| row.byte_len() + 8 <= BLOCK_BYTES)
                    .map(|((_, row), _, _)| row.data().to_vec())
                    .collect();
                let (handles, owner) = interner
                    .intern(&bytes)
                    .await
                    .expect("feedback payloads fit store");
                let mut handles = handles.into_iter();
                let mut metadata: Column<((UpsertKey, Value), T, Diff)> = Default::default();
                for ((key, row), time, diff) in rows {
                    let value = if row.byte_len() + 8 > BLOCK_BYTES {
                        Value::Inline(row)
                    } else {
                        Value::External(handles.next().expect("one handle per external row"))
                    };
                    metadata.push_into(&((key, value), time, diff));
                }
                chunker.push_into(&mut metadata);
                while let Some(column) = chunker.extract() {
                    output.give(&cap, PayloadChunk::new(std::mem::take(column), &[&owner]));
                }
                tokio::task::yield_now().await;
            }
        }
    });
    (stream, button.press_on_drop())
}

pub(super) type FeedbackChunker<T> = PayloadChunker<FeedbackLayout, T>;
