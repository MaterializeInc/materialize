// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The persist-feedback side of the upsert-v2 operator: the feedback
//! arrangement's types, and the `UpsertValue` to `Row` encoding it stores.

use differential_dataflow::hashable::Hashable;
use differential_dataflow::logging::Logger;
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::trace::{Batcher, Description};
use differential_dataflow::{AsCollection, VecCollection};
use mz_repr::{Datum, Diff, Row};
// Only the fuzzing-gated `datum_seq_to_upsert_value` takes a `DatumSeq`.
#[cfg(feature = "fuzzing")]
use mz_row_spine::DatumSeq;
use mz_storage_types::errors::{DataflowError, UpsertError};
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::chunk::{ColumnChunk, StorageSpill};
use mz_timely_util::columnar::{Col2ValPagedBatcher, Column};
use timely::container::PushInto;
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Exchange as _;
use timely::dataflow::operators::generic::Operator;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::AntichainRef;
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use crate::statistics::SourceStatistics;
use crate::upsert::{UpsertKey, UpsertValue};

/// The paged flavor's persist-feedback batcher, wrapping
/// [`Col2ValPagedBatcher`] only to capture the storage upsert-stash pager at
/// construction.
///
/// `arrange_core` builds its batcher via [`Batcher::new`], which has no pager
/// hook, so a plain `Col2ValPagedBatcher` falls back to the process-global
/// (compute) pager, meaning the feedback arrangement's spill would be gated
/// by compute's `enable_column_paged_batcher_spill` rather than storage's
/// `enable_upsert_paged_spill`. Injecting `upsert_stash_pager::pager()` in
/// `new` puts the feedback arrangement under the same flag as the source
/// stash. Every other method delegates to the inner batcher unchanged.
pub(super) struct UpsertFeedbackBatcher<T: columnar::Columnar>(
    Col2ValPagedBatcher<UpsertKey, Row, T, Diff>,
);

impl<T> Batcher for UpsertFeedbackBatcher<T>
where
    T: Timestamp + columnar::Columnar + Default + PartialOrder,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
{
    type Output = Column<((UpsertKey, Row), T, Diff)>;
    type Time = T;

    fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        let mut batcher =
            <Col2ValPagedBatcher<UpsertKey, Row, T, Diff> as Batcher>::new(logger, operator_id);
        batcher.set_pager(crate::upsert::upsert_stash_pager::pager());
        Self(batcher)
    }

    fn seal(&mut self, upper: Antichain<T>) -> (Vec<Self::Output>, Description<T>) {
        self.0.seal(upper)
    }

    fn frontier(&mut self) -> AntichainRef<'_, T> {
        self.0.frontier()
    }
}

impl<T> PushInto<Column<((UpsertKey, Row), T, Diff)>> for UpsertFeedbackBatcher<T>
where
    T: Timestamp + columnar::Columnar + Default + PartialOrder,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
{
    fn push_into(&mut self, chunk: Column<((UpsertKey, Row), T, Diff)>) {
        self.0.push_into(chunk)
    }
}

/// One persist-feedback update: a key, its current value row, the time, and
/// an additive count.
pub(super) type FeedbackUpdate<T> = ((UpsertKey, Row), T, Diff);

/// One feedback-arrangement chunk: a sorted, consolidated run of updates,
/// resident or spilled to the buffer pool. Both batcher chains and sealed
/// spine batches are sequences of these, so the arrangement's state pages out
/// of RSS under the pool's budget, and the drain reads it back through the
/// bulk [`UnloadChunk`](mz_timely_util::columnar::unload::UnloadChunk)
/// surface: copy-out probes, no cursor borrows.
pub(super) type FeedbackChunk<T> = ColumnChunk<(UpsertKey, Row), T, Diff, StorageSpill>;

/// The feedback arrangement's trace: a funded spine of `Rc`-shared chunk
/// batches. See [`mz_timely_util::funded_spine`].
pub(super) type FeedbackSpine<T> =
    mz_timely_util::funded_spine::Spine<std::rc::Rc<ChunkBatch<FeedbackChunk<T>>>>;

// The persist-feedback arrangement stores `(UpsertKey, Row)` pairs in columnar
// chunk batches ([`FeedbackSpine`]). `UpsertValue` is
// `Result<Row, Box<UpsertError>>`, so we fold both arms into a single `Row`
// with a leading tag column so they share the value column.

/// Encode an [`UpsertValue`] as a `Row` with a leading tag column so both `Ok`
/// and `Err` payloads round-trip through `Row` byte storage.
///
/// Used on the render path. `pub` only so [`crate::fuzz_exports`] can re-export
/// it under the `fuzzing` feature for the storage fuzz crate. The enclosing
/// module is crate-private, so it is not otherwise reachable. Not a stable
/// public API.
#[doc(hidden)]
pub fn upsert_value_to_row(value: &UpsertValue) -> Row {
    let mut row = Row::default();
    let mut packer = row.packer();
    match value {
        Ok(ok) => {
            packer.push(Datum::UInt8(0));
            packer.extend(ok.iter());
        }
        Err(err) => {
            packer.push(Datum::UInt8(1));
            let bytes =
                bincode::serialize(err.as_ref()).expect("UpsertError is serializable via bincode");
            packer.push(Datum::Bytes(&bytes));
        }
    }
    row
}

/// Heap-size estimate for an emitted [`UpsertValue`], used to drive
/// `give_fueled` yielding on the output edge.
pub(super) fn upsert_value_byte_len(value: &UpsertValue) -> usize {
    match value {
        Ok(row) => row.byte_len(),
        Err(err) => std::mem::size_of_val(err.as_ref()),
    }
}

/// Decode an [`UpsertValue`] produced by [`upsert_value_to_row`] back from the
/// `DatumSeq` view returned by a `ValRowSpine` cursor.
///
/// Exists only for the storage fuzz crate, so it is gated behind the `fuzzing`
/// feature. Not a stable public API.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub fn datum_seq_to_upsert_value(seq: DatumSeq<'_>) -> UpsertValue {
    decode_upsert_value(seq)
}

/// Decode an [`UpsertValue`] produced by [`upsert_value_to_row`] from any
/// datum iterator, whether over a columnar `Row` reference or an owned `Row`.
pub(super) fn decode_upsert_value<'a>(mut iter: impl Iterator<Item = Datum<'a>>) -> UpsertValue {
    let tag = match iter.next() {
        Some(Datum::UInt8(tag)) => tag,
        other => panic!("upsert value missing UInt8 tag, got {:?}", other),
    };
    match tag {
        0 => {
            let mut row = Row::default();
            row.packer().extend(iter);
            Ok(row)
        }
        1 => {
            let bytes = match iter.next() {
                Some(Datum::Bytes(b)) => b,
                other => panic!("upsert error tag missing Bytes payload, got {:?}", other),
            };
            let err: UpsertError =
                bincode::deserialize(bytes).expect("UpsertError bincode round-trip");
            Err(Box::new(err))
        }
        tag => panic!("unknown upsert value tag {tag}"),
    }
}

/// Key the persist feedback by [`UpsertKey`], record source statistics, and
/// encode `(UpsertKey, UpsertValue)` as `(UpsertKey, Row)` `Column` chunks,
/// the input both flavors' feedback arrangements consume. Built with
/// `Pipeline` downstream of an `UpsertKey::hashed` exchange, so the
/// arrangement keeps that locality.
pub(super) fn encode_feedback<'scope, T>(
    persist_ok: VecCollection<'scope, T, Row, Diff>,
    persist_err: VecCollection<'scope, T, DataflowError, Diff>,
    key_indices: Vec<usize>,
    source_statistics: SourceStatistics,
) -> Stream<'scope, T, Column<((UpsertKey, Row), T, Diff)>>
where
    T: Timestamp + TotalOrder + Sync,
    T: Refines<mz_repr::Timestamp> + differential_dataflow::lattice::Lattice,
    T: columnation::Columnation,
    T: columnar::Columnar + Default,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
{
    let persist_keyed = crate::upsert::key_persist_feedback(persist_ok, persist_err, key_indices);
    let persist_keyed = persist_keyed
        .inner
        // The arrangement already implicitly exchanges by key, so this is redundant, but we want to
        // do it earlier so that we can inspect the stream properly for source statistics.
        .exchange(move |((key, _), _, _)| UpsertKey::hashed(key))
        .as_collection()
        .inspect(move |((_, row), _, diff)| {
            source_statistics.update_records_indexed_by(diff.into_inner());
            source_statistics.update_bytes_indexed_by(
                row.as_ref().map_or(0, |r| r.byte_len().try_into().unwrap()) * diff.into_inner(),
            );
        });
    persist_keyed
        .inner
        .unary::<ColumnBuilder<((UpsertKey, Row), T, Diff)>, _, _, _>(
            Pipeline,
            "Persist feedback encode",
            |_, _| {
                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session_with_builder(&time);
                        for ((key, value), ts, diff) in data.drain(..) {
                            let row = upsert_value_to_row(&value);
                            session.give(((&key, &row), &ts, &diff));
                        }
                    });
                }
            },
        )
}
