//! Dataflow operators over the columnar [`RecordedUpdates`](super::RecordedUpdates)
//! container — the collection-level surface of the columnar chunk.
//!
//! [`join_function`] is a columnar `flat_map` (subsuming map/filter/negate/
//! enter_at); [`leave_dynamic`] truncates dynamic-scope timestamps; and
//! [`as_recorded_updates`] extracts a `RecordedUpdates` collection from a
//! columnar arrangement's batch stream.

use crate::columnar::layout;
use crate::columnar::trace::Spine;
use super::{Builder, RecordedUpdates};

/// A columnar flat_map: iterates RecordedUpdates, calls logic per (key, val, time, diff),
/// joins output times with input times, multiplies output diffs with input diffs.
///
/// This subsumes map, filter, negate, and enter_at for columnar collections.
pub fn join_function<U, I, L>(
    input: crate::Collection<U::Time, RecordedUpdates<U>>,
    mut logic: L,
) -> crate::Collection<U::Time, RecordedUpdates<U>>
where
    U::Time: crate::lattice::Lattice,
    U: layout::ColumnarUpdate<Diff: crate::difference::Multiply<U::Diff, Output = U::Diff>>,
    I: IntoIterator<Item = (U::Key, U::Val, U::Time, U::Diff)>,
    L: FnMut(
        columnar::Ref<'_, U::Key>,
        columnar::Ref<'_, U::Val>,
        columnar::Ref<'_, U::Time>,
        columnar::Ref<'_, U::Diff>,
    ) -> I + 'static,
{
    use timely::dataflow::operators::generic::Operator;
    use timely::dataflow::channels::pact::Pipeline;
    use crate::AsCollection;
    use crate::difference::Multiply;
    use crate::lattice::Lattice;
    use columnar::Columnar;

    input
        .inner
        .unary::<Builder<U>, _, _, _>(Pipeline, "JoinFunction", move |_, _| {
            move |input, output| {
                let mut t1o = U::Time::default();
                let mut d1o = U::Diff::default();
                input.for_each(|time, data| {
                    let mut session = output.session_with_builder(&time);
                    for (k1, v1, t1, d1) in data.updates.view().iter() {
                        Columnar::copy_from(&mut t1o, t1);
                        Columnar::copy_from(&mut d1o, d1);
                        for (k2, v2, t2, d2) in logic(k1, v1, t1, d1) {
                            let t3 = t2.join(&t1o);
                            let d3 = d2.multiply(&d1o);
                            session.give((&k2, &v2, &t3, &d3));
                        }
                    }
                });
            }
        })
        .as_collection()
}

/// Timestamp shape of a dynamic iterative scope: an outer timestamp paired
/// with a per-level `PointStamp` of loop counters.
pub type DynTime<TOuter, T> = timely::order::Product<TOuter, crate::dynamic::pointstamp::PointStamp<T>>;

/// Leave a dynamic iterative scope, truncating PointStamp coordinates.
///
/// Uses OperatorBuilder (not unary) for the custom input connection summary
/// that tells timely how the PointStamp is affected (retain `level - 1` coordinates).
///
/// Consolidates after truncation since distinct PointStamp coordinates can collapse.
pub fn leave_dynamic<K, V, R, TOuter, T>(
    input: crate::Collection<DynTime<TOuter, T>, RecordedUpdates<(K, V, DynTime<TOuter, T>, R)>>,
    level: usize,
) -> crate::Collection<DynTime<TOuter, T>, RecordedUpdates<(K, V, DynTime<TOuter, T>, R)>>
where
    K: columnar::Columnar,
    V: columnar::Columnar,
    R: columnar::Columnar,
    TOuter: timely::progress::Timestamp + Default + columnar::Columnar,
    T: timely::progress::Timestamp + Default + columnar::Columnar,
    (K, V, DynTime<TOuter, T>, R): layout::ColumnarUpdate<Key = K, Val = V, Time = DynTime<TOuter, T>, Diff = R>,
{
    assert!(level > 0, "leave_dynamic requires level > 0");
    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
    use timely::dataflow::operators::generic::OutputBuilder;
    use timely::order::Product;
    use timely::progress::Antichain;
    use timely::container::{ContainerBuilder, PushInto};
    use crate::AsCollection;
    use crate::dynamic::pointstamp::{PointStamp, PointStampSummary};
    use columnar::Columnar;

    let mut builder = OperatorBuilder::new("LeaveDynamic".to_string(), input.inner.scope());
    let (output, stream) = builder.new_output();
    let mut output = OutputBuilder::from(output);
    let mut op_input = builder.new_input_connection(
        input.inner,
        Pipeline,
        [(
            0,
            Antichain::from_elem(Product {
                outer: Default::default(),
                inner: PointStampSummary {
                    retain: Some(level - 1),
                    actions: Vec::new(),
                },
            }),
        )],
    );

    builder.build(move |_capability| {
        let mut col_builder = Builder::<(K, V, DynTime<TOuter, T>, R)>::default();
        let mut time = DynTime::<TOuter, T>::default();
        move |_frontier| {
            let mut output = output.activate();
            op_input.for_each(|cap, data| {
                // Truncate the capability's timestamp.
                let mut new_time = cap.time().clone();
                let mut vec = std::mem::take(&mut new_time.inner).into_inner();
                vec.truncate(level - 1);
                new_time.inner = PointStamp::new(vec);
                let new_cap = cap.delayed(&new_time, 0);
                // Push updates with truncated times into the builder.
                // The builder's form call on flush sorts and consolidates,
                // handling the duplicate times that truncation can produce.
                // TODO: The input trie is already sorted; a streaming form
                // that accepts pre-sorted, potentially-collapsing timestamps
                // could avoid the re-sort inside the builder.
                for (k, v, t, d) in data.updates.view().iter() {
                    Columnar::copy_from(&mut time, t);
                    let mut inner_vec = std::mem::take(&mut time.inner).into_inner();
                    inner_vec.truncate(level - 1);
                    time.inner = PointStamp::new(inner_vec);
                    col_builder.push_into((k, v, &time, d));
                }
                let mut session = output.session(&new_cap);
                while let Some(container) = col_builder.finish() {
                    session.give_container(container);
                }
            });
        }
    });

    stream.as_collection()
}

/// Extract a `Collection<_, RecordedUpdates<U>>` from a columnar `Arranged`.
///
/// Cursors through each batch and pushes `(key, val, time, diff)` refs into
/// a `Builder`, which sorts and consolidates on flush.
pub fn as_recorded_updates<U>(
    arranged: crate::operators::arrange::Arranged<
        crate::operators::arrange::TraceAgent<Spine<U::Key, U::Val, U::Time, U::Diff>>,
    >,
) -> crate::Collection<U::Time, RecordedUpdates<U>>
where
    U: layout::ColumnarUpdate,
{
    use timely::dataflow::operators::generic::Operator;
    use timely::dataflow::channels::pact::Pipeline;
    use crate::trace::{Navigable, Cursor};
    use crate::AsCollection;

    arranged.stream
        .unary::<Builder<U>, _, _, _>(Pipeline, "AsRecordedUpdates", |_, _| {
            move |input, output| {
                input.for_each(|time, batches| {
                    let mut session = output.session_with_builder(&time);
                    for batch in batches.drain(..) {
                        let mut cursor = batch.cursor();
                        while cursor.key_valid(&batch) {
                            while cursor.val_valid(&batch) {
                                let key = cursor.key(&batch);
                                let val = cursor.val(&batch);
                                cursor.map_times(&batch, |time, diff| {
                                    session.give((key, val, time, diff));
                                });
                                cursor.step_val(&batch);
                            }
                            cursor.step_key(&batch);
                        }
                    }
                });
            }
        })
        .as_collection()
}
