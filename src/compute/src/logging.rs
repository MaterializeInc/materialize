// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging dataflows for events generated by various subsystems.

pub mod compute;
mod differential;
pub(super) mod initialize;
mod reachability;
mod timely;

use std::any::Any;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

use ::timely::container::{CapacityContainerBuilder, ContainerBuilder};
use ::timely::dataflow::channels::pact::Pipeline;
use ::timely::dataflow::channels::pushers::buffer::Session;
use ::timely::dataflow::channels::pushers::{Counter, Tee};
use ::timely::dataflow::operators::capture::{Event, EventLink, EventPusher};
use ::timely::dataflow::operators::Operator;
use ::timely::dataflow::StreamCore;
use ::timely::progress::Timestamp as TimelyTimestamp;
use ::timely::scheduling::Activator;
use ::timely::Container;
use differential_dataflow::trace::Batcher;
use mz_compute_client::logging::{ComputeLog, DifferentialLog, LogVariant, TimelyLog};
use mz_expr::{permutation_for_arrangement, MirScalarExpr};
use mz_repr::{Datum, Diff, Row, RowPacker, RowRef, Timestamp};
use mz_timely_util::activator::RcActivator;
use mz_timely_util::containers::ColumnBuilder;
use mz_timely_util::operator::consolidate_pact;

use crate::typedefs::RowRowAgent;

pub use crate::logging::initialize::initialize;

/// An update of value `D` at a time and with a diff.
pub(super) type Update<D> = (D, Timestamp, Diff);
/// A pusher for containers `C`.
pub(super) type Pusher<C> = Counter<Timestamp, C, Tee<Timestamp, C>>;
/// An output session for the specified container builder.
pub(super) type OutputSession<'a, CB> =
    Session<'a, Timestamp, CB, Pusher<<CB as ContainerBuilder>::Container>>;
/// An output session for vector-based containers of updates `D`, using a capacity container builder.
pub(super) type OutputSessionVec<'a, D> = OutputSession<'a, CapacityContainerBuilder<Vec<D>>>;
/// An output session for columnar containers of updates `D`, using a column builder.
pub(super) type OutputSessionColumnar<'a, D> = OutputSession<'a, ColumnBuilder<D>>;

/// Logs events as a timely stream, with progress statements.
struct BatchLogger<C, P>
where
    P: EventPusher<Timestamp, C>,
{
    /// Time in milliseconds of the current expressed capability.
    time_ms: Timestamp,
    /// Pushes events to the logging dataflow.
    event_pusher: P,
    /// Each time is advanced to the strictly next millisecond that is a multiple of this interval.
    /// This means we should be able to perform the same action on timestamp capabilities, and only
    /// flush buffers when this timestamp advances.
    interval_ms: u128,
    _marker: PhantomData<C>,
}

impl<C, P> BatchLogger<C, P>
where
    P: EventPusher<Timestamp, C>,
{
    /// Creates a new batch logger.
    fn new(event_pusher: P, interval_ms: u128) -> Self {
        BatchLogger {
            time_ms: Timestamp::minimum(),
            event_pusher,
            interval_ms,
            _marker: PhantomData,
        }
    }
}

impl<C, P> BatchLogger<C, P>
where
    P: EventPusher<Timestamp, C>,
    C: Container,
{
    /// Publishes a batch of logged events.
    fn publish_batch(&mut self, data: C) {
        self.event_pusher.push(Event::Messages(self.time_ms, data));
    }

    /// Indicate progress up to `time`, advances the capability.
    ///
    /// Returns `true` if the capability was advanced.
    fn report_progress(&mut self, time: Duration) -> bool {
        let time_ms = ((time.as_millis() / self.interval_ms) + 1) * self.interval_ms;
        let new_time_ms: Timestamp = time_ms.try_into().expect("must fit");
        if self.time_ms < new_time_ms {
            self.event_pusher
                .push(Event::Progress(vec![(new_time_ms, 1), (self.time_ms, -1)]));
            self.time_ms = new_time_ms;
            true
        } else {
            false
        }
    }
}

impl<C, P> Drop for BatchLogger<C, P>
where
    P: EventPusher<Timestamp, C>,
{
    fn drop(&mut self) {
        self.event_pusher
            .push(Event::Progress(vec![(self.time_ms, -1)]));
    }
}

/// Parts to connect a logging dataflows the timely runtime.
///
/// This is just a bundle-type intended to make passing around its contents in the logging
/// initialization code more convenient.
///
/// The `N` type parameter specifies the number of links to create for the event queue. We need
/// separate links for queues that feed from multiple loggers because the `EventLink` type is not
/// multi-producer safe (it is a linked-list, and multiple writers would blindly append, replacing
/// existing new data, and cutting off other writers).
#[derive(Clone)]
struct EventQueue<C, const N: usize = 1> {
    links: [Rc<EventLink<Timestamp, C>>; N],
    activator: RcActivator,
}

impl<C, const N: usize> EventQueue<C, N> {
    fn new(name: &str) -> Self {
        let activator_name = format!("{name}_activator");
        let activate_after = 128;
        Self {
            links: [(); N].map(|_| Rc::new(EventLink::new())),
            activator: RcActivator::new(activator_name, activate_after),
        }
    }
}

/// State shared between different logging dataflow fragments.
#[derive(Default)]
struct SharedLoggingState {
    /// Activators for arrangement heap size operators.
    arrangement_size_activators: BTreeMap<usize, Activator>,
}

/// Helper to pack collections of [`Datum`]s into key and value row.
pub(crate) struct PermutedRowPacker {
    key: Vec<usize>,
    value: Vec<usize>,
    key_row: Row,
    value_row: Row,
}

impl PermutedRowPacker {
    /// Construct based on the information within the log variant.
    pub(crate) fn new<V: Into<LogVariant>>(variant: V) -> Self {
        let variant = variant.into();
        let key = variant.index_by();
        let (_, value) = permutation_for_arrangement(
            &key.iter()
                .cloned()
                .map(MirScalarExpr::Column)
                .collect::<Vec<_>>(),
            variant.desc().arity(),
        );
        Self {
            key,
            value,
            key_row: Row::default(),
            value_row: Row::default(),
        }
    }

    /// Pack a slice of datums suitable for the key columns in the log variant.
    pub(crate) fn pack_slice(&mut self, datums: &[Datum]) -> (&RowRef, &RowRef) {
        self.pack_by_index(|packer, index| packer.push(datums[index]))
    }

    /// Pack a slice of datums suitable for the key columns in the log variant, returning owned
    /// rows.
    ///
    /// This is equivalent to calling [`PermutedRowPacker::pack_slice`] and then calling `to_owned`
    /// on the returned rows.
    pub(crate) fn pack_slice_owned(&mut self, datums: &[Datum]) -> (Row, Row) {
        let (key, value) = self.pack_slice(datums);
        (key.to_owned(), value.to_owned())
    }

    /// Pack using a callback suitable for the key columns in the log variant.
    pub(crate) fn pack_by_index<F: Fn(&mut RowPacker, usize)>(
        &mut self,
        logic: F,
    ) -> (&RowRef, &RowRef) {
        let mut packer = self.key_row.packer();
        for index in &self.key {
            logic(&mut packer, *index);
        }

        let mut packer = self.value_row.packer();
        for index in &self.value {
            logic(&mut packer, *index);
        }

        (&self.key_row, &self.value_row)
    }
}

/// Information about a collection exported from a logging dataflow.
struct LogCollection {
    /// Trace handle providing access to the logged records.
    trace: RowRowAgent<Timestamp, Diff>,
    /// Token that should be dropped to drop this collection.
    token: Rc<dyn Any>,
}

/// A single-purpose function to consolidate and pack updates for log collection.
///
/// The function first consolidates worker-local updates using the [`Pipeline`] pact, then converts
/// the updates into `(Row, Row)` pairs using the provided logic function. It is crucial that the
/// data is not exchanged between workers, as the consolidation would not function as desired
/// otherwise.
pub(super) fn consolidate_and_pack<G, B, CB, L, F>(
    input: &StreamCore<G, B::Input>,
    log: L,
    mut logic: F,
) -> StreamCore<G, CB::Container>
where
    G: ::timely::dataflow::Scope<Timestamp = Timestamp>,
    B: Batcher<Time = G::Timestamp> + 'static,
    B::Input: Container + Clone + 'static,
    B::Output: Container + Clone + 'static,
    CB: ContainerBuilder,
    L: Into<LogVariant>,
    F: for<'a> FnMut(
            <B::Output as Container>::ItemRef<'a>,
            &mut PermutedRowPacker,
            &mut OutputSession<CB>,
        ) + 'static,
{
    let log = log.into();
    // TODO: Use something other than the debug representation of the log variant as a name.
    let c_name = &format!("Consolidate {log:?}");
    let u_name = &format!("ToRow {log:?}");
    let mut packer = PermutedRowPacker::new(log);
    let consolidated = consolidate_pact::<B, _, _>(input, Pipeline, c_name);
    consolidated.unary::<CB, _, _, _>(Pipeline, u_name, |_, _| {
        move |input, output| {
            while let Some((time, data)) = input.next() {
                let mut session = output.session_with_builder(&time);
                for item in data.iter().flatten().flat_map(|chunk| chunk.iter()) {
                    logic(item, &mut packer, &mut session);
                }
            }
        }
    })
}
