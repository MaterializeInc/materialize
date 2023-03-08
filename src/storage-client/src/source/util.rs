// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::rc::Rc;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::{OperatorInfo, OutputHandle};
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::{Scope, Stream};
use timely::progress::frontier::AntichainRef;
use timely::scheduling::ActivateOnDrop;
use timely::Data;

use mz_ore::collections::CollectionExt;
use mz_repr::Timestamp;

use crate::types::sources::SourceToken;

use arrayvec::ArrayVec;
use bytes::BufMut;
use differential_dataflow::lattice::{Lattice, Maximum};
use mz_persist_types::columnar::{DataType, PartDecoder, PartEncoder, Schema};
use mz_persist_types::part::{ColumnsMut, ColumnsRef};
use mz_persist_types::Codec;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use timely::order::TotalOrder;
use timely::progress::{PathSummary, Timestamp as TimelyTimestamp};
use timely::PartialOrder;

/// Constructs a source named `name` in `scope` whose lifetime is controlled
/// both internally and externally.
///
/// The logic for the source is supplied by `construct`, which must return a
/// `tick` function that satisfies `L`. This function will be called
/// periodically while the source is alive and supplied with a capability to
/// produce data and the output handle into which data should be given. The
/// `tick` function is responsible for periodically downgrading this capability
/// whenever it can see that a timestamp is "closed", according to whatever
/// logic makes sense for the source.
///
/// If `tick` realizes it will never produce data again, it should indicate that
/// fact by downgrading the given `CapabilitySet` to the empty frontier before
/// returning. This will guarantee that `tick` is never called again.
///
/// It is `tick`'s responsibility to inform Timely of its desire to be scheduled
/// again by chatting with a [`timely::scheduling::activate::Activator`].
/// Holding on to capabilities using the `CapabilitySet` does not alone cause
/// the source to be scheduled again; it merely keeps the source alive.
///
/// The lifetime of the source is also controlled by the returned
/// [`SourceToken`]. When the last clone of the `SourceToken` is dropped, the
/// `tick` function will no longer be called, and the capability will eventually
/// be dropped.
///
/// When the source token is dropped, the timestamping_flag is set to false
/// to terminate any spawned threads in the source operator
pub fn source<G, D, B, L>(
    scope: &G,
    name: String,
    flow_control_input: &Stream<G, ()>,
    construct: B,
) -> (Stream<G, D>, SourceToken)
where
    G: Scope<Timestamp = Timestamp>,
    D: Data,
    B: FnOnce(OperatorInfo) -> L,
    L: FnMut(
            &mut CapabilitySet<Timestamp>,
            AntichainRef<G::Timestamp>,
            &mut OutputHandle<G::Timestamp, D, Tee<G::Timestamp, D>>,
        ) -> ()
        + 'static,
{
    let mut token = None;

    let mut builder = OperatorBuilder::new(name, scope.clone());
    let operator_info = builder.operator_info();

    let (mut data_output, data_stream) = builder.new_output();

    let _flow_control_handle = builder.new_input(flow_control_input, Pipeline);

    builder.build(|capabilities| {
        let cap_set = CapabilitySet::from_elem(capabilities.into_element());

        let drop_activator = Rc::new(ActivateOnDrop::new(
            (),
            Rc::new(operator_info.address.clone()),
            scope.activations(),
        ));
        let drop_activator_weak = Rc::downgrade(&drop_activator);

        // Export a token to the outside word that will keep this source alive.
        token = Some(SourceToken {
            _activator: drop_activator,
        });

        let tick = construct(operator_info);
        let mut cap_and_tick = Some((cap_set, tick));

        move |frontiers| {
            // Drop all capabilities if `token` is dropped.
            if drop_activator_weak.upgrade().is_none() {
                // Drop the tick closure, too, in case dropping anything it owns
                // (such as a MutexGuard) is important.
                //
                // TODO: This assumes that `tick` is "cancel safe" (in async
                // lingo). Perhaps we want a more graceful shutdown protocol
                // instead/in addition.
                cap_and_tick = None;
            }
            if let Some((cap, tick)) = &mut cap_and_tick {
                // We still have our capability, so the source is still alive.
                // Delegate to the inner source.
                let flow_control_frontier = frontiers[0].frontier();
                tick(cap, flow_control_frontier, &mut data_output.activate());
                if cap.is_empty() {
                    // The inner source is finished. Drop our capability.
                    cap_and_tick = None;
                }
            }
        }
    });

    // `build()` promises to call the provided closure before returning,
    // so we are guaranteed that `token` is non-None.
    (data_stream, token.unwrap())
}

/// A key along with its serialized representation. Used to both deserialize the data and keep
/// around the raw bytes for comparison.
#[derive(Debug)]
pub struct WithPrefix<K>(pub K, pub ArrayVec<u8, 16>);

#[derive(Debug)]
pub struct Prefixed<S>(pub S);

impl<K: Codec> Codec for WithPrefix<K> {
    type Schema = Prefixed<Arc<K::Schema>>;

    fn codec_name() -> String {
        K::codec_name()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        self.0.encode(buf);
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        let len = buf.len().min(16);
        let mut vec = ArrayVec::new();
        vec.try_extend_from_slice(&buf[..len]).unwrap();
        Ok(WithPrefix(K::decode(buf)?, vec))
    }
}

impl<'a, T, D: PartDecoder<'a, T>> PartDecoder<'a, WithPrefix<T>> for Prefixed<D> {
    fn decode(&self, idx: usize, val: &mut WithPrefix<T>) {
        // Since this interface doesn't provide a meaningful sort key, default to the empty key.
        // TODO: revisit the sort key type when we switch interfaces.
        val.1.clear();
        self.0.decode(idx, &mut val.0)
    }
}

impl<'a, T, D: PartEncoder<'a, T>> PartEncoder<'a, WithPrefix<T>> for Prefixed<D> {
    fn encode(&mut self, val: &WithPrefix<T>) {
        self.0.encode(&val.0)
    }
}

impl<K: Codec> Schema<WithPrefix<K>> for Prefixed<Arc<K::Schema>> {
    type Encoder<'a> = Prefixed<<K::Schema as Schema<K>>::Encoder<'a>>;
    type Decoder<'a> = Prefixed<<K::Schema as Schema<K>>::Decoder<'a>>;

    fn columns(&self) -> Vec<(String, DataType)> {
        self.0.columns()
    }

    fn decoder<'a>(&self, cols: ColumnsRef<'a>) -> Result<Self::Decoder<'a>, String> {
        self.0.decoder(cols).map(Prefixed)
    }

    fn encoder<'a>(&self, cols: ColumnsMut<'a>) -> Result<Self::Encoder<'a>, String> {
        self.0.encoder(cols).map(Prefixed)
    }
}

/// A timestamp based on the sort order of our data in Persist, intended for use as the
/// inner timestamp of a Hybrid timestamp type.
#[derive(PartialOrd, PartialEq, Ord, Eq, Debug, Clone, Serialize, Deserialize, Hash)]
pub enum Sort {
    /// An actual key. Meaningless aside from the sort.
    Data(ArrayVec<u8, 16>),
    /// A key that compares larger than any other sort key.
    Max, // Here for the lattice implementation; otherwise unused.
}

impl PartialOrder for Sort {
    fn less_equal(&self, other: &Self) -> bool {
        self <= other
    }
}

impl TotalOrder for Sort {}

impl Lattice for Sort {
    fn join(&self, other: &Self) -> Self {
        self.max(other).clone()
    }

    fn meet(&self, other: &Self) -> Self {
        self.min(other).clone()
    }
}

impl TimelyTimestamp for Sort {
    type Summary = ();

    fn minimum() -> Self {
        Sort::Data(ArrayVec::new())
    }
}

impl Maximum for Sort {
    fn maximum() -> Self {
        Sort::Max
    }
}

impl PathSummary<Sort> for () {
    fn results_in(&self, src: &Sort) -> Option<Sort> {
        Some(src.clone())
    }

    fn followed_by(&self, (): &Self) -> Option<Self> {
        Some(())
    }
}
