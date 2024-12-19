// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to the creation of dataflow raw sources.
//!
//! Raw sources are streams (currently, Timely streams) of data directly
//! produced by the upstream service. The main export of this module is
//! [`create_raw_source`], which turns
//! [`RawSourceCreationConfigs`](RawSourceCreationConfig) and
//! [`SourceConnections`](mz_storage_types::sources::SourceConnection)
//! implementations into the aforementioned streams.
//!
//! The full source, which is the _differential_ stream that represents the
//! actual object created by a `CREATE SOURCE` statement, is created by
//! composing [`create_raw_source`] with decoding,
//! [`SourceEnvelope`](mz_storage_types::sources::SourceEnvelope) rendering, and
//! more.

// https://github.com/tokio-rs/prost/issues/237
#![allow(missing_docs)]

use crate::source::types::SourceMessage;

pub mod generator;
mod kafka;
mod mysql;
mod postgres;
pub(crate) mod reclock;
mod source_reader_pipeline;
mod statistics;
pub mod types;

pub use kafka::KafkaSourceReader;
pub use source_reader_pipeline::{
    create_raw_source, RawSourceCreationConfig, SourceExportCreationConfig,
};
use mz_ore::cast::CastFrom;
use timely::container::{PushInto, SizableContainer};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::{Scope, StreamCore};
use timely::Container;

/// Partition a stream of records into multiple streams.
pub trait PartitionCore<G: Scope, C: Container> {
    /// Produces `parts` output streams, containing records produced and assigned by `route`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Partition, Inspect};
    ///
    /// timely::example(|scope| {
    ///     let streams = (0..10).to_stream(scope)
    ///                          .partition(3, |x| (x % 3, x));
    ///
    ///     streams[0].inspect(|x| println!("seen 0: {:?}", x));
    ///     streams[1].inspect(|x| println!("seen 1: {:?}", x));
    ///     streams[2].inspect(|x| println!("seen 2: {:?}", x));
    /// });
    /// ```
    fn partition_core<C2, F, D2>(&self, parts: u64, route: F) -> Vec<StreamCore<G, C2>>
    where
        C2: SizableContainer + PushInto<D2>,
        F: FnMut(C::Item<'_>) -> (usize, D2) + 'static;
}

impl<G: Scope, C: Container> PartitionCore<G, C> for StreamCore<G, C> {
    fn partition_core<C2, F, D2>(&self, parts: u64, mut route: F) -> Vec<StreamCore<G, C2>>
    where
        C2: SizableContainer + PushInto<D2>,
        F: FnMut(C::Item<'_>) -> (usize, D2) + 'static,
    {
        let mut builder = OperatorBuilder::new("Partition".to_owned(), self.scope());

        let mut input = builder.new_input(self, Pipeline);
        let mut outputs = Vec::with_capacity(usize::cast_from(parts));
        let mut streams = Vec::with_capacity(usize::cast_from(parts));

        for _ in 0..parts {
            let (output, stream) = builder.new_output();
            outputs.push(output);
            streams.push(stream);
        }

        builder.build(move |_| {
            move |_frontiers| {
                let mut handles = outputs.iter_mut().map(|o| o.activate()).collect::<Vec<_>>();
                input.for_each(|time, data| {
                    let mut sessions = handles
                        .iter_mut()
                        .map(|h| h.session(&time))
                        .collect::<Vec<_>>();

                    for datum in data.drain() {
                        let (part, datum2) = route(datum);
                        sessions[part].give(datum2);
                    }
                });
            }
        });

        streams
    }
}
