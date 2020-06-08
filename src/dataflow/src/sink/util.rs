// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::{FrontieredInputHandle, OperatorInfo};
use timely::dataflow::{Scope, Stream};
use timely::Data;

pub fn sink_reschedule<G: Scope, D, B, L, P>(s: &Stream<G, D>, pact: P, name: &str, constructor: B)
where
    D: Data,
    B: FnOnce(OperatorInfo) -> L,
    L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D, P::Puller>) -> bool + 'static,
    P: ParallelizationContract<G::Timestamp, D>,
{
    let mut builder = OperatorBuilder::new(name.to_owned(), s.scope());
    let operator_info = builder.operator_info();
    let mut input = builder.new_input(s, pact);

    builder.build_reschedule(|_capabilities| {
        let mut logic = constructor(operator_info);
        move |frontiers| {
            let mut input_handle = FrontieredInputHandle::new(&mut input, &frontiers[0]);
            logic(&mut input_handle)
        }
    });
}
