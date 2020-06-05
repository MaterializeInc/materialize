use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::FrontieredInputHandle;
use timely::dataflow::{Scope, Stream};
use timely::Data;

pub fn sink_reschedule<G: Scope, D, L, P>(s: &Stream<G, D>, pact: P, name: &str, mut logic: L)
where
    D: Data,
    L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D, P::Puller>) -> bool + 'static,
    P: ParallelizationContract<G::Timestamp, D>,
{
    let mut builder = OperatorBuilder::new(name.to_owned(), s.scope());
    let mut input = builder.new_input(s, pact);

    builder.build_reschedule(|_capabilities| {
        move |frontiers| {
            let mut input_handle = FrontieredInputHandle::new(&mut input, &frontiers[0]);
            logic(&mut input_handle)
        }
    });
}
