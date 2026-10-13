//! Show an iterative scope example that uses a wrapper type around a container

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::order::Product;
use timely::dataflow::Stream;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use differential_dataflow::{AsCollection, Collection};
use differential_dataflow::input::Input;
use differential_dataflow::operators::iterate::Variable;
use differential_dataflow::collection::containers::{Enter, Leave, Negate, ResultsIn};

/// A wrapper around a container that implements the necessary traits to be used in iterative scopes.
#[derive(Clone, Default)]
struct ContainerWrapper<C>(C);

impl<C: timely::container::Accountable> timely::container::Accountable for ContainerWrapper<C> {
    #[inline(always)] fn record_count(&self) -> i64 { self.0.record_count() }
    #[inline(always)] fn is_empty(&self) -> bool { self.0.is_empty() }
}
impl<C: Enter<T1, T2>, T1, T2> Enter<T1, T2> for ContainerWrapper<C> {
    type InnerContainer = ContainerWrapper<C::InnerContainer>;
    #[inline(always)] fn enter(self) -> Self::InnerContainer { ContainerWrapper(self.0.enter()) }
}
impl<C: Leave<T1, T2>, T1, T2> Leave<T1, T2> for ContainerWrapper<C> {
    type OuterContainer = ContainerWrapper<C::OuterContainer>;
    #[inline(always)] fn leave(self) -> Self::OuterContainer { ContainerWrapper(self.0.leave()) }
}
impl<C: Negate> Negate for ContainerWrapper<C> {
    #[inline(always)] fn negate(self) -> Self { ContainerWrapper(self.0.negate()) }
}
impl<C: ResultsIn<TS>, TS> ResultsIn<TS> for ContainerWrapper<C> {
    #[inline(always)] fn results_in(self, step: &TS) -> Self { ContainerWrapper(self.0.results_in(step)) }
}

fn wrap<T: timely::progress::Timestamp, C: timely::Container>(stream: Stream<T, C>) -> Stream<T, ContainerWrapper<C>> {
    let mut builder = OperatorBuilder::new("Wrap".to_string(), stream.scope());
    let (mut output, stream_out) = builder.new_output();
    let mut input = builder.new_input(stream, Pipeline);
    builder.build(move |_capability| move |_frontier| {
        let mut output = output.activate();
        input.for_each(|time, data| {
            output.give(&time, &mut ContainerWrapper(std::mem::take(data)));
        });
    });
    stream_out
}


fn main() {
    timely::example(|scope| {

        let numbers = scope.new_collection_from(1 .. 10u32).1;
        let numbers: Collection<_, _>  = wrap(numbers.inner).as_collection();

        scope.iterative::<u64,_,_>(|nested| {
            let summary = Product::new(Default::default(), 1);
            let (variable, collection) = Variable::new_from(numbers.enter(nested), summary);
            let mapped: Collection<_, _> = collection.clone().inner.unary(Pipeline, "Map", |_,_| {
                |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session(&time);
                        for (x, _t, _d) in data.0.iter_mut() {
                            *x = if *x % 2 == 0 { *x/2 } else { *x };
                        }
                        session.give_container(data);
                    });
                }
            }).as_collection();
            let result = mapped.inner.unary(Pipeline, "Unwrap", |_,_| {
                |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session(&time);
                        session.give_container(&mut data.0);
                    });
                }
            }).as_collection().consolidate();
            let result = wrap(result.inner).as_collection();
            variable.set(result);
            collection.leave(scope)
        });
    })
}
