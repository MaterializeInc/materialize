use std::collections::HashMap;

use timely::dataflow::Scope;
use differential_dataflow::Collection;

use crate::repr;
use crate::repr::Datum;
use crate::dataflow2::types::{RelationExpr, ScalarExpr, AggregateExpr};


pub fn render<G>(
    plan: RelationExpr,
    scope: &mut G,
    context: &HashMap<String, Collection<G, Vec<Datum>, isize>>,
) -> Collection<G, Vec<Datum>, isize>
where
    G: Scope,
    G::Timestamp: differential_dataflow::lattice::Lattice,
{

    match plan {

        RelationExpr::Get{ name, typ } => {
            context.get(&name).expect("failed to find source").clone()
        },
        RelationExpr::Let{ name, value, body } => {
            let value = render(*value, scope, context);
            let mut new_context = context.clone();
            new_context.insert(name, value);
            render(*body, scope, &new_context)
        },
        RelationExpr::Project{ input, outputs } => {
            let input = render(*input, scope, context);
            input.map(move |tuple| outputs.iter().map(|i| tuple[*i].clone()).collect())
        },
        RelationExpr::Map{ input, scalars } => {
            let input = render(*input, scope, context);
            // obvious lifetime/borrow issues here.
            input.map(move |mut tuple| {
                tuple.extend(scalars.iter().map(|s| {
                    // s.apply_to(tuple)
                    unimplemented!()
                }));
                tuple
            })
        },
        RelationExpr::Filter{ input, predicate } => {
            let input = render(*input, scope, context);
            input.filter(move |x| {
                // predicate.apply(x).as_bool()
                unimplemented!()
            })
        },
        RelationExpr::Join{ inputs, variables } => {
            // Much work.
            unimplemented!()
        },
        RelationExpr::Reduce{ input, group_key, aggregates } => {
            use differential_dataflow::operators::Reduce;
            let input = render(*input, scope, context);
            // input.map(move |tuple|
            //     (
            //         group_key.iter().map(|i| tuple[*i].clone()).collect::<Vec<_>>(),
            //         tuple,
            //     ))
            //     .reduce(|_key, source, target| {
            //         // not sure how to apply AggregateExpr
            //         unimplemented!()
            //     });

            unimplemented!()
        },

        RelationExpr::OrDefault{ input, default } => {

            use timely::dataflow::operators::to_stream::ToStream;
            use differential_dataflow::collection::AsCollection;
            use differential_dataflow::operators::{Join, Reduce};
            use differential_dataflow::operators::reduce::Threshold;

            let input = render(*input, scope, context);
            let present = input.map(|_| ()).distinct();
            let default =
            vec![(((), default), Default::default(), 1isize)]
                .to_stream(scope)
                .as_collection()
                .antijoin(&present)
                .map(|((),default)| default);

            input.concat(&default)
        },
        RelationExpr::Negate{ input } => {
            let input = render(*input, scope, context);
            input.negate()
        },
        RelationExpr::Distinct{ input } => {
            use differential_dataflow::operators::reduce::Threshold;
            let input = render(*input, scope, context);
            input.distinct()
        },
        RelationExpr::Union{ left, right } => {
            let input1 = render(*left, scope, context);
            let input2 = render(*right, scope, context);
            input1.concat(&input2)
        },

    }

}

// fn optimize(relation: RelationExpr, metadata: RelationType) -> (RelationExpr, RelationType) {

//     if let RelationExpr::Join(inputs, variables)

// }
