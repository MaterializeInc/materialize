// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::collections::HashMap;

use differential_dataflow::Collection;
use timely::dataflow::Scope;

use crate::dataflow2::types::{AggregateExpr, RelationExpr, ScalarExpr};
use crate::repr;
use crate::repr::Datum;

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
        RelationExpr::Constant { .. } => unimplemented!(),
        RelationExpr::Get { name, typ } => {
            context.get(&name).expect("failed to find source").clone()
        }
        RelationExpr::Let { name, value, body } => {
            let value = render(*value, scope, context);
            let mut new_context = context.clone();
            new_context.insert(name, value);
            render(*body, scope, &new_context)
        }
        RelationExpr::Project { input, outputs } => {
            let input = render(*input, scope, context);
            input.map(move |tuple| outputs.iter().map(|i| tuple[*i].clone()).collect())
        }
        RelationExpr::Map { input, scalars } => {
            let input = render(*input, scope, context);
            input.map(move |mut tuple| {
                let len = tuple.len();
                for s in scalars.iter() {
                    let to_push = s.0.eval_on(&tuple[..len]);
                    tuple.push(to_push);
                }
                tuple
            })
        }
        RelationExpr::Filter { input, predicates } => {
            let input = render(*input, scope, context);
            input.filter(move |x| {
                predicates
                    .iter()
                    .all(|predicate| match predicate.eval_on(x) {
                        Datum::True => true,
                        Datum::False => false,
                        _ => unreachable!(),
                    })
            })
        }
        RelationExpr::Join { inputs, variables } => {
            // Much work.
            unimplemented!()
        }
        RelationExpr::Reduce {
            input,
            group_key,
            aggregates,
        } => {
            use differential_dataflow::operators::Reduce;
            let input = render(*input, scope, context);
            input
                .map(move |tuple| {
                    (
                        group_key
                            .iter()
                            .map(|i| tuple[*i].clone())
                            .collect::<Vec<_>>(),
                        tuple,
                    )
                })
                .reduce(move |_key, source, target| {
                    let mut result = Vec::with_capacity(aggregates.len());
                    for (idx, (agg, typ)) in aggregates.iter().enumerate() {
                        let iter = source.iter().flat_map(|(v, w)| {
                            std::iter::repeat(v[idx].clone()).take(std::cmp::max(*w, 0) as usize)
                        });
                        result.push((agg.func.func())(iter));
                    }
                    target.push((result, 1));
                })
                .map(|(mut key, agg)| {
                    key.extend(agg.into_iter());
                    key
                })
        }

        RelationExpr::OrDefault { input, default } => {
            use differential_dataflow::collection::AsCollection;
            use differential_dataflow::operators::reduce::Threshold;
            use differential_dataflow::operators::{Join, Reduce};
            use timely::dataflow::operators::to_stream::ToStream;

            let input = render(*input, scope, context);
            let present = input.map(|_| ()).distinct();
            let default = vec![(((), default), Default::default(), 1isize)]
                .to_stream(scope)
                .as_collection()
                .antijoin(&present)
                .map(|((), default)| default);

            input.concat(&default)
        }
        RelationExpr::Negate { input } => {
            let input = render(*input, scope, context);
            input.negate()
        }
        RelationExpr::Distinct { input } => {
            use differential_dataflow::operators::reduce::Threshold;
            let input = render(*input, scope, context);
            input.distinct()
        }
        RelationExpr::Union { left, right } => {
            let input1 = render(*left, scope, context);
            let input2 = render(*right, scope, context);
            input1.concat(&input2)
        }
    }
}

// fn optimize(relation: RelationExpr, metadata: RelationType) -> (RelationExpr, RelationType) {

//     if let RelationExpr::Join(inputs, variables)

// }
