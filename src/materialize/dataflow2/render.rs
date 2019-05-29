// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::collections::{HashMap, HashSet};

use differential_dataflow::Collection;
use timely::dataflow::Scope;

use crate::dataflow2::types::{AggregateExpr, RelationExpr, RelationType, ScalarExpr};
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
        RelationExpr::Join {
            inputs,
            arities,
            variables,
        } => {
            use differential_dataflow::operators::join::Join;

            // For the moment, assert that each relation participates at most
            // once in each equivalence class. If not, we should be able to
            // push a filter upwards, and if we can't do that it means a bit
            // more filter logic in this operator which doesn't exist yet.
            assert!(variables.iter().all(|h| {
                let len = h.len();
                let mut list = h.iter().map(|(i, _)| i).collect::<Vec<_>>();
                list.sort();
                list.dedup();
                len == list.len()
            }));

            // The plan is to implement join as a `fold` over `inputs`.
            let mut input_iter = inputs.into_iter().enumerate();
            if let Some((index, input)) = input_iter.next() {
                let mut joined = render(input, scope, context);

                // Maintain sources of each in-progress column.
                let mut columns = (0..arities[index]).map(|c| (index, c)).collect::<Vec<_>>();

                // The intent is to maintain `joined` as the full cross
                // product of all input relations so far, subject to all
                // of the equality constraints in `variables`. This means
                for (index, input) in input_iter {
                    let input = render(input, scope, context);

                    // Determine keys. there is at most one key for each
                    // equivalence class, and an equivalence class is only
                    // engaged if it contains both a new and an old column.
                    // If the class contains *more than one* new column we
                    // may need to put a `filter` in, or perhaps await a
                    // later join (and ensure that one exists).

                    let mut old_keys = Vec::new();
                    let mut new_keys = Vec::new();

                    for sets in variables.iter() {
                        let new_pos = sets.iter().position(|(i, _)| i == &index);
                        let old_pos = columns.iter().position(|i| sets.contains(i));

                        // If we have both a new and an old column in the constraint ...
                        if let (Some(new_pos), Some(old_pos)) = (new_pos, old_pos) {
                            old_keys.push(old_pos);
                            new_keys.push(new_pos);
                        }
                    }

                    let old_keyed = joined.map(move |tuple| {
                        (
                            old_keys
                                .iter()
                                .map(|i| tuple[*i].clone())
                                .collect::<Vec<_>>(),
                            tuple,
                        )
                    });
                    let new_keyed = input.map(move |tuple| {
                        (
                            new_keys
                                .iter()
                                .map(|i| tuple[*i].clone())
                                .collect::<Vec<_>>(),
                            tuple,
                        )
                    });

                    joined = old_keyed.join_map(&new_keyed, |_keys, old, new| {
                        old.iter().chain(new).cloned().collect::<Vec<_>>()
                    });

                    columns.extend((0..arities[index]).map(|c| (index, c)));
                }

                joined
            } else {
                panic!("Empty join; why?");
            }
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
                        if agg.distinct {
                            let iter = source
                                .iter()
                                .flat_map(|(v, w)| {
                                    if *w > 0 {
                                        Some(agg.expr.eval_on(v))
                                    } else {
                                        None
                                    }
                                })
                                .collect::<HashSet<_>>();
                            result.push((agg.func.func())(iter));
                        } else {
                            let iter = source.iter().flat_map(|(v, w)| {
                                let eval = agg.expr.eval_on(v);
                                std::iter::repeat(eval).take(std::cmp::max(*w, 0) as usize)
                            });
                            result.push((agg.func.func())(iter));
                        }
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

/// Re-order relations in a join to process them in an order that makes sense.
///
/// ```rust
/// use materialize::dataflow2::RelationExpr;
/// use materialize::dataflow2::ColumnType;
/// use materialize::dataflow2::optimize_join_order;
/// use materialize::repr::FType;
///
/// let input1 = RelationExpr::Constant { rows: vec![], typ: vec![ColumnType { typ: FType::Bool, is_nullable: false }] };
/// let input2 = RelationExpr::Constant { rows: vec![], typ: vec![ColumnType { typ: FType::Bool, is_nullable: false }] };
/// let input3 = RelationExpr::Constant { rows: vec![], typ: vec![ColumnType { typ: FType::Bool, is_nullable: false }] };
/// let join = RelationExpr::Join {
///     inputs: vec![input1, input2, input3],
///     arities: vec![1, 1, 1],
///     variables: vec![vec![(0,0),(2,0)].into_iter().collect()],
/// };
/// let typ = vec![
///     ColumnType { typ: FType::Bool, is_nullable: false },
///     ColumnType { typ: FType::Bool, is_nullable: false },
///     ColumnType { typ: FType::Bool, is_nullable: false },
/// ];
/// let (opt_rel, opt_typ) = optimize_join_order(join, typ);
///
/// if let RelationExpr::Project { input, outputs } = opt_rel {
///     assert_eq!(outputs, vec![0, 2, 1]);
/// }
/// ```
pub fn optimize_join_order(
    relation: RelationExpr,
    metadata: RelationType,
) -> (RelationExpr, RelationType) {
    if let RelationExpr::Join {
        inputs,
        arities,
        variables,
    } = relation
    {
        // Step 1: determine a plan order starting from `inputs[0]`.
        let mut plan_order = vec![0];
        while plan_order.len() < inputs.len() {
            let mut candidates = (0..inputs.len())
                .filter(|i| !plan_order.contains(i))
                .map(|i| {
                    (
                        variables
                            .iter()
                            .filter(|vars| {
                                vars.iter().any(|(idx, _)| &i == idx)
                                    && vars.iter().any(|(idx, _)| plan_order.contains(idx))
                            })
                            .count(),
                        i,
                    )
                })
                .collect::<Vec<_>>();

            candidates.sort();
            plan_order.push(candidates.pop().expect("Candidate expected").1);
        }

        // Step 2: rewrite `variables`.
        let mut positions = vec![0; plan_order.len()];
        for (index, input) in plan_order.iter().enumerate() {
            positions[*input] = index;
        }

        let mut new_variables = Vec::new();
        for variable in variables.iter() {
            let mut new_set = HashSet::new();
            for (rel, col) in variable.iter() {
                new_set.insert((positions[*rel], *col));
            }
            new_variables.push(new_set);
        }

        // Step 3: prepare `Project`.
        // We want to present as if in the order we promised, so we need to permute.
        // In particular, for each (rel, col) in order, we want to figure out where
        // it lives in our weird local order, and build an expr that picks it out.
        let mut offset = 0;
        let mut offsets = vec![0; plan_order.len()];
        for input in plan_order.iter() {
            offsets[*input] = offset;
            offset += arities[*input];
        }

        let mut projection = Vec::new();
        for rel in 0..inputs.len() {
            for col in 0..arities[rel] {
                let position = offsets[rel] + col;
                projection.push(position);
            }
        }

        // Step 4: prepare output
        let mut new_inputs = Vec::new();
        let mut new_arities = Vec::new();
        for rel in plan_order.into_iter() {
            new_inputs.push(inputs[rel].clone()); // TODO: Extract from `inputs`.
            new_arities.push(arities[rel]);
        }

        let join = RelationExpr::Join {
            inputs: new_inputs,
            arities: new_arities,
            variables: new_variables,
        };

        // Output projection
        let output = RelationExpr::Project {
            input: Box::new(join),
            outputs: projection,
        };

        (output, metadata)
    } else {
        (relation, metadata)
    }
}
