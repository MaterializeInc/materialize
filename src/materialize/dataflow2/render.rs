// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::collections::HashSet;
// use std::hash::BuildHasher;

use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;

use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::{lattice::Lattice, Collection};

use crate::dataflow2::context::{ArrangementFlavor, Context};
use crate::dataflow2::types::RelationExpr;
use crate::repr::Datum;

pub fn render<G>(
    plan: RelationExpr,
    scope: &mut G,
    context: &mut Context<G, RelationExpr, Datum, crate::clock::Timestamp>,
) -> Collection<G, Vec<Datum>, isize>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<crate::clock::Timestamp>,
    // S: BuildHasher + Clone,
{
    if context.collection(&plan).is_none() {
        let collection = match plan.clone() {
            RelationExpr::Constant { rows, .. } => {
                use differential_dataflow::collection::AsCollection;
                use timely::dataflow::operators::{Map, ToStream};
                rows.to_stream(scope)
                    .map(|x| (x, Default::default(), 1))
                    .as_collection()
            }
            RelationExpr::Get { name, typ: _ } => {
                // TODO: something more tasteful.
                // perhaps load an empty collection, warn?
                panic!("Collection {} not pre-loaded", name);
            }
            RelationExpr::Let { name, value, body } => {
                let typ = value.typ();
                let bind = RelationExpr::Get { name, typ };
                if context.collection(&bind).is_some() {
                    panic!("Inappropriate to re-bind name: {:?}", bind);
                } else {
                    let value = render(*value, scope, context);
                    context.collections.insert(bind.clone(), value);
                    render(*body, scope, context)
                }
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
                        let to_push = s.0.eval(&tuple[..len]);
                        tuple.push(to_push);
                    }
                    tuple
                })
            }
            RelationExpr::Filter { input, predicates } => {
                let input = render(*input, scope, context);
                input.filter(move |x| {
                    predicates.iter().all(|predicate| match predicate.eval(x) {
                        Datum::True => true,
                        Datum::False => false,
                        _ => unreachable!(),
                    })
                })
            }
            RelationExpr::Join { inputs, variables } => {
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

                let arities = inputs.iter().map(|i| i.arity()).collect::<Vec<_>>();

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

                        let old_keyed = joined
                            .map(move |tuple| {
                                (
                                    old_keys
                                        .iter()
                                        .map(|i| tuple[*i].clone())
                                        .collect::<Vec<_>>(),
                                    tuple,
                                )
                            })
                            .arrange_by_key();

                        // TODO: easier idioms for detecting, re-using, and stashing.
                        if !context.arrangement(&input, &new_keys[..]).is_none() {
                            let rendered = render(input.clone(), scope, context);
                            let new_keys2 = new_keys.clone();
                            let new_keyed = rendered
                                .map(move |tuple| {
                                    (
                                        new_keys2
                                            .iter()
                                            .map(|i| tuple[*i].clone())
                                            .collect::<Vec<_>>(),
                                        tuple,
                                    )
                                })
                                .arrange_by_key();
                            context.set_local(input.clone(), &new_keys[..], new_keyed);
                        }

                        joined = match context.arrangement(&input, &new_keys[..]) {
                            Some(ArrangementFlavor::Local(local)) => {
                                old_keyed.join_core(&local, |_keys, old, new| {
                                    Some(old.iter().chain(new).cloned().collect::<Vec<_>>())
                                })
                            }
                            Some(ArrangementFlavor::Trace(trace)) => {
                                old_keyed.join_core(&trace, |_keys, old, new| {
                                    Some(old.iter().chain(new).cloned().collect::<Vec<_>>())
                                })
                            }
                            None => {
                                panic!("Arrangement alarmingly absent!");
                            }
                        };

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
                use differential_dataflow::operators::reduce::ReduceCore;
                use differential_dataflow::trace::implementations::ord::OrdValSpine;

                let self_clone = plan.clone();
                let keys_clone = group_key.clone();
                let input = render(*input, scope, context);

                let arrangement = input
                    .map(move |tuple| {
                        (
                            group_key
                                .iter()
                                .map(|i| tuple[*i].clone())
                                .collect::<Vec<_>>(),
                            tuple,
                        )
                    })
                    // .reduce_abelian::<>(move |_key, source, target| {
                    .reduce_abelian::<_, OrdValSpine<_, _, _, _>>(move |key, source, target| {
                        let mut result = Vec::with_capacity(key.len() + aggregates.len());
                        result.extend(key.iter().cloned());
                        for (_idx, (agg, _typ)) in aggregates.iter().enumerate() {
                            if agg.distinct {
                                let iter =
                                    source
                                        .iter()
                                        .flat_map(|(v, w)| {
                                            if *w > 0 {
                                                Some(agg.expr.eval(v))
                                            } else {
                                                None
                                            }
                                        })
                                        .collect::<HashSet<_>>();
                                result.push((agg.func.func())(iter));
                            } else {
                                let iter = source.iter().flat_map(|(v, w)| {
                                    let eval = agg.expr.eval(v);
                                    std::iter::repeat(eval).take(std::cmp::max(*w, 0) as usize)
                                });
                                result.push((agg.func.func())(iter));
                            }
                        }
                        target.push((result, 1));
                    });

                context.set_local(self_clone, &keys_clone[..], arrangement.clone());
                arrangement.as_collection(|_key, tuple| tuple.clone())
            }

            RelationExpr::OrDefault { input, default } => {
                use differential_dataflow::collection::AsCollection;
                use differential_dataflow::operators::reduce::Threshold;
                use differential_dataflow::operators::Join;
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
                // TODO: re-use and publish arrangement here.
                let arity = input.arity();
                let keys = (0..arity).collect::<Vec<_>>();

                // TODO: easier idioms for detecting, re-using, and stashing.
                if !context.arrangement(&input, &keys[..]).is_none() {
                    let rendered = render((*input).clone(), scope, context);
                    let keys2 = keys.clone();
                    let keyed = rendered
                        .map(move |tuple| {
                            (
                                keys2.iter().map(|i| tuple[*i].clone()).collect::<Vec<_>>(),
                                tuple,
                            )
                        })
                        .arrange_by_key();
                    context.set_local((*input).clone(), &keys[..], keyed);
                }

                use differential_dataflow::operators::reduce::ReduceCore;
                use differential_dataflow::trace::implementations::ord::OrdValSpine;

                let arranged = match context.arrangement(&input, &keys[..]) {
                    Some(ArrangementFlavor::Local(local)) => local
                        .reduce_abelian::<_, OrdValSpine<_, _, _, _>>(move |k, _s, t| {
                            t.push((k.to_vec(), 1))
                        }),
                    Some(ArrangementFlavor::Trace(trace)) => trace
                        .reduce_abelian::<_, OrdValSpine<_, _, _, _>>(move |k, _s, t| {
                            t.push((k.to_vec(), 1))
                        }),
                    None => {
                        panic!("Arrangement alarmingly absent!");
                    }
                };

                context.set_local(*input, &keys[..], arranged.clone());
                arranged.as_collection(|_k, v| v.clone())
            }
            RelationExpr::Union { left, right } => {
                let input1 = render(*left, scope, context);
                let input2 = render(*right, scope, context);
                input1.concat(&input2)
            }
        };

        context.collections.insert(plan.clone(), collection);
    }

    context
        .collection(&plan)
        .expect("Collection surprisingly absent")
        .clone()
}
