// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use timely::dataflow::Scope;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::trace::implementations::ord::OrdValSpine;

use dogsdogsdogs::altneu::AltNeu;

use dataflow_types::Timestamp;
use expr::{EvalEnv, RelationExpr};
use repr::{Datum, Row};

use super::context::{ArrangementFlavor, Context};

impl<G> Context<G, RelationExpr, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// Renders `RelationExpr:Join` using dogs^3 delta query dataflows.
    pub fn render_delta_join<F>(
        &mut self,
        relation_expr: &RelationExpr,
        env: &EvalEnv,
        scope: &mut G,
        worker_index: usize,
        subtract: F,
    ) where
        F: Fn(&G::Timestamp) -> G::Timestamp + Clone + 'static,
    {
        if let RelationExpr::Join {
            inputs,
            variables,
            demand: _,
        } = relation_expr
        {
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

            for input in inputs.iter() {
                self.ensure_rendered(input, env, scope, worker_index);
            }

            // We'll need a new scope, to hold `AltNeu` wrappers, and we'll want
            // to import all traces as alt and neu variants (unless we do a more
            // careful analysis).
            let results =
                scope
                    .clone()
                    .scoped::<AltNeu<G::Timestamp>, _, _>("delta query", |inner| {
                        // Our plan is to iterate through each input relation, and attempt
                        // to find a plan that maximally uses existing keys (better: uses
                        // existing arrangements, to which we have access).
                        let mut delta_queries = Vec::new();

                        // We'll need type information for arities, if nothing else.
                        let types = inputs.iter().map(|input| input.typ()).collect::<Vec<_>>();
                        let arities = types
                            .iter()
                            .map(|typ| typ.column_types.len())
                            .collect::<Vec<_>>();

                        for relation in 0..inputs.len() {
                            // This collection determines changes that result from updates inbound
                            // from `inputs[relation]` and reflects all strictly prior updates and
                            // concurrent updates from relations prior to `relation`.
                            let delta_query = inner.clone().region(|region| {
                                // Ensure this input is rendered, and extract its update stream.
                                let mut update_stream = self
                                    .collection(&inputs[relation])
                                    .expect("Failed to render update stream")
                                    .enter(inner)
                                    .enter(region);

                                // We track the sources of each column in our update stream.
                                let mut update_column_sources = (0..arities[relation])
                                    .map(|c| (relation, c))
                                    .collect::<Vec<_>>();

                                // We must determine an order to join the other relations, starting from
                                // `relation`, and ideally maximizing the number of arrangements used.
                                let arrange_keys = inputs
                                    .iter()
                                    .map(|i| self.available_keys(i))
                                    .collect::<Vec<_>>();
                                let order = order_delta_join(
                                    inputs.len(),
                                    relation,
                                    variables,
                                    &arrange_keys,
                                );

                                // Repeatedly update `update_stream` to reflect joins with more and more
                                // other relations, in the specified order.
                                for index in 1..order.len() {
                                    let other = order[index];

                                    // Determine which existing columns must be keys, and which incoming
                                    // columns must be keys.
                                    let mut prev_key = Vec::new();
                                    let mut next_key = Vec::new();

                                    for variable in variables {
                                        // A constraint is active iff it constraints both prior and new relations.
                                        let prev_col = variable
                                            .iter()
                                            .find(|(rel, _)| order[..index].contains(rel));
                                        let next_col =
                                            variable.iter().find(|(rel, _)| rel == &other);
                                        if let (Some(prev), Some(next)) = (prev_col, next_col) {
                                            prev_key.push(
                                                update_column_sources
                                                    .iter()
                                                    .position(|cs| cs == prev)
                                                    .expect("Did not find prior key column"),
                                            );
                                            next_key.push(next.1); // capture only the column.
                                        }
                                    }

                                    // Keys might be empty (in cross-join scenarios) but ideally we now have
                                    // some none-trivial columns to work with.

                                    // Ensures that the necessary arrangement exists. Manufacture it if not.
                                    if self
                                        .arrangement_columns(&inputs[other], &next_key[..])
                                        .is_none()
                                    {
                                        let built = self
                                            .collection(&inputs[other])
                                            .expect("Collection not found!");
                                        let next_key_clone = next_key.clone();
                                        let next_keyed = built
                                            .map(move |row| {
                                                let datums = row.unpack();
                                                let key_row = Row::pack(
                                                    next_key_clone.iter().map(|i| datums[*i]),
                                                );
                                                (key_row, row)
                                            })
                                            .arrange_named::<OrdValSpine<_, _, _, _>>(&format!(
                                                "DeltaJoinIndex: {}, {}, {}",
                                                relation, other, index,
                                            ));
                                        self.set_local_columns(
                                            &inputs[other],
                                            &next_key[..],
                                            next_keyed,
                                        );
                                    }

                                    // We require different logic based on the flavor of arrangement.
                                    // We may need to cache each of these if we want to re-use the same wrapped
                                    // arrangement, rather than re-wrap each time we use a thing.
                                    let subtract = subtract.clone();
                                    update_stream = match self
                                        .arrangement_columns(&inputs[other], &next_key[..])
                                        .expect("Arrangement alarmingly absent!")
                                    {
                                        ArrangementFlavor::Local(local) => {
                                            if other > relation {
                                                let local = local
                                                    .enter_at(
                                                        inner,
                                                        |_, _, t| AltNeu::alt(t.clone()),
                                                        move |t| subtract(&t.time),
                                                    )
                                                    .enter(region);
                                                build_lookup(update_stream, local, prev_key)
                                            } else {
                                                let local = local
                                                    .enter_at(
                                                        inner,
                                                        |_, _, t| AltNeu::neu(t.clone()),
                                                        move |t| subtract(&t.time),
                                                    )
                                                    .enter(region);
                                                build_lookup(update_stream, local, prev_key)
                                            }
                                        }
                                        ArrangementFlavor::Trace(trace) => {
                                            if other > relation {
                                                let trace = trace
                                                    .enter_at(
                                                        inner,
                                                        |_, _, t| AltNeu::alt(t.clone()),
                                                        move |t| subtract(&t.time),
                                                    )
                                                    .enter(region);
                                                build_lookup(update_stream, trace, prev_key)
                                            } else {
                                                let trace = trace
                                                    .enter_at(
                                                        inner,
                                                        |_, _, t| AltNeu::neu(t.clone()),
                                                        move |t| subtract(&t.time),
                                                    )
                                                    .enter(region);
                                                build_lookup(update_stream, trace, prev_key)
                                            }
                                        }
                                    };

                                    // Update our map of the sources of each column in the update stream.
                                    update_column_sources
                                        .extend((0..arities[other]).map(|c| (other, c)));
                                }

                                // We must now de-permute the results to return to the common order.
                                update_stream = update_stream.map(move |row| {
                                    let datums = row.unpack();
                                    let mut to_sort = update_column_sources
                                        .iter()
                                        .zip(datums)
                                        .collect::<Vec<_>>();
                                    to_sort.sort();
                                    Row::pack(to_sort.into_iter().map(|(_, datum)| datum))
                                });

                                update_stream.leave()
                            });

                            delta_queries.push(delta_query);
                        }

                        // Concatenate the results of each delta query as the accumulated results.
                        differential_dataflow::collection::concatenate(inner, delta_queries).leave()
                    });

            self.collections.insert(relation_expr.clone(), results);
        }
    }
}

/// Orders `0 .. relations` starting with `start` so that arrangement use is maximized.
///
/// The ordering starts from `start` and attempts to add relations if we have access to an
/// arrangement with keys that could be used for the join with the relations thus far. This
/// reasoning does not know about uniqueness (yet) and may make bad decisions that inflate
/// the number of updates flowing through the system (but not the arranged footprint).
fn order_delta_join(
    relations: usize,
    start: usize,
    constraints: &[Vec<(usize, usize)>],
    arrange_keys: &[Vec<Vec<expr::ScalarExpr>>],
) -> Vec<usize> {
    let mut order = vec![start];
    while order.len() < relations {
        // Attempt to find a next relation, not yet in `order` and whose unique keys are all bound
        // by columns of relations that are present in `order`.
        let mut candidate = (0..relations).filter(|i| !order.contains(i)).find(|i| {
            arrange_keys[*i].iter().any(|keys| {
                keys.iter().all(|key| {
                    if let expr::ScalarExpr::Column(key) = key {
                        constraints.iter().any(|variables| {
                            let contains_key = variables.contains(&(*i, *key));
                            let contains_bound =
                                variables.iter().any(|(idx, _)| order.contains(idx));
                            contains_key && contains_bound
                        })
                    } else {
                        false
                    }
                })
            })
        });

        // Perhaps we found no relation with a key; we should find a relation with some constraint.
        if candidate.is_none() {
            let mut candidates = (0..relations)
                .filter(|i| !order.contains(i))
                .map(|i| {
                    (
                        constraints
                            .iter()
                            .filter(|vars| {
                                vars.iter().any(|(idx, _)| &i == idx)
                                    && vars.iter().any(|(idx, _)| order.contains(idx))
                            })
                            .count(),
                        i,
                    )
                })
                .collect::<Vec<_>>();

            candidates.sort();
            candidate = candidates.pop().map(|(_count, index)| index);
        }

        order.push(candidate.expect("No candidate found!"));
    }
    order
}

use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::BatchReader;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::Collection;

/// Constructs a `lookup_map` from supplied arguments.
///
/// This method exists to factor common logic from four code paths that are generic over the type of trace.
fn build_lookup<G, Tr>(
    updates: Collection<G, Row>,
    trace: Arranged<G, Tr>,
    prev_key: Vec<usize>,
) -> Collection<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice,
    Tr: TraceReader<Time = G::Timestamp, Key = Row, Val = Row, R = isize> + Clone + 'static,
    Tr::Batch: BatchReader<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
    Tr::Cursor: Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
{
    dogsdogsdogs::operators::lookup_map(
        &updates,
        trace,
        move |row, key| {
            // Prefix key selector must populate `key` with key from prefix `row`.
            // TODO: This could re-use the allocation behind `key`.
            let datums = row.unpack();
            *key = Row::pack(prev_key.iter().map(|i| datums[*i]));
        },
        |prev_row, diff1, next_row, diff2| {
            // Output selector must produce (d_out, r_out) for each match.
            // TODO: We can improve this.
            let prev_datums = prev_row.unpack();
            let next_datums = next_row.unpack();
            // Append columns on to accumulated columns.
            (
                Row::pack(prev_datums.into_iter().chain(next_datums)),
                diff1 * diff2,
            )
        },
        // Three default values, for decoding keys into.
        Row::pack::<_, Datum>(None),
        Row::pack::<_, Datum>(None),
        Row::pack::<_, Datum>(None),
    )
}
