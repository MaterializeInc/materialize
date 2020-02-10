// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::op_ref)]

use timely::dataflow::Scope;

use differential_dataflow::lattice::Lattice;

use dogsdogsdogs::altneu::AltNeu;

use dataflow_types::Timestamp;
use expr::{EvalEnv, RelationExpr, ScalarExpr};
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
        predicates: &[ScalarExpr],
        env: &EvalEnv,
        scope: &mut G,
        worker_index: usize,
        subtract: F,
    ) -> Collection<G, Row>
    where
        F: Fn(&G::Timestamp) -> G::Timestamp + Clone + 'static,
    {
        if let RelationExpr::Join {
            inputs,
            variables,
            demand: _,
            implementation: expr::JoinImplementation::DeltaQuery(orders),
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

                        let mut offset = 0;
                        let mut prior_arities = Vec::new();
                        for input in 0..inputs.len() {
                            prior_arities.push(offset);
                            offset += arities[input];
                        }

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

                                let mut predicates = predicates.to_vec();
                                update_stream = build_filter(
                                    update_stream,
                                    &update_column_sources,
                                    &mut predicates,
                                    &prior_arities,
                                    env,
                                );

                                // We use the order specified by the implementation.
                                let order = &orders[relation];

                                // Repeatedly update `update_stream` to reflect joins with more and more
                                // other relations, in the specified order.
                                for (other, next_key) in order.iter() {
                                    // Keys for the incoming updates are determined by locating
                                    // the elements of `next_keys` among the existing `columns`.
                                    let prev_key = next_key
                                        .iter()
                                        .map(|k| {
                                            if let ScalarExpr::Column(c) = k {
                                                variables
                                                    .iter()
                                                    .find(|v| v.contains(&(*other, *c)))
                                                    .expect("Column in key not bound!")
                                                    .iter()
                                                    .flat_map(|rel_col1| {
                                                        // Find the first (rel,col) pair in `update_column_sources`.
                                                        // One *should* exist, but it is not the case that all must.us
                                                        update_column_sources.iter().position(
                                                            |rel_col2| rel_col1 == rel_col2,
                                                        )
                                                    })
                                                    .next()
                                                    .expect(
                                                        "Column in key not bound by prior column",
                                                    )
                                            } else {
                                                panic!(
                                                    "Non-column keys are not currently supported"
                                                );
                                            }
                                        })
                                        .collect::<Vec<_>>();

                                    // TODO: Investigate demanded columns as in DifferentialLinear join.

                                    // We require different logic based on the flavor of arrangement.
                                    // We may need to cache each of these if we want to re-use the same wrapped
                                    // arrangement, rather than re-wrap each time we use a thing.
                                    let subtract = subtract.clone();
                                    update_stream = match self
                                        .arrangement(&inputs[*other], &next_key[..])
                                        .unwrap_or_else(|| {
                                            panic!(
                                                "Arrangement alarmingly absent!: {}, {:?}",
                                                inputs[*other].pretty(),
                                                &next_key[..]
                                            )
                                        }) {
                                        ArrangementFlavor::Local(local) => {
                                            if other > &relation {
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
                                            if other > &relation {
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
                                        .extend((0..arities[*other]).map(|c| (*other, c)));

                                    update_stream = build_filter(
                                        update_stream,
                                        &update_column_sources,
                                        &mut predicates,
                                        &prior_arities,
                                        env,
                                    );
                                }

                                // We must now de-permute the results to return to the common order.
                                // TODO: Non-demanded columns would need default values here.
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
            results
        } else {
            panic!("delta_join invoke on non-delta join");
        }
    }
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

/// Filters updates on some columns by predicates that are ready to go.
///
/// The `predicates` argument has all applied predicates removed.
pub fn build_filter<G>(
    updates: Collection<G, Row>,
    columns: &[(usize, usize)],
    predicates: &mut Vec<ScalarExpr>,
    prior_arities: &[usize],
    env: &EvalEnv,
) -> Collection<G, Row>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let mut map = std::collections::HashMap::new();
    for (pos, (rel, col)) in columns.iter().enumerate() {
        map.insert(prior_arities[*rel] + *col, pos);
    }

    let mut ready_to_go = Vec::new();
    predicates.retain(|predicate| {
        if predicate.support().iter().all(|c| map.contains_key(c)) {
            let mut predicate = predicate.clone();
            predicate.visit_mut(&mut |e| {
                if let ScalarExpr::Column(c) = e {
                    *c = map[c];
                }
            });
            ready_to_go.push(predicate);
            false
        } else {
            true
        }
    });

    if ready_to_go.is_empty() {
        updates
    } else {
        let env = env.clone();
        let temp_storage = repr::RowArena::new();
        updates.filter(move |input_row| {
            let datums = input_row.unpack();
            ready_to_go.iter().all(
                |predicate| match predicate.eval(&datums, &env, &temp_storage) {
                    Datum::True => true,
                    Datum::False | Datum::Null => false,
                    _ => unreachable!(),
                },
            )
        })
    }
}
