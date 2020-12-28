// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::op_ref)]
use differential_dataflow::lattice::Lattice;
use dogsdogsdogs::altneu::AltNeu;
use std::collections::{HashMap, HashSet};
use timely::dataflow::Scope;

use dataflow_types::DataflowError;
use expr::{JoinInputMapper, MapFilterProject, RelationExpr, ScalarExpr};
use repr::{Datum, Row, RowArena, RowPacker, Timestamp};

use super::context::{ArrangementFlavor, Context};
use crate::operator::CollectionExt;

impl<G> Context<G, RelationExpr, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// Renders `RelationExpr:Join` using dogs^3 delta query dataflows.
    ///
    /// The join is followed by the application of `map_filter_project`, whose
    /// implementation will be pushed in to the join pipeline if at all possible.
    pub fn render_delta_join<F>(
        &mut self,
        relation_expr: &RelationExpr,
        predicates: &[ScalarExpr],
        scope: &mut G,
        worker_index: usize,
        subtract: F,
    ) -> (Collection<G, Row>, Collection<G, DataflowError>)
    where
        F: Fn(&G::Timestamp) -> G::Timestamp + Clone + 'static,
    {
        if let RelationExpr::Join {
            inputs,
            equivalences,
            demand: _,
            implementation: expr::JoinImplementation::DeltaQuery(orders),
        } = relation_expr
        {
            for input in inputs.iter() {
                self.ensure_rendered(input, scope, worker_index);
            }

            // Collects error streams for the ambient scope.
            let mut scope_errs = Vec::new();

            // Deduplicate the error streams of multiply used arrangements.
            let mut local_err_dedup = HashSet::new();
            let mut trace_err_dedup = HashSet::new();

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

                        let input_mapper = JoinInputMapper::new(inputs);
                        let map_filter_project = MapFilterProject::new(input_mapper.total_columns()).filter(predicates.iter().cloned());

                        // First let's prepare the input arrangements we will need.
                        // This reduces redundant imports, and simplifies the dataflow structure.
                        // As the arrangements are all shared, it should not dramatically improve
                        // the efficiency, but the dataflow simplification is worth doing.
                        //
                        // The arrangements are keyed by input and arrangement key, and by whether
                        // the arrangement is "alt" or "neu", which corresponds to whether the use
                        // of the arrangement is by a relation before or after it in the order, resp.
                        // Because the alt and neu variants have different types, we will maintain
                        // them in different collections.
                        let mut arrangements_alt = std::collections::HashMap::new();
                        let mut arrangements_neu = std::collections::HashMap::new();
                        for relation in 0 .. inputs.len() {
                            let order = &orders[relation];
                            for (other, next_key) in order.iter() {
                                let subtract = subtract.clone();
                                // Alt case
                                if other > &relation {
                                    arrangements_alt
                                        .entry((&inputs[*other], &next_key[..]))
                                        .or_insert_with(|| match self
                                            .arrangement(&inputs[*other], &next_key[..])
                                            .unwrap_or_else(|| {
                                                panic!(
                                                    "Arrangement alarmingly absent!: {}, {:?}",
                                                    inputs[*other].pretty(),
                                                    &next_key[..]
                                                )
                                            }) {
                                            ArrangementFlavor::Local(oks, errs) => {
                                                if local_err_dedup.insert((&inputs[*other], &next_key[..])) {
                                                    scope_errs.push(errs.as_collection(|k, _v| k.clone()));
                                                }
                                                Ok(oks
                                                    .enter_at(
                                                        inner,
                                                        |_, _, t| AltNeu::alt(t.clone()),
                                                        move |t| subtract(&t.time),
                                                    ))
                                            }
                                            ArrangementFlavor::Trace(_gid, oks, errs) => {
                                                if trace_err_dedup.insert((&inputs[*other], &next_key[..])) {
                                                    scope_errs.push(errs.as_collection(|k, _v| k.clone()));
                                                }
                                                Err(oks
                                                    .enter_at(
                                                        inner,
                                                        |_, _, t| AltNeu::alt(t.clone()),
                                                        move |t| subtract(&t.time),
                                                    ))
                                            }
                                        });
                                } else {
                                    arrangements_neu
                                    .entry((&inputs[*other], &next_key[..]))
                                    .or_insert_with(|| match self
                                        .arrangement(&inputs[*other], &next_key[..])
                                        .unwrap_or_else(|| {
                                            panic!(
                                                "Arrangement alarmingly absent!: {}, {:?}",
                                                inputs[*other].pretty(),
                                                &next_key[..]
                                            )
                                        }) {
                                        ArrangementFlavor::Local(oks, errs) => {
                                            if local_err_dedup.insert((&inputs[*other], &next_key[..])) {
                                                scope_errs.push(errs.as_collection(|k, _v| k.clone()));
                                            }
                                            Ok(oks
                                                .enter_at(
                                                    inner,
                                                    |_, _, t| AltNeu::neu(t.clone()),
                                                    move |t| subtract(&t.time),
                                                ))
                                        }
                                        ArrangementFlavor::Trace(_gid, oks, errs) => {
                                            if trace_err_dedup.insert((&inputs[*other], &next_key[..])) {
                                                scope_errs.push(errs.as_collection(|k, _v| k.clone()));
                                            }
                                            Err(oks
                                                .enter_at(
                                                    inner,
                                                    |_, _, t| AltNeu::neu(t.clone()),
                                                    move |t| subtract(&t.time),
                                                ))
                                        }
                                    });
                                }
                            }
                        }

                        // Collects error streams for the inner scope. Concats before leaving.
                        let mut inner_errs = Vec::with_capacity(inputs.len());
                        for relation in 0..inputs.len() {

                            // Other than the stream of updates, our loop-carried state are these
                            // three variables: `column_map`, `equivalences`, and `mfp`, which
                            // record the current locations of extended output columns, and what
                            // work remains to be done on them (other than the joining itself).
                            let mut column_map = HashMap::new();
                            for column in input_mapper.global_columns(relation) {
                                column_map.insert(column, column_map.len());
                            }
                            // We maintain a private copy of `equivalences`, which we will digest
                            // as we produce the join.
                            let mut equivalences = equivalences.clone();
                            for equivalence in equivalences.iter_mut() {
                                equivalence.sort();
                                equivalence.dedup();
                            }
                            // We maintain a private copy of `map_filter_project`, which we will
                            // digest as we produce the join.
                            let mut mfp = map_filter_project.clone();

                            // This collection determines changes that result from updates inbound
                            // from `inputs[relation]` and reflects all strictly prior updates and
                            // concurrent updates from relations prior to `relation`.
                            let name = format!("delta path {}", relation);
                            let delta_query = inner.clone().region_named(&name, |region| {

                                // The plan is to move through each relation, starting from `relation` and in the order
                                // indicated in `orders[relation]`. At each moment, we will have the columns from the
                                // subset of relations encountered so far, and we will have applied as much as we can
                                // of the filters in `equivalences` and the logic in `map_filter_project`, based on the
                                // available columns.
                                //
                                // As we go, we will track the physical locations of each intended output column, as well
                                // as the locations of intermediate results from partial application of `map_filter_project`.
                                //
                                // Just before we apply the `lookup` function to perform a join, we will first use our
                                // available information to determine the filtering and logic that we can apply, and
                                // introduce that in to the `lookup` logic to cause it to happen in that operator.

                                // Collects error streams for the region scope. Concats before leaving.
                                let mut region_errs = Vec::with_capacity(inputs.len());

                                // At this point we are able to construct a per-row closure that can be applied once
                                // we have the first wave of columns in place. We will not apply it quite yet, because
                                // we have three code paths that might produce data and it is complicated.
                                // TODO(mcsherry): apply `closure` early, in `as_collection`.
                                let closure = MyClosure::build(&mut column_map, &mut equivalences, &mut mfp);

                                // Ensure this input is rendered, and extract its update stream.
                                let update_stream =
                                if let Some((_key, val)) = arrangements_alt.iter().find(|(key, _val)| key.0 == &inputs[relation]) {
                                    match val {
                                        Ok(local) => local.as_collection(|_k,v| v.clone()).enter_region(region),
                                        Err(trace) => trace.as_collection(|_k,v| v.clone()).enter_region(region),
                                    }
                                } else {
                                    self
                                        .collection(&inputs[relation])
                                        .expect("Failed to render update stream").0.enter(inner).enter_region(region)
                                };

                                // Apply what `closure` we are able to, and record any errors.
                                let (mut update_stream, errs) = update_stream.flat_map_fallible({
                                    move |row| {
                                        // TODO(mcsherry): re-use allocation for unpacking.
                                        let mut unpacked = row.unpack();
                                        let temp_storage = RowArena::new();
                                        closure.apply(&mut unpacked, &temp_storage).transpose()
                                    }
                                });
                                region_errs.push(errs.map(DataflowError::from));

                                // We track the input relations as they are added to the join so we can figure out
                                // which expressions have been bound.
                                let mut bound_inputs = vec![relation];
                                // We use the order specified by the implementation.
                                let order = &orders[relation];

                                // Repeatedly update `update_stream` to reflect joins with more and more
                                // other relations, in the specified order.
                                for (other, next_key) in order.iter() {

                                    let next_key_rebased = next_key.iter().map(
                                        |k| input_mapper.map_expr_to_global(k.clone(), *other)
                                    ).collect::<Vec<_>>();

                                    // Keys for the incoming updates are determined by locating
                                    // the elements of `next_keys` among the existing `columns`.
                                    let prev_key = next_key_rebased
                                        .iter()
                                        .map(|expr| {
                                            let mut bound_expr = input_mapper
                                                .find_bound_expr(expr, &bound_inputs, &equivalences)
                                                .expect("Expression in join plan is not bound at time of use");
                                            // Rewrite column references to physical locations.
                                            bound_expr.permute_map(&column_map);
                                            bound_expr
                                        })
                                        .collect::<Vec<_>>();

                                    // Remove each element of `next_keys` from `equivalences`, so that we
                                    // avoid redundant predicate work. This removal also paves the way for
                                    // more precise "demand" information going forward.
                                    for equivalence in equivalences.iter_mut() {
                                        equivalence.retain(|expr| !next_key_rebased.contains(expr));
                                    }
                                    equivalences.retain(|e| e.len() > 1);

                                    // TODO(mcsherry): Investigate demanded columns as in DifferentialLinear join.

                                    // Update our map of the sources of each column in the update stream.
                                    for column in input_mapper.global_columns(*other) {
                                        column_map.insert(column, column_map.len());
                                    }

                                    // At this point we are able to construct a per-row closure that can be applied
                                    // once we have added additional columns from `lookup`. We build it now so that
                                    // it can be applied immediately in the `lookup` operator.
                                    let closure = MyClosure::build(&mut column_map, &mut equivalences, &mut mfp);

                                    // We require different logic based on the flavor of arrangement.
                                    // We may need to cache each of these if we want to re-use the same wrapped
                                    // arrangement, rather than re-wrap each time we use a thing.
                                    let (oks, errs) = if other > &relation {
                                        match arrangements_alt.get(&(&inputs[*other], &next_key[..])).unwrap() {
                                            Ok(local) => build_lookup(update_stream, local.enter_region(region), prev_key, closure),
                                            Err(trace) => build_lookup(update_stream, trace.enter_region(region), prev_key, closure),
                                        }
                                    } else {
                                        match arrangements_neu.get(&(&inputs[*other], &next_key[..])).unwrap() {
                                            Ok(local) => build_lookup(update_stream, local.enter_region(region), prev_key, closure),
                                            Err(trace) => build_lookup(update_stream, trace.enter_region(region), prev_key, closure),
                                        }
                                    };
                                    update_stream = oks;
                                    region_errs.push(errs);

                                    bound_inputs.push(*other);
                                }

                                // We must now apply `mfp` as it *should* have sufficient support for
                                // full evaluation. Before we do this, we need to permute it to refer
                                // to the correct physical column locations.
                                mfp.permute(&column_map, column_map.len());
                                // TODO(mcsherry): this could have been done in the previous `lookup`,
                                // but that seems like a fair bit of code complexity at the moment.
                                // TODO(mcsherry): at the least, this *should* just be permutation,
                                // which we could determine cannot error.
                                let (oks, errs) =
                                update_stream.flat_map_fallible({
                                    let mut row_packer = repr::RowPacker::new();
                                    move |row| {
                                        // TODO(mcsherry): re-use an allocation for unpacking.
                                        let mut unpacked = row.unpack();
                                        let temp_storage = RowArena::new();
                                        mfp.evaluate(&mut unpacked, &temp_storage, &mut row_packer).map_err(DataflowError::from).transpose()
                                    }
                                });

                                region_errs.push(errs);
                                inner_errs.push(differential_dataflow::collection::concatenate(region, region_errs).leave());
                                oks.leave()
                            });

                            delta_queries.push(delta_query);
                        }

                        scope_errs.push(differential_dataflow::collection::concatenate(inner, inner_errs).leave());

                        // Concatenate the results of each delta query as the accumulated results.
                        (
                            differential_dataflow::collection::concatenate(inner, delta_queries)
                                .leave(),
                            differential_dataflow::collection::concatenate(scope, scope_errs),
                        )
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
    prev_key: Vec<ScalarExpr>,
    closure: MyClosure,
) -> (Collection<G, Row>, Collection<G, DataflowError>)
where
    G: Scope,
    G::Timestamp: Lattice,
    Tr: TraceReader<Time = G::Timestamp, Key = Row, Val = Row, R = isize> + Clone + 'static,
    Tr::Batch: BatchReader<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
    Tr::Cursor: Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
{
    let (updates, errs) = updates.map_fallible(move |row| {
        // TODO(mcsherry): re-use allocation for unpacking.
        let datums = row.unpack();
        let temp_storage = RowArena::new();
        // TODO(mcsherry): re-use row packer.
        // TODO(mcsherry): re-use row allocation.
        let row_key = Row::try_pack(prev_key.iter().map(|e| e.eval(&datums, &temp_storage)))?;
        Ok((row, row_key))
    });

    use differential_dataflow::AsCollection;
    use timely::dataflow::operators::OkErr;
    let (oks, errs2) = dogsdogsdogs::operators::lookup_map(
        &updates,
        trace,
        move |(_row, row_key), key| {
            // Prefix key selector must populate `key` with key from prefix `row`.
            // TODO(mcsherry): investigate `clone_into`.
            *key = row_key.clone();
        },
        // TODO(mcsherry): consider `RefOrMut` in `lookup` interface to allow re-use.
        move |(prev_row, _prev_row_key), diff1, next_row, diff2| {
            // Output selector must produce (d_out, r_out) for each match.
            // TODO(mcsherry): re-use an allocation for unpacking.
            let mut datums = Vec::new();
            datums.extend(prev_row.iter());
            datums.extend(next_row.iter());
            let temp_storage = RowArena::new();
            // Return the result
            (closure.apply(&mut datums, &temp_storage), diff1 * diff2)
        },
        // Three default values, for decoding keys into.
        Row::pack::<_, Datum>(None),
        Row::pack::<_, Datum>(None),
        Row::pack::<_, Datum>(None),
    )
    .inner
    .ok_err(|(x, t, d)| {
        // TODO(mcsherry): consider `ok_err()` for `Collection`.
        match x {
            Ok(x) => Ok((x, t, d)),
            Err(x) => Err((DataflowError::from(x), t, d)),
        }
    });

    (
        oks.as_collection().flat_map(|x| x),
        errs.concat(&errs2.as_collection()),
    )
}

/// Filters updates on some columns by predicates that are ready to go.
///
/// Both the `predicates` and `equivalences` arguments will have all applied
/// predicates removed. Importantly, `equivalences` equates expressions with
/// the `Datum::eq` method, not `BinaryFunc::eq` which does not equate `Null`.
pub fn build_filter<G>(
    updates: Collection<G, Row>,
    source_columns: &[usize],
    predicates: &mut Vec<ScalarExpr>,
    equivalences: &mut Vec<Vec<ScalarExpr>>,
) -> (Collection<G, Row>, Option<Collection<G, DataflowError>>)
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let mut ready_to_go = Vec::new();

    // Extract predicates fully supported by available columns.
    predicates.retain(|p| {
        if p.support().into_iter().all(|c| source_columns.contains(&c)) {
            ready_to_go.push(p.clone());
            false
        } else {
            true
        }
    });
    // Extract equivalences fully supported by available columns.
    // This only happens if at least *two* expressions are fully supported.
    // Importantly, we should *not* use `BinaryFunc::Eq` to compare these
    // terms, as this would cause `Datum::Null` to not match.
    let mut ready_equivalences = Vec::new();
    for equivalence in equivalences.iter_mut() {
        if let Some(pos) = equivalence
            .iter()
            .position(|e| e.support().into_iter().all(|c| source_columns.contains(&c)))
        {
            let mut should_equate = Vec::new();
            let mut cursor = pos + 1;
            while cursor < equivalence.len() {
                if equivalence[cursor]
                    .support()
                    .into_iter()
                    .all(|c| source_columns.contains(&c))
                {
                    // Remove expression and equate with the first bound expression.
                    should_equate.push(equivalence.remove(cursor));
                } else {
                    cursor += 1;
                }
            }
            if !should_equate.is_empty() {
                should_equate.push(equivalence[pos].clone());
                ready_equivalences.push(should_equate);
            }
        }
    }
    equivalences.retain(|e| e.len() > 1);

    // Rewrite column references to their locations under `source_columns`.
    for expr in ready_to_go.iter_mut() {
        expr.visit_mut(&mut |e| {
            if let ScalarExpr::Column(c) = e {
                *c = source_columns
                    .iter()
                    .position(|x| x == c)
                    .expect("Column not found in source_columns");
            }
        })
    }
    for exprs in ready_equivalences.iter_mut() {
        for expr in exprs.iter_mut() {
            expr.visit_mut(&mut |e| {
                if let ScalarExpr::Column(c) = e {
                    *c = source_columns
                        .iter()
                        .position(|x| x == c)
                        .expect("Column not found in source_columns");
                }
            });
        }
    }

    // Apply a filter if either list of constraints is non-empty.
    if ready_to_go.is_empty() && ready_equivalences.is_empty() {
        (updates, None)
    } else {
        let (ok_collection, err_collection) = updates.filter_fallible(move |input_row| {
            let temp_storage = repr::RowArena::new();
            let datums = input_row.unpack();
            for p in &ready_to_go {
                if p.eval(&datums, &temp_storage)? != Datum::True {
                    return Ok(false);
                }
            }
            for exprs in &ready_equivalences {
                // Each list of expressions should be equal to the same value.
                let val = exprs[0].eval(&datums, &temp_storage)?;
                for expr in exprs[1..].iter() {
                    if expr.eval(&datums, &temp_storage)? != val {
                        return Ok(false);
                    }
                }
            }
            Ok::<_, DataflowError>(true)
        });
        (ok_collection, Some(err_collection))
    }
}

/// A manual closure implementation of filtering and logic application.
///
/// This manual implementation exists to express lifetime constraints clearly,
/// as there is a relationship between the borrowed lifetime of the closed-over
/// state and the arguments it takes when invoked. It was not clear how to do
/// this with a Rust closure (glorious battle was waged, but ultimately lost).
pub struct MyClosure {
    ready_equivalences: Vec<Vec<ScalarExpr>>,
    before: MapFilterProject,
}

impl MyClosure {
    /// Applies per-row filtering and logic.
    pub fn apply<'a>(
        &'a self,
        datums: &mut Vec<Datum<'a>>,
        temp_storage: &'a RowArena,
    ) -> Result<Option<Row>, expr::EvalError> {
        for exprs in self.ready_equivalences.iter() {
            // Each list of expressions should be equal to the same value.
            let val = exprs[0].eval(&datums[..], &temp_storage)?;
            for expr in exprs[1..].iter() {
                if expr.eval(&datums, &temp_storage)? != val {
                    return Ok(None);
                }
            }
        }
        // TODO(mcsherry): re-use an existing row packer.
        let mut row_packer = RowPacker::new();
        self.before.evaluate(datums, &temp_storage, &mut row_packer)
    }

    /// Construct an instance of the closue from available columns.
    ///
    /// This method updates the available columns, equivalences, and
    /// the `MapFilterProject` instance. The columns are updated to
    /// include reference to any columns added by the application of
    /// this logic, which might result from partial application of
    /// the `MapFilterProject` instance.
    pub fn build(
        columns: &mut HashMap<usize, usize>,
        equivalences: &mut Vec<Vec<ScalarExpr>>,
        mfp: &mut MapFilterProject,
    ) -> Self {
        // First, determine which columns should be compare due to `equivalences`.
        let mut ready_equivalences = Vec::new();
        for equivalence in equivalences.iter_mut() {
            if let Some(pos) = equivalence
                .iter()
                .position(|e| e.support().into_iter().all(|c| columns.contains_key(&c)))
            {
                let mut should_equate = Vec::new();
                let mut cursor = pos + 1;
                while cursor < equivalence.len() {
                    if equivalence[cursor]
                        .support()
                        .into_iter()
                        .all(|c| columns.contains_key(&c))
                    {
                        // Remove expression and equate with the first bound expression.
                        should_equate.push(equivalence.remove(cursor));
                    } else {
                        cursor += 1;
                    }
                }
                if !should_equate.is_empty() {
                    should_equate.push(equivalence[pos].clone());
                    ready_equivalences.push(should_equate);
                }
            }
        }
        equivalences.retain(|e| e.len() > 1);

        // Update ready_equivalences to reference correct column locations.
        for exprs in ready_equivalences.iter_mut() {
            for expr in exprs.iter_mut() {
                expr.visit_mut(&mut |e| {
                    if let ScalarExpr::Column(c) = e {
                        *c = columns[c];
                    }
                });
            }
        }

        // Next, partition `mfp` into `before` and `after`, the former of which can be
        // applied now.
        let (before, after) = std::mem::replace(mfp, MapFilterProject::new(mfp.input_arity))
            .partition(columns, columns.len());
        *mfp = after;

        // While it is top of mind, add any new columns to `columns` and increase `input_arity`.
        let bonus_columns = before.projection.len() - before.input_arity;
        for bonus_column in 0..bonus_columns {
            columns.insert(mfp.input_arity + bonus_column, columns.len());
        }

        // Cons up an instance of the closue with the closed-over state.
        Self {
            ready_equivalences,
            before,
        }
    }
}
