// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use timely::dataflow::Scope;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::difference::DiffPair;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::reduce::Count;
use differential_dataflow::operators::reduce::Reduce;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::Collection;

use expr::{AggregateExpr, AggregateFunc, EvalEnv, RelationExpr};
use repr::{Datum, Row, RowArena, RowPacker};

use super::context::Context;

impl<G, T> Context<G, RelationExpr, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + timely::progress::timestamp::Refines<T>,
    T: timely::progress::Timestamp + Lattice,
{
    pub fn render_reduce_robust(
        &mut self,
        relation_expr: &RelationExpr,
        env: &EvalEnv,
        scope: &mut G,
        worker_index: usize,
    ) {
        if let RelationExpr::Reduce {
            input,
            group_key,
            aggregates,
        } = relation_expr
        {
            // The reduce operator may have multiple aggregation functions, some of
            // which should only be applied to distinct values for each key. We need
            // to build a non-trivial dataflow fragment to robustly implement these
            // aggregations, including:
            //
            // 1. Different reductions for each aggregation, to avoid maintaining
            //    state proportional to the cross-product of values.
            //
            // 2. Distinct operators before each reduction which requires distinct
            //    inputs, to avoid recomputation when the distinct set is stable.
            //
            // 3. Hierachical aggregation for operators like min and max that we
            //    cannot perform in the diff field.
            //
            // Our plan is to perform these actions, and the re-integrate the results
            // in a final reduce whose output arrangement looks just as if we had
            // applied a single reduction (which should be good for any consumers
            // of the operator and its arrangement).

            let float_scale = f64::from(1 << 24);

            use differential_dataflow::operators::reduce::ReduceCore;
            use timely::dataflow::operators::map::Map;

            let keys_clone = group_key.clone();

            self.ensure_rendered(input, env, scope, worker_index);
            let input = self.collection(input).unwrap();

            // Distinct is a special case, as there are no aggregates to aggregate.
            if aggregates.is_empty() {
                let arrangement = input
                    .map({
                        let env = env.clone();
                        let group_key = group_key.clone();
                        move |row| {
                            let temp_storage = RowArena::new();
                            let datums = row.unpack();
                            (
                                Row::pack(
                                    group_key
                                        .iter()
                                        .map(|i| i.eval(&datums, &env, &temp_storage)),
                                ),
                                (),
                            )
                        }
                    })
                    .reduce_abelian::<_, OrdValSpine<_, _, _, _>>("DistinctBy", {
                        |key, _input, output| {
                            output.push((key.clone(), 1));
                        }
                    });

                let index = (0..keys_clone.len()).collect::<Vec<_>>();
                self.set_local_columns(relation_expr, &index[..], arrangement);
            } else {
                // Bound the complex dataflow in a region, for better intepretability.
                let mut partials = Vec::with_capacity(aggregates.len());
                scope.region(|region| {
                    // Create an iterator over collections, where each is the application
                    // of one aggregation function whose results are annotated with its
                    // position in the final results. To be followed by a merge reduction.
                    for (index, aggr) in aggregates.iter().enumerate() {
                        // let mut aggr = aggr.clone();

                        let AggregateExpr {
                            func,
                            expr,
                            distinct,
                        } = aggr.clone();

                        let mut partial = input.enter(region).map({
                            let env = env.clone();
                            let group_key = group_key.clone();
                            move |row| {
                                let temp_storage = RowArena::new();
                                let datums = row.unpack();
                                (
                                    Row::pack(
                                        group_key
                                            .iter()
                                            .map(|i| i.eval(&datums, &env, &temp_storage)),
                                    ),
                                    Row::pack(Some(expr.eval(&datums, &env, &temp_storage))),
                                )
                            }
                        });

                        if distinct {
                            partial = partial.distinct();
                        }

                        // The `distinct` field is no longer set, so we can move to directly
                        // apply the aggregation as best we know how. That will either be
                        // packing the value into the diff field, or performing a hierarchical
                        // reduction.
                        let (accumulable, hierarchical) = match aggr.func {
                            AggregateFunc::SumInt32
                            | AggregateFunc::SumInt64
                            | AggregateFunc::SumFloat32
                            | AggregateFunc::SumFloat64
                            | AggregateFunc::SumDecimal
                            | AggregateFunc::SumNull
                            | AggregateFunc::Count
                            | AggregateFunc::CountAll
                            | AggregateFunc::Any
                            | AggregateFunc::All => (true, false),
                            AggregateFunc::MaxInt32
                            | AggregateFunc::MaxInt64
                            | AggregateFunc::MaxFloat32
                            | AggregateFunc::MaxFloat64
                            | AggregateFunc::MaxDecimal
                            | AggregateFunc::MaxBool
                            | AggregateFunc::MaxString
                            | AggregateFunc::MaxDate
                            | AggregateFunc::MaxTimestamp
                            | AggregateFunc::MaxTimestampTz
                            | AggregateFunc::MaxNull
                            | AggregateFunc::MinInt32
                            | AggregateFunc::MinInt64
                            | AggregateFunc::MinFloat32
                            | AggregateFunc::MinFloat64
                            | AggregateFunc::MinDecimal
                            | AggregateFunc::MinBool
                            | AggregateFunc::MinString
                            | AggregateFunc::MinDate
                            | AggregateFunc::MinTimestamp
                            | AggregateFunc::MinTimestampTz
                            | AggregateFunc::MinNull => (false, true),
                        };

                        let to_collate = if accumulable {
                            // Pack an accumulable quantity into the diff field.
                            partial
                                .inner
                                .map(|(d, t, r)| (d, t, r as i128))
                                .as_collection()
                                .explode({
                                    let func = func.clone();
                                    move |(key, row)| {
                                        let datum = row.unpack()[0];
                                        // let eval = aggr.expr.eval(&datum, &env, &temp_storage);
                                        let (aggs, nonnulls) = match func {
                                            AggregateFunc::CountAll => {
                                                // Nothing beyond the accumulated count is needed.
                                                (0i128, 0i128)
                                            }
                                            AggregateFunc::Count => {
                                                // Count needs to distinguish nulls from zero.
                                                (1, if datum.is_null() { 0 } else { 1 })
                                            }
                                            AggregateFunc::Any => match datum {
                                                Datum::True => (1, 0),
                                                Datum::Null => (0, 0),
                                                Datum::False => (0, 1),
                                                _ => {
                                                    panic!("Invalid argument to AggregateFunc::Any")
                                                }
                                            },
                                            AggregateFunc::All => match datum {
                                                Datum::True => (1, 0),
                                                Datum::Null => (0, 0),
                                                Datum::False => (0, 1),
                                                _ => {
                                                    panic!("Invalid argument to AggregateFunc::All")
                                                }
                                            },
                                            _ => {
                                                // Other accumulations need to disentangle the accumulable
                                                // value from its NULL-ness, which is not quite as easily
                                                // accumulated.
                                                match datum {
                                                    Datum::Int32(i) => (i128::from(i), 1),
                                                    Datum::Int64(i) => (i128::from(i), 1),
                                                    Datum::Float32(f) => {
                                                        ((f64::from(*f) * float_scale) as i128, 1)
                                                    }
                                                    Datum::Float64(f) => {
                                                        ((*f * float_scale) as i128, 1)
                                                    }
                                                    Datum::Decimal(d) => (d.as_i128(), 1),
                                                    Datum::Null => (0, 0),
                                                    x => panic!(
                                                        "Accumulating non-integer data: {:?}",
                                                        x
                                                    ),
                                                }
                                            }
                                        };
                                        Some((
                                            key,
                                            DiffPair::new(1i128, DiffPair::new(aggs, nonnulls)),
                                        ))
                                    }
                                })
                                .count()
                                .map(move |(key, accum)| {
                                    let tot = accum.element1;
                                    let agg = accum.element2.element1;
                                    let nnl = accum.element2.element2;

                                    let value = match (&func, nnl) {
                                        (AggregateFunc::Count, _) => Datum::Int64(agg as i64),
                                        (AggregateFunc::CountAll, _) => Datum::Int64(tot as i64),
                                        (AggregateFunc::All, _) => {
                                            // If any false, else if all true, else must be no false and some nulls.
                                            if nnl > 0 {
                                                Datum::False
                                            } else if tot == agg {
                                                Datum::True
                                            } else {
                                                Datum::Null
                                            }
                                        }
                                        (AggregateFunc::Any, _) => {
                                            // If any true, else if all false, else must be no true and some nulls.
                                            if agg > 0 {
                                                Datum::True
                                            } else if tot == nnl {
                                                Datum::False
                                            } else {
                                                Datum::Null
                                            }
                                        }
                                        // Below this point, anything with only nulls should be null.
                                        (_, 0) => Datum::Null,
                                        // If any non-nulls, just report the aggregate.
                                        (AggregateFunc::SumInt32, _) => Datum::Int32(agg as i32),
                                        (AggregateFunc::SumInt64, _) => Datum::Int64(agg as i64),
                                        (AggregateFunc::SumFloat32, _) => Datum::Float32(
                                            (((agg as f64) / float_scale) as f32).into(),
                                        ),
                                        (AggregateFunc::SumFloat64, _) => {
                                            Datum::Float64(((agg as f64) / float_scale).into())
                                        }
                                        (AggregateFunc::SumDecimal, _) => Datum::from(agg),
                                        (AggregateFunc::SumNull, _) => Datum::Null,
                                        x => panic!("Surprising accumulable aggregation: {:?}", x),
                                    };
                                    (key, (index, Row::pack(Some(value))))
                                })
                        } else {
                            // If hierarchical, we can repeatedly digest the groups, to minimize the incremental
                            // update costs on relatively small updates.
                            if hierarchical {
                                let mut partial_stage =
                                    partial.map({ move |(key, row)| ((key, row.hashed()), row) });

                                for log_modulus in
                                    [60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20, 16, 12, 8, 4u64]
                                        .iter()
                                {
                                    // here we do not apply `offset`, but instead restrict ourself with a limit
                                    // that includes the offset. We cannot apply `offset` until we perform the
                                    // final, complete reduction.
                                    partial_stage = build_hierarchical_stage(
                                        partial_stage,
                                        aggr.func.clone(),
                                        env.clone(),
                                        1u64 << log_modulus,
                                    );
                                }

                                partial = partial_stage.map(|((key, _hash), val)| (key, val));
                            }

                            partial.reduce_named("ReduceInaggregable", {
                                let env = env.clone();
                                move |_key, source, target| {
                                    let iter = source.iter().flat_map(|(v, w)| {
                                        std::iter::repeat(v.iter().next().unwrap())
                                            .take(*w as usize)
                                    });
                                    let temp_storage = RowArena::new();
                                    target.push((
                                        (
                                            index,
                                            Row::pack(Some(func.eval(iter, &env, &temp_storage))),
                                        ),
                                        1,
                                    ));
                                }
                            })
                        };

                        partials.push(to_collate.leave());
                    }
                });

                let arrangement =
                    differential_dataflow::collection::concatenate::<_, _, _, _>(scope, partials)
                        .reduce_abelian::<_, OrdValSpine<_, _, _, _>>("ReduceCollation", {
                        let aggregates_len = aggregates.len();
                        move |key, input, output| {
                            // The intent, unless things are terribly wrong, is that `input`
                            // contains, in order, the values to drop into `output`.
                            assert_eq!(input.len(), aggregates_len);
                            let mut result = RowPacker::new();
                            result.extend(key.iter());
                            for (index, ((pos, val), cnt)) in input.iter().enumerate() {
                                assert_eq!(*pos, index);
                                assert_eq!(*cnt, 1);
                                result.push(val.unpack().pop().unwrap());
                            }
                            output.push((result.finish(), 1));
                        }
                    });

                let index = (0..keys_clone.len()).collect::<Vec<_>>();
                self.set_local_columns(relation_expr, &index[..], arrangement);
            }
        }
    }
}

fn build_hierarchical_stage<G>(
    collection: Collection<G, ((Row, u64), Row)>,
    aggr: AggregateFunc,
    env: EvalEnv,
    modulus: u64,
) -> Collection<G, ((Row, u64), Row)>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    collection
        .map(move |((key, hash), row)| ((key, hash % modulus), row))
        .reduce_named("ReduceHierarchical", {
            move |_key, source, target| {
                let iter = source.iter().flat_map(|(v, w)| {
                    std::iter::repeat(v.iter().next().unwrap()).take(*w as usize)
                });
                let temp_storage = RowArena::new();
                target.push((Row::pack(Some(aggr.eval(iter, &env, &temp_storage))), 1));
            }
        })
}
