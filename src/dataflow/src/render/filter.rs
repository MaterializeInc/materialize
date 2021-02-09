// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders a filter expression that may reference `mz_logical_timestamp`.
//!
//! There are restricted options for how one can reference this term in
//! a maintained dataflow. Specifically, all predicates need to be of the
//! form
//! ```ignore
//! mz_logical_timestamp cmp_op expr
//! ```
//! where `cmp_op` is a comparison operator (e.g. <, >, =, >=, or <=) and
//! `expr` is an expression that does not contain `mz_logical_timestamp`.

use std::convert::TryFrom;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use dataflow_types::*;
use expr::{BinaryFunc, MirRelationExpr, MirScalarExpr, NullaryFunc};
use repr::{adt::decimal::Significand, Datum, Row, RowArena, ScalarType};

use crate::operator::StreamExt;
use crate::render::context::Context;
use crate::render::datum_vec::DatumVec;

impl<G, T> Context<G, MirRelationExpr, Row, T>
where
    G: Scope<Timestamp = repr::Timestamp>,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    /// Renders a filter expression that may reference `mz_logical_timestamp`.
    ///
    /// There are restricted options for how one can reference this term in
    /// a maintained dataflow. Specifically, all predicates need to be of the
    /// form
    /// ```ignore
    /// mz_logical_timestamp cmp_op expr
    /// ```
    /// where `cmp_op` is a comparison operator (e.g. <, >, =, >=, or <=) and
    /// `expr` is an expression that does not contain `mz_logical_timestamp`.
    pub fn render_filter(
        &mut self,
        relation_expr: &MirRelationExpr,
    ) -> (Collection<G, Row>, Collection<G, DataflowError>) {
        if let MirRelationExpr::Filter { input, predicates } = relation_expr {
            // Partition predicates into normal, and temporal lower/upper bounds.
            let (normal, lower, upper) = extract_temporal(predicates.iter().cloned())
                .unwrap_or_else(|err| {
                    panic!("Temporal predicate error: {:?}", err);
                });

            let (ok_collection, err_collection) = self.collection(input).unwrap();

            let (oks, errs) = ok_collection.inner.flat_map_fallible({
                let mut datums = DatumVec::new();
                move |(data, time, diff)| {
                    let temp_storage = RowArena::new();
                    let datums_local = datums.borrow_with(&data);
                    // Ignore any records failing normal predicate evaluation.
                    let mut passed = true;
                    for pred in normal.iter() {
                        passed = passed
                            && match pred.eval(&datums_local, &temp_storage) {
                                Err(e) => {
                                    return Some(Err((DataflowError::from(e), time, diff)))
                                        .into_iter()
                                        .chain(None.into_iter());
                                }
                                Ok(Datum::True) => true,
                                _ => false,
                            }
                    }
                    if passed {
                        // In order to work with times, it is easiest to convert it to an i128.
                        // This is because our decimal type uses that representation, and going
                        // from i128 to u64 is even more painful.
                        let mut time_128 = i128::from(time);
                        // Track whether we have seen a null in either bound, as this should
                        // prevent the record from being produced at any time.
                        let mut null_eval = false;

                        // If there is a lower bound, advance `time` to that lower bound.
                        // This decimal stuff is brittle; let's hope the scale never changes.
                        for l in lower.iter() {
                            match l.eval(&datums_local, &temp_storage) {
                                Err(e) => {
                                    return Some(Err((DataflowError::from(e), time, diff)))
                                        .into_iter()
                                        .chain(None.into_iter());
                                }
                                Ok(Datum::Decimal(s)) => {
                                    if time_128 < s.as_i128() {
                                        time_128 = s.as_i128();
                                    }
                                }
                                Ok(Datum::Null) => {
                                    null_eval = true;
                                }
                                x => {
                                    panic!("Non-decimal value in temporal predicate: {:?}", x);
                                }
                            }
                        }

                        // Extract an minimum upper bound. If not for errors, we would just write:
                        // let upper_bound = upper.iter().map(|l| l.eval(&datums_local, &temp_storage)?).min();
                        let mut upper_bound = None;
                        for u in upper.iter() {
                            match u.eval(&datums_local, &temp_storage) {
                                Err(e) => {
                                    return Some(Err((DataflowError::from(e), time, diff)))
                                        .into_iter()
                                        .chain(None.into_iter());
                                }
                                Ok(Datum::Decimal(s)) => {
                                    if upper_bound < Some(s.as_i128()) {
                                        upper_bound = Some(s.as_i128());
                                    }
                                }
                                Ok(Datum::Null) => {
                                    null_eval = true;
                                }
                                x => {
                                    panic!("Non-decimal value in temporal predicate: {:?}", x);
                                }
                            }
                        }

                        // Convert an i128 to a u64. Clamp values outside the valid range.
                        let upper_bound = upper_bound.map(|u| {
                            if u < 0 {
                                0u64
                            } else if u > i128::from(u64::MAX) {
                                u64::MAX
                            } else {
                                u64::try_from(u).unwrap()
                            }
                        });
                        let time = if time_128 < 0 {
                            0u64
                        } else if time_128 > i128::from(u64::MAX) {
                            u64::MAX
                        } else {
                            u64::try_from(time_128).unwrap()
                        };

                        // Only proceed if the new time is not greater or equal to upper,
                        // and if no null values were encountered in bound evaluation.
                        if upper_bound.as_ref().map(|u| time < *u).unwrap_or(true) && !null_eval {
                            Some(Ok((data.clone(), time, diff)))
                                .into_iter()
                                .chain(upper_bound.map(|u| Ok((data.clone(), u, -diff))))
                        } else {
                            None.into_iter().chain(None.into_iter())
                        }
                    } else {
                        None.into_iter().chain(None.into_iter())
                    }
                }
            });

            let err_collection = err_collection.concat(&errs.as_collection());
            (oks.as_collection(), err_collection)
        } else {
            panic!("Non-Filter expression provided to `render_filter`");
        }
    }
}

/// Partitions `predicates` into non-temporal, and lower and upper temporal bounds.
///
/// The first returned list is of predicates that do not contain `mz_logical_timestamp`.
/// The second and third returned lists contain expressions that, once evaluated, lower
/// and upper bound the validity interval of a record, respectively. These second two
/// lists are populared only by binary expressions of the form
/// ```ignore
/// mz_logical_timestamp cmp_op expr
/// ```
/// where `cmp_op` is a comparison operator and `expr` does not contain `mz_logical_timestamp`.
///
/// If any unsupported expression is found, for example one that uses `mz_logical_timestamp`
/// in an unsupported position, an error is returned.
pub fn extract_temporal<I>(
    predicates: I,
) -> Result<(Vec<MirScalarExpr>, Vec<MirScalarExpr>, Vec<MirScalarExpr>), String>
where
    I: IntoIterator<Item = MirScalarExpr>,
{
    let mut normal = Vec::new();
    let mut lower = Vec::new();
    let mut upper = Vec::new();

    for predicate in predicates {
        if !predicate.contains_temporal() {
            normal.push(predicate);
        } else if let MirScalarExpr::CallBinary {
            mut func,
            mut expr1,
            mut expr2,
        } = predicate
        {
            // Attempt to put `MzLogicalTimestamp` in the first argument position.
            if !expr1.contains_temporal()
                && *expr2 == MirScalarExpr::CallNullary(NullaryFunc::MzLogicalTimestamp)
            {
                std::mem::swap(&mut expr1, &mut expr2);
                func = match func {
                    BinaryFunc::Eq => BinaryFunc::Eq,
                    BinaryFunc::Lt => BinaryFunc::Gt,
                    BinaryFunc::Lte => BinaryFunc::Gte,
                    BinaryFunc::Gt => BinaryFunc::Lt,
                    BinaryFunc::Gte => BinaryFunc::Lte,
                    x => {
                        return Err(format!("Unsupported binary temporal operation: {:?}", x));
                    }
                };
            }

            // Error if MLT is referenced in an unsuppported position.
            if expr2.contains_temporal()
                || *expr1 != MirScalarExpr::CallNullary(NullaryFunc::MzLogicalTimestamp)
            {
                return Err("Unsupported temporal predicate: `mz_logical_timestamp()` must be directly compared to a non-temporal expression ".to_string());
            }

            // We'll need to use this a fair bit.
            let decimal_one = MirScalarExpr::literal_ok(
                Datum::Decimal(Significand::new(1)),
                ScalarType::Decimal(38, 0),
            );

            // MzLogicalTimestamp <OP> <EXPR2> for several supported operators.
            match func {
                BinaryFunc::Eq => {
                    // Lower bound of expr, upper bound of expr+1
                    lower.push((*expr2).clone());
                    upper.push(expr2.call_binary(decimal_one, BinaryFunc::AddDecimal));
                }
                BinaryFunc::Lt => {
                    upper.push(*expr2);
                }
                BinaryFunc::Lte => {
                    upper.push(expr2.call_binary(decimal_one, BinaryFunc::AddDecimal));
                }
                BinaryFunc::Gt => {
                    lower.push(expr2.call_binary(decimal_one, BinaryFunc::AddDecimal));
                }
                BinaryFunc::Gte => {
                    lower.push(*expr2);
                }
                _ => {
                    return Err(format!("Unsupported binary temporal operation: {:?}", func));
                }
            }
        } else {
            return Err("Unsupported temporal predicate: `mz_logical_timestamp()` must be directly compared to a non-temporal expression ".to_string());
        }
    }

    Ok((normal, lower, upper))
}
