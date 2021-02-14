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

/// Predicates partitioned into temporal and non-temporal.
///
/// Temporal predicates require some recognition to determine their
/// structure, and it is best to do that once and re-use the results.
///
/// There are restrictions on the temporal predicates we currently support.
/// They must directly constrain `MzLogicalTimestamp` from below or above,
/// by expressions that do not themselves contain `MzLogicalTimestamp`.
/// Conjunctions of such constraints are also ok.
pub struct FilterPlan {
    /// Normal predicates to evaluate on `&[Datum]` and expect `Ok(Datum::True)`.
    normal: Vec<MirScalarExpr>,
    /// Expressions that when evaluated lower-bound `MzLogicalTimestamp`.
    lower_bounds: Vec<MirScalarExpr>,
    /// Expressions that when evaluated upper-bound `MzLogicalTimestamp`.
    upper_bounds: Vec<MirScalarExpr>,
}

impl FilterPlan {
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
    pub fn create_from<I>(predicates: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = MirScalarExpr>,
    {
        let mut normal = Vec::new();
        let mut lower_bounds = Vec::new();
        let mut upper_bounds = Vec::new();

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
                        lower_bounds.push((*expr2).clone());
                        upper_bounds.push(expr2.call_binary(decimal_one, BinaryFunc::AddDecimal));
                    }
                    BinaryFunc::Lt => {
                        upper_bounds.push(*expr2);
                    }
                    BinaryFunc::Lte => {
                        upper_bounds.push(expr2.call_binary(decimal_one, BinaryFunc::AddDecimal));
                    }
                    BinaryFunc::Gt => {
                        lower_bounds.push(expr2.call_binary(decimal_one, BinaryFunc::AddDecimal));
                    }
                    BinaryFunc::Gte => {
                        lower_bounds.push(*expr2);
                    }
                    _ => {
                        return Err(format!("Unsupported binary temporal operation: {:?}", func));
                    }
                }
            } else {
                return Err("Unsupported temporal predicate: `mz_logical_timestamp()` must be directly compared to a non-temporal expression ".to_string());
            }
        }
        // Order predicates to put literal errors last.
        normal.sort_by_key(|p| p.is_literal_err());

        Ok(Self {
            normal,
            lower_bounds,
            upper_bounds,
        })
    }

    /// Evaluate the predicates, temporal and non-, and return times and differences for `data`.
    ///
    /// If `self` contains only non-temporal predicates, the result will either be `(time, diff)`,
    /// or an evaluation error. If `self contains temporal predicates, the results can be times
    /// that are greater than the input `time`, and may contain negated `diff` values.
    pub fn evaluate(
        &self,
        datums: &mut Vec<Datum>,
        time: repr::Timestamp,
        diff: isize,
    ) -> impl Iterator<Item = Result<(repr::Timestamp, isize), (DataflowError, repr::Timestamp, isize)>>
    {
        let temp_storage = RowArena::new();

        // Ignore any records failing normal predicate evaluation.
        for pred in self.normal.iter() {
            match pred.eval(datums, &temp_storage) {
                Err(e) => {
                    return Some(Err((DataflowError::from(e), time, diff)))
                        .into_iter()
                        .chain(None.into_iter());
                }
                Ok(Datum::True) => {}
                _ => {
                    return None.into_iter().chain(None.into_iter());
                }
            }
        }

        // In order to work with times, it is easiest to convert it to an i128.
        // This is because our decimal type uses that representation, and going
        // from i128 to u64 is even more painful.
        let mut lower_bound_i128 = i128::from(time);
        let mut upper_bound_i128 = None;

        // Track whether we have seen a null in either bound, as this should
        // prevent the record from being produced at any time.
        let mut null_eval = false;

        // Advance our lower bound to be at least the result of any lower bound
        // expressions.
        // TODO: This decimal stuff is brittle; let's hope the scale never changes.
        for l in self.lower_bounds.iter() {
            match l.eval(datums, &temp_storage) {
                Err(e) => {
                    return Some(Err((DataflowError::from(e), time, diff)))
                        .into_iter()
                        .chain(None.into_iter());
                }
                Ok(Datum::Decimal(s)) => {
                    if lower_bound_i128 < s.as_i128() {
                        lower_bound_i128 = s.as_i128();
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

        // If there are any upper bounds, determine the minimum upper bound.
        for u in self.upper_bounds.iter() {
            match u.eval(datums, &temp_storage) {
                Err(e) => {
                    return Some(Err((DataflowError::from(e), time, diff)))
                        .into_iter()
                        .chain(None.into_iter());
                }
                Ok(Datum::Decimal(s)) => {
                    // Replace `upper_bound` if it is none
                    if upper_bound_i128.is_none() || upper_bound_i128 > Some(s.as_i128()) {
                        upper_bound_i128 = Some(s.as_i128());
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

        // Force the upper bound to be at least the lower bound.
        // This should have the effect downstream of making the two equal,
        // which will result in no output.
        // Doing it this way spares us some awkward option comparison logic.
        // This also ensures that `upper_bound_u128` will be at least `time`,
        // which means "non-negative" / not needing to be clamped from below.
        if let Some(u) = upper_bound_i128.as_mut() {
            if *u < lower_bound_i128 {
                *u = lower_bound_i128;
            }
        }

        // Convert both of our bounds to `Option<u64>`, where negative numbers
        // are advanced up to `Some(0)` and numbers larger than `u64::MAX` are
        // set to `None`. These choices are believed correct to narrow intervals
        // of `i128` values to potentially half-open `u64` values.

        // We are "certain" that `lower_bound_i128` is at least `time`, which
        // means "non-negative" / not needing to be clamped from below.
        let lower_bound_u64 = if lower_bound_i128 > u64::MAX.into() {
            None
        } else {
            Some(u64::try_from(lower_bound_i128).unwrap())
        };

        // We ensured that `upper_bound_i128` is at least `lower_bound_i128`,
        // and so it also does not need to be clamped from below.
        let upper_bound_u64 = match upper_bound_i128 {
            Some(u) if u < 0 => {
                panic!("upper bound was ensured at least `time`; should be non-negative");
            }
            Some(u) if u > u64::MAX.into() => None,
            Some(u) => Some(u64::try_from(u).unwrap()),
            None => None,
        };

        // Only proceed if the new time is not greater or equal to upper,
        // and if no null values were encountered in bound evaluation.
        if lower_bound_u64 != upper_bound_u64 && !null_eval {
            let lower_opt = lower_bound_u64.map(|time| Ok((time, diff)));
            let upper_opt = upper_bound_u64.map(|time| Ok((time, -diff)));
            lower_opt.into_iter().chain(upper_opt.into_iter())
        } else {
            None.into_iter().chain(None.into_iter())
        }
    }

    /// True when `self` contains no temporal predicates.
    pub fn non_temporal(&self) -> bool {
        self.lower_bounds.is_empty() && self.upper_bounds.is_empty()
    }
}

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
            let filter_plan =
                FilterPlan::create_from(predicates.iter().cloned()).unwrap_or_else(|err| {
                    panic!("Temporal predicate error: {:?}", err);
                });

            let (ok_collection, err_collection) = self.collection(input).unwrap();

            let (oks, errs) = ok_collection.inner.flat_map_fallible({
                let mut datums = DatumVec::new();
                move |(data, time, diff)| {
                    let mut datums_local = datums.borrow_with(&data);
                    let times_diffs = filter_plan.evaluate(&mut datums_local, time, diff);
                    // Drop to release borrow on `data` and allow it to move into the closure.
                    drop(datums_local);
                    // Each produced (time, diff) results in a copy of `data` in the output.
                    // TODO: It would be nice to avoid the `data.clone` for the last output.
                    times_diffs.map(move |time_diff| time_diff.map(|(t, d)| (data.clone(), t, d)))
                }
            });

            let err_collection = err_collection.concat(&errs.as_collection());
            (oks.as_collection(), err_collection)
        } else {
            panic!("Non-Filter expression provided to `render_filter`");
        }
    }
}
