// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Threshold planning logic.
//!
//! The threshold operator produces only rows with a positive cardinality, for example required to
//! provide SQL except and intersect semantics.
//!
//! We build a plan ([ThresholdPlan]) encapsulating all decisions and requirements on the specific
//! threshold implementation. The idea is to decouple the logic deciding which plan to select from
//! the actual implementation of each variant available.
//!
//! Currently, we provide two variants:
//! * The [BasicThresholdPlan] maintains all its outputs as an arrangement. It is beneficial if the
//!     threshold is the final operation, or a downstream operators expects arranged inputs.
//! * The [RetractionsThresholdPlan] maintains retractions, i.e. rows that are not in the output. It
//!     is beneficial to use this operator if the number of retractions is expected to be small, and
//!     if a potential downstream operator does not expect its input to be arranged.

use mz_expr::{MirScalarExpr, permutation_for_arrangement};
use mz_repr::ColumnType;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::plan::{AvailableCollections, any_arranged_thin};

/// A plan describing how to compute a threshold operation.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub enum ThresholdPlan {
    /// Basic threshold maintains all positive inputs.
    Basic(BasicThresholdPlan),
}

impl ThresholdPlan {
    /// Reports all keys of produced arrangements, with optionally
    /// given types describing the rows that would be in the raw
    /// form of the collection.
    ///
    /// This is likely either an empty vector, for no arrangement,
    /// or a singleton vector containing the list of expressions
    /// that key a single arrangement.
    pub fn keys(&self, types: Option<Vec<ColumnType>>) -> AvailableCollections {
        match self {
            ThresholdPlan::Basic(plan) => {
                AvailableCollections::new_arranged(vec![plan.ensure_arrangement.clone()], types)
            }
        }
    }
}

/// A plan to maintain all inputs with positive counts.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct BasicThresholdPlan {
    /// Description of how the input has been arranged, and how to arrange the output
    #[proptest(strategy = "any_arranged_thin()")]
    pub ensure_arrangement: (Vec<MirScalarExpr>, Vec<usize>, Vec<usize>),
}

/// A plan to maintain all inputs with negative counts, which are subtracted from the output
/// in order to maintain an equivalent collection compared to [BasicThresholdPlan].
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct RetractionsThresholdPlan {
    /// Description of how the input has been arranged
    #[proptest(strategy = "any_arranged_thin()")]
    pub ensure_arrangement: (Vec<MirScalarExpr>, Vec<usize>, Vec<usize>),
}

impl ThresholdPlan {
    /// Construct the plan from the number of columns (`arity`).
    ///
    /// Also returns the arrangement and thinning required for the input.
    pub fn create_from(arity: usize) -> (Self, (Vec<MirScalarExpr>, Vec<usize>, Vec<usize>)) {
        // Arrange the input by all columns in order.
        let mut all_columns = Vec::new();
        for column in 0..arity {
            all_columns.push(mz_expr::MirScalarExpr::Column(column));
        }
        let (permutation, thinning) = permutation_for_arrangement(&all_columns, arity);
        let ensure_arrangement = (all_columns, permutation, thinning);
        let plan = ThresholdPlan::Basic(BasicThresholdPlan {
            ensure_arrangement: ensure_arrangement.clone(),
        });
        (plan, ensure_arrangement)
    }
}
