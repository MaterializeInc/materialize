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

use expr::MirScalarExpr;
use serde::{Deserialize, Serialize};

/// A plan describing how to compute a threshold operation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ThresholdPlan {
    /// Basic threshold maintains all positive inputs.
    Basic(BasicThresholdPlan),
    /// Retractions threshold maintains all negative inputs.
    Retractions(RetractionsThresholdPlan),
}

impl ThresholdPlan {
    /// Reports all keys of produced arrangements.
    ///
    /// This is likely either an empty vector, for no arrangement,
    /// or a singleton vector containing the list of expressions
    /// that key a single arrangement.
    pub fn keys(&self) -> Vec<Vec<MirScalarExpr>> {
        match self {
            ThresholdPlan::Basic(plan) => {
                vec![plan.ensure_arrangement.clone()]
            }
            ThresholdPlan::Retractions(_plan) => {
                vec![]
            }
        }
    }
}

/// A plan to maintain all inputs with positive counts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BasicThresholdPlan {
    /// Description of how to arrange the output
    pub ensure_arrangement: Vec<MirScalarExpr>,
}

/// A plan to maintain all inputs with negative counts, which are subtracted from the output
/// in order to maintain an equivalent collection compared to [BasicThresholdPlan].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetractionsThresholdPlan {
    /// Description of how to arrange the output
    pub ensure_arrangement: Vec<MirScalarExpr>,
}

impl ThresholdPlan {
    /// Construct the plan from the number of columns (`arity`). `maintain_retractions` allows to
    /// switch between an implementation that maintains rows with negative counts (`true`), or
    /// rows with positive counts (`false`).
    pub fn create_from(arity: usize, maintain_retractions: bool) -> Self {
        // Arrange the input by all columns in order.
        let mut all_columns = Vec::new();
        for column in 0..arity {
            all_columns.push(expr::MirScalarExpr::Column(column));
        }
        if maintain_retractions {
            ThresholdPlan::Retractions(RetractionsThresholdPlan {
                ensure_arrangement: all_columns,
            })
        } else {
            ThresholdPlan::Basic(BasicThresholdPlan {
                ensure_arrangement: all_columns,
            })
        }
    }
}
