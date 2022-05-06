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

// https://github.com/tokio-rs/prost/issues/237
#![allow(missing_docs)]

use std::collections::HashMap;

use crate::plan::any_arranged_thin;
use mz_expr::{permutation_for_arrangement, MirScalarExpr};
use mz_repr::proto::TryFromProtoError;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use super::AvailableCollections;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_dataflow_types.plan.threshold.rs"
));

/// A plan describing how to compute a threshold operation.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum ThresholdPlan {
    /// Basic threshold maintains all positive inputs.
    Basic(BasicThresholdPlan),
    /// Retractions threshold maintains all negative inputs.
    Retractions(RetractionsThresholdPlan),
}

impl From<&ThresholdPlan> for ProtoThresholdPlan {
    fn from(x: &ThresholdPlan) -> Self {
        use proto_threshold_plan::Kind;
        Self {
            kind: Some(match x {
                ThresholdPlan::Basic(p) => Kind::Basic((&p.ensure_arrangement).into()),
                ThresholdPlan::Retractions(p) => Kind::Retractions((&p.ensure_arrangement).into()),
            }),
        }
    }
}

impl TryFrom<ProtoThresholdPlan> for ThresholdPlan {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoThresholdPlan) -> Result<Self, Self::Error> {
        use proto_threshold_plan::Kind;
        let kind = x
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoThresholdPlan::kind"))?;
        Ok(match kind {
            Kind::Basic(p) => ThresholdPlan::Basic(BasicThresholdPlan {
                ensure_arrangement: p.try_into()?,
            }),
            Kind::Retractions(p) => ThresholdPlan::Retractions(RetractionsThresholdPlan {
                ensure_arrangement: p.try_into()?,
            }),
        })
    }
}

impl ThresholdPlan {
    /// Reports all keys of produced arrangements.
    ///
    /// This is likely either an empty vector, for no arrangement,
    /// or a singleton vector containing the list of expressions
    /// that key a single arrangement.
    pub fn keys(&self) -> AvailableCollections {
        match self {
            ThresholdPlan::Basic(plan) => {
                AvailableCollections::new_arranged(vec![plan.ensure_arrangement.clone()])
            }
            ThresholdPlan::Retractions(_plan) => AvailableCollections::new_raw(),
        }
    }
}

/// A plan to maintain all inputs with positive counts.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BasicThresholdPlan {
    /// Description of how the input has been arranged, and how to arrange the output
    #[proptest(strategy = "any_arranged_thin()")]
    pub ensure_arrangement: (Vec<MirScalarExpr>, HashMap<usize, usize>, Vec<usize>),
}

/// A plan to maintain all inputs with negative counts, which are subtracted from the output
/// in order to maintain an equivalent collection compared to [BasicThresholdPlan].
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct RetractionsThresholdPlan {
    /// Description of how the input has been arranged
    #[proptest(strategy = "any_arranged_thin()")]
    pub ensure_arrangement: (Vec<MirScalarExpr>, HashMap<usize, usize>, Vec<usize>),
}

impl ThresholdPlan {
    /// Construct the plan from the number of columns (`arity`). `maintain_retractions` allows to
    /// switch between an implementation that maintains rows with negative counts (`true`), or
    /// rows with positive counts (`false`).
    ///
    /// Also returns the arrangement and thinning required for the input.
    pub fn create_from(
        arity: usize,
        maintain_retractions: bool,
    ) -> (
        Self,
        (Vec<MirScalarExpr>, HashMap<usize, usize>, Vec<usize>),
    ) {
        // Arrange the input by all columns in order.
        let mut all_columns = Vec::new();
        for column in 0..arity {
            all_columns.push(mz_expr::MirScalarExpr::Column(column));
        }
        let (permutation, thinning) = permutation_for_arrangement(&all_columns, arity);
        let ensure_arrangement = (all_columns, permutation, thinning);
        let plan = if maintain_retractions {
            ThresholdPlan::Retractions(RetractionsThresholdPlan {
                ensure_arrangement: ensure_arrangement.clone(),
            })
        } else {
            ThresholdPlan::Basic(BasicThresholdPlan {
                ensure_arrangement: ensure_arrangement.clone(),
            })
        };
        (plan, ensure_arrangement)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::proto::protobuf_roundtrip;
    use proptest::prelude::*;

    proptest! {
       #[test]
        fn threshold_plan_protobuf_roundtrip(expect in any::<ThresholdPlan>() ) {
            let actual = protobuf_roundtrip::<_, ProtoThresholdPlan>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
