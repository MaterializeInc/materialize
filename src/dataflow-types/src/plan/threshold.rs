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
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_expr::{permutation_for_arrangement, MirScalarExpr};
use mz_proto::{ProtoType, RustType, TryFromProtoError};

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

impl RustType<ProtoThresholdPlan> for ThresholdPlan {
    fn into_proto(&self) -> ProtoThresholdPlan {
        use proto_threshold_plan::Kind::*;
        ProtoThresholdPlan {
            kind: Some(match self {
                ThresholdPlan::Basic(p) => Basic(p.ensure_arrangement.into_proto()),
                ThresholdPlan::Retractions(p) => Retractions(p.ensure_arrangement.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoThresholdPlan) -> Result<Self, TryFromProtoError> {
        use proto_threshold_plan::Kind::*;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoThresholdPlan::kind"))?;
        Ok(match kind {
            Basic(p) => ThresholdPlan::Basic(BasicThresholdPlan {
                ensure_arrangement: p.into_rust()?,
            }),
            Retractions(p) => ThresholdPlan::Retractions(RetractionsThresholdPlan {
                ensure_arrangement: p.into_rust()?,
            }),
        })
    }
}

impl RustType<ProtoArrangement> for (Vec<MirScalarExpr>, HashMap<usize, usize>, Vec<usize>) {
    fn into_proto(&self) -> ProtoArrangement {
        use proto_arrangement::ProtoArrangementPermutation;
        ProtoArrangement {
            all_columns: self.0.into_proto(),
            permutation: self
                .1
                .iter()
                .map(|x| ProtoArrangementPermutation {
                    key: x.0.into_proto(),
                    val: x.1.into_proto(),
                })
                .collect(),
            thinning: self.2.iter().map(|x| x.into_proto()).collect(),
        }
    }

    fn from_proto(proto: ProtoArrangement) -> Result<Self, TryFromProtoError> {
        let perm: Result<HashMap<usize, usize>, TryFromProtoError> = proto
            .permutation
            .iter()
            .map(|x| {
                let key = usize::from_proto(x.key);
                let val = usize::from_proto(x.val);
                Ok((key?, val?))
            })
            .collect();
        Ok((
            proto.all_columns.into_rust()?,
            perm?,
            proto.thinning.into_rust()?,
        ))
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
    use mz_proto::protobuf_roundtrip;
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
