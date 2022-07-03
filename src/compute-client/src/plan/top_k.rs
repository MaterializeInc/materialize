// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TopK planning logic.
//!
//! We provide a plan ([TopKPlan]) encoding variants of the TopK operator, and provide
//! implementations specific to plan variants.
//!
//! The TopK variants can be distinguished as follows:
//! * A [MonotonicTop1Plan] maintains a single row per key and is suitable for monotonic inputs.
//! * A [MonotonicTopKPlan] maintains up to K rows per key and is suitable for monotonic inputs.
//! * A [BasicTopKPlan] maintains up to K rows per key and can handle retractions.

#![allow(missing_docs)]

use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_expr::ColumnOrder;
use mz_proto::{ProtoType, RustType, TryFromProtoError};

include!(concat!(env!("OUT_DIR"), "/mz_compute_client.plan.top_k.rs"));

/// A plan encapsulating different variants to compute a TopK operation.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum TopKPlan {
    /// A plan for Top1 for monotonic inputs.
    MonotonicTop1(MonotonicTop1Plan),
    /// A plan for TopK for monotonic inputs.
    MonotonicTopK(MonotonicTopKPlan),
    /// A plan for generic TopK operations.
    Basic(BasicTopKPlan),
}

impl TopKPlan {
    /// Create a plan from the information provided. Here we decide on which of the TopK plan
    /// variants to select.
    ///
    /// * `group_key` - The columns serving as the group key.
    /// * `order_key` - The columns specifying an ordering withing each group.
    /// * `offset` - The number of rows to skip at the top. Provide 0 to reveal all rows.
    /// * `limit` - An optional limit of how many rows should be revealed.
    /// * `arity` - The number of columns in the input and output.
    /// * `monotonic` - `true` if the input is monotonic.
    pub(crate) fn create_from(
        group_key: Vec<usize>,
        order_key: Vec<ColumnOrder>,
        offset: usize,
        limit: Option<usize>,
        arity: usize,
        monotonic: bool,
    ) -> Self {
        if monotonic && offset == 0 && limit == Some(1) {
            TopKPlan::MonotonicTop1(MonotonicTop1Plan {
                group_key,
                order_key,
            })
        } else if monotonic && offset == 0 {
            // For monotonic inputs, we are able to retract inputs that can no longer be produced
            // as outputs. Any inputs beyond `offset + limit` will never again be produced as
            // outputs, and can be removed. The simplest form of this is when `offset == 0` and
            // these removable records are those in the input not produced in the output.
            // TODO: consider broadening this optimization to `offset > 0` by first filtering
            // down to `offset = 0` and `limit = offset + limit`, followed by a finishing act
            // of `offset` and `limit`, discarding only the records not produced in the intermediate
            // stage.
            TopKPlan::MonotonicTopK(MonotonicTopKPlan {
                group_key,
                order_key,
                limit,
                arity,
            })
        } else {
            // A plan for all other inputs
            TopKPlan::Basic(BasicTopKPlan {
                group_key,
                order_key,
                offset,
                limit,
                arity,
            })
        }
    }
}

impl RustType<ProtoTopKPlan> for TopKPlan {
    fn into_proto(&self) -> ProtoTopKPlan {
        use crate::plan::top_k::proto_top_k_plan::Kind::*;

        ProtoTopKPlan {
            kind: match self {
                TopKPlan::Basic(plan) => Some(Basic(plan.into_proto())),
                TopKPlan::MonotonicTop1(plan) => Some(MonotonicTop1(plan.into_proto())),
                TopKPlan::MonotonicTopK(plan) => Some(MonotonicTopK(plan.into_proto())),
            },
        }
    }

    fn from_proto(proto: ProtoTopKPlan) -> Result<Self, TryFromProtoError> {
        use crate::plan::top_k::proto_top_k_plan::Kind::*;

        match proto.kind {
            Some(Basic(plan)) => Ok(TopKPlan::Basic(plan.into_rust()?)),
            Some(MonotonicTop1(plan)) => Ok(TopKPlan::MonotonicTop1(plan.into_rust()?)),
            Some(MonotonicTopK(plan)) => Ok(TopKPlan::MonotonicTopK(plan.into_rust()?)),
            None => Err(TryFromProtoError::missing_field("ProtoTopKPlan::kind")),
        }
    }
}

/// A plan for monotonic TopKs with an offset of 0 and a limit of 1.
///
/// If the input to a TopK is monotonic (aka append-only aka no retractions) then we
/// don't have to worry about keeping every row we've seen around forever. Instead,
/// the reduce can incrementally compute a new answer by looking at just the old TopK
/// for a key and the incremental data.
///
/// This optimization generalizes to any TopK over a monotonic source, but we special
/// case only TopK with offset=0 and limit=1 (aka Top1) for now. This is because (1)
/// Top1 can merge in each incremental row in constant space and time while the
/// generalized solution needs something like a priority queue and (2) we expect Top1
/// will be a common pattern used to turn a Kafka source's "upsert" semantics into
/// differential's semantics. (2) is especially interesting because Kafka is
/// monotonic with an ENVELOPE of NONE, which is the default for ENVELOPE in
/// Materialize and commonly used by users.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MonotonicTop1Plan {
    /// The columns that form the key for each group.
    pub group_key: Vec<usize>,
    /// Ordering that is used within each group.
    pub order_key: Vec<mz_expr::ColumnOrder>,
}

impl RustType<ProtoMonotonicTop1Plan> for MonotonicTop1Plan {
    fn into_proto(&self) -> ProtoMonotonicTop1Plan {
        ProtoMonotonicTop1Plan {
            group_key: self.group_key.into_proto(),
            order_key: self.order_key.into_proto(),
        }
    }

    fn from_proto(proto: ProtoMonotonicTop1Plan) -> Result<Self, TryFromProtoError> {
        Ok(MonotonicTop1Plan {
            group_key: proto.group_key.into_rust()?,
            order_key: proto.order_key.into_rust()?,
        })
    }
}

/// A plan for monotonic TopKs with an offset of 0 and an arbitrary limit.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct MonotonicTopKPlan {
    /// The columns that form the key for each group.
    pub group_key: Vec<usize>,
    /// Ordering that is used within each group.
    pub order_key: Vec<mz_expr::ColumnOrder>,
    /// Optionally, an upper bound on the per-group ordinal position of the
    /// records to produce from each group.
    pub limit: Option<usize>,
    /// The number of columns in the input and output.
    pub arity: usize,
}

impl RustType<ProtoMonotonicTopKPlan> for MonotonicTopKPlan {
    fn into_proto(&self) -> ProtoMonotonicTopKPlan {
        ProtoMonotonicTopKPlan {
            group_key: self.group_key.into_proto(),
            order_key: self.order_key.into_proto(),
            limit: self.limit.into_proto(),
            arity: self.arity.into_proto(),
        }
    }

    fn from_proto(proto: ProtoMonotonicTopKPlan) -> Result<Self, TryFromProtoError> {
        Ok(MonotonicTopKPlan {
            group_key: proto.group_key.into_rust()?,
            order_key: proto.order_key.into_rust()?,
            limit: proto.limit.into_rust()?,
            arity: proto.arity.into_rust()?,
        })
    }
}

/// A plan for generic TopKs that don't fit any more specific category.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct BasicTopKPlan {
    /// The columns that form the key for each group.
    pub group_key: Vec<usize>,
    /// Ordering that is used within each group.
    pub order_key: Vec<mz_expr::ColumnOrder>,
    /// Optionally, an upper bound on the per-group ordinal position of the
    /// records to produce from each group.
    pub limit: Option<usize>,
    /// A lower bound on the per-group ordinal position of the records to
    /// produce from each group.
    ///
    /// This can be set to zero to have no effect.
    pub offset: usize,
    /// The number of columns in the input and output.
    pub arity: usize,
}

impl RustType<ProtoBasicTopKPlan> for BasicTopKPlan {
    fn into_proto(&self) -> ProtoBasicTopKPlan {
        ProtoBasicTopKPlan {
            group_key: self.group_key.into_proto(),
            order_key: self.order_key.into_proto(),
            limit: self.limit.into_proto(),
            offset: self.offset.into_proto(),
            arity: self.arity.into_proto(),
        }
    }

    fn from_proto(proto: ProtoBasicTopKPlan) -> Result<Self, TryFromProtoError> {
        Ok(BasicTopKPlan {
            group_key: proto.group_key.into_rust()?,
            order_key: proto.order_key.into_rust()?,
            limit: proto.limit.into_rust()?,
            offset: proto.offset.into_rust()?,
            arity: proto.arity.into_rust()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn top_k_plan_protobuf_roundtrip(expect in any::<TopKPlan>()) {
            let actual = protobuf_roundtrip::<_, ProtoTopKPlan>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
